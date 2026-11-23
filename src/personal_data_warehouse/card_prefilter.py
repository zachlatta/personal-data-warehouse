"""Cheap, deliberately permissive candidate discovery; never a Luhn or credential decision."""
from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
import os
import re
import threading

from personal_data_warehouse.card_redaction import CARD_POLICY, sensitive_count, redact_row
from personal_data_warehouse.credential_redaction import SQL_PREFILTER, might_contain_credential

UNICODE_UNIT = r'u[0-9a-fA-F]{4}'
UNIT_DIGITS = '0000'
MIN_CARD_DIGITS = min(n for issuer in CARD_POLICY['issuers'] for n in issuer['lengths'])
DIGIT_WINDOW = r'[0-9]([ -]{0,3}[0-9]){'+str(MIN_CARD_DIGITS-1)+'}'
_UNIT = re.compile(UNICODE_UNIT)
_WINDOW = re.compile(DIGIT_WINDOW)


def sql_prefilter(expression, *, scope='all'):
    """Parallel-safe SQL expression, requiring no installed function or DDL.

    Remove backslashes; replace each possible Unicode unit by FOUR digits.
    A bare 'u' can introduce ordinary prose digits, so one sentinel would lose
    up to four literal PAN digits. Four preserves BOTH possible readings: at
    most four literal digits or one decoded character. No possible PAN digit
    is lost; escape syntax introduces only extra digits, including when the
    backslashes or the escape's own 'u' are encoded at further JSON levels.
    Every remaining PAN separator is at most three spaces/dashes. Thus a PAN
    implies 13 digits within 49 normalized characters at any escaping depth.
    This is an upper bound, not an escape decoder or a card decision.
    False positives are always confirmed in Python.
    The expression is supplied by code, never by CLI input.
    """
    if scope == 'credentials':
        # Credential literals survive backslash removal; Unicode escapes leave
        # 'u00'. Python confirmation makes the final decision.
        return (f"(translate(COALESCE(({expression})::text, ''), chr(92), '') ~* "
                f"'{_sql_literal(SQL_PREFILTER)}')")
    # Credential literals survive the unit rewrite (no rule needs a 'u'+hex
    # run), and rewritten units already match the four-digit sentinel.
    return (f"(regexp_replace(translate(COALESCE(({expression})::text, ''), chr(92), ''), "
            f"'{UNICODE_UNIT}', '{UNIT_DIGITS}', 'g') ~* '({DIGIT_WINDOW}|{UNIT_DIGITS}|{_sql_literal(SQL_PREFILTER)})')")


def _sql_literal(pattern):
    return pattern.replace("'", "''")


def might_contain_sensitive(value):
    return (might_contain_credential(value) or 'u00' in value or
            bool(_WINDOW.search(_UNIT.sub(UNIT_DIGITS,value.replace('\\','')))))


def available_cpus():
    return max(1,len(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else os.cpu_count() or 1)


def count_row(row):
    return sum(sensitive_count(v if isinstance(v,str) else json.dumps(v))
               for v in row.values() if isinstance(v,(str,dict,list)))


class ConfirmationPool:
    """Bounded in-memory IPC; spawned children never inherit DB connections."""
    def __init__(self, workers, selector=None):
        self.workers = max(1,workers)
        self.selector = selector
        self.left_alone = 0
        self._pool = None
        self._lock = threading.Lock()

    def counts(self, rows, *, statistics=None):
        counts = self._map(self.selector.count_row if self.selector else count_row,rows)
        if self.selector and self.selector.scope != "all":
            total = sum(self._map(count_row,rows))
            with self._lock: self.left_alone += total - sum(counts)
            if statistics is not None: statistics['left_alone'] += total - sum(counts)
        return counts

    def redacted(self, rows):
        return self._map(self.selector.redact_row if self.selector else redact_row,rows)

    def _map(self,function,rows):
        if self.workers == 1 or len(rows) < 64:
            return list(map(function,rows))
        with self._lock:
            if self._pool is None:
                self._pool = ProcessPoolExecutor(max_workers=self.workers,mp_context=multiprocessing.get_context('spawn'))
        try:
            return list(self._pool.map(function,rows,chunksize=8))
        except TimeoutError:
            raise
        except Exception:
            raise RuntimeError('candidate confirmation worker failed') from None

    def __enter__(self): return self

    def __exit__(self,exc_type,*args):
        if self._pool is None: return
        if exc_type is not None:
            # Python 3.12 has no public terminate_workers API. An aborted
            # maintenance deadline must not wait for queued confirmations.
            for process in (self._pool._processes or {}).values():
                process.terminate()
            self._pool.shutdown(wait=False,cancel_futures=True)
        else:
            self._pool.shutdown(cancel_futures=True)
