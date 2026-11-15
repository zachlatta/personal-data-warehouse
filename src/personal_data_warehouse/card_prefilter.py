"""Cheap, deliberately permissive candidate discovery; never a Luhn decision."""
from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
import os
import re
import threading

from personal_data_warehouse.card_redaction import card_count, redact_row

UNICODE_UNIT = r'u[0-9a-fA-F]{4}'
DIGIT_WINDOW = r'[0-9]([ -]{0,3}[0-9]){12}'
_UNIT = re.compile(UNICODE_UNIT)
_WINDOW = re.compile(DIGIT_WINDOW)


def sql_prefilter(expression):
    """Parallel-safe SQL expression, requiring no installed function or DDL.

    Remove all literal backslashes; replace EVERY Unicode unit by a digit
    sentinel (not only escaped digits). Each decoded PAN digit contributes at
    least one literal/sentinel digit. Escaped separators/backslashes contribute
    extra digits; remaining separators are at most three spaces/dashes. Thus a
    PAN implies 13 normalized digits within 49 normalized characters, whatever
    the raw escaping depth. False positives are always confirmed in Python.
    The expression is supplied by code, never by CLI input.
    """
    return (f"regexp_replace(translate(COALESCE(({expression})::text, ''), chr(92), ''), "
            f"'{UNICODE_UNIT}', '0', 'g') ~ '{DIGIT_WINDOW}'")


def might_contain_card(value):
    return bool(_WINDOW.search(_UNIT.sub('0',value.replace('\\',''))))


def available_cpus():
    return max(1,len(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else os.cpu_count() or 1)


def count_row(row):
    return sum(card_count(v if isinstance(v,str) else json.dumps(v))
               for v in row.values() if isinstance(v,(str,dict,list)))


class ConfirmationPool:
    """Bounded in-memory IPC; spawned children never inherit DB connections."""
    def __init__(self, workers):
        self.workers = max(1,workers)
        self._pool = None
        self._lock = threading.Lock()

    def counts(self, rows):
        return self._map(count_row,rows)

    def redacted(self, rows):
        return self._map(redact_row,rows)

    def _map(self,function,rows):
        if self.workers == 1 or len(rows) < 64:
            return list(map(function,rows))
        with self._lock:
            if self._pool is None:
                self._pool = ProcessPoolExecutor(max_workers=self.workers,mp_context=multiprocessing.get_context('spawn'))
        try:
            return list(self._pool.map(function,rows,chunksize=8))
        except Exception:
            raise RuntimeError('candidate confirmation worker failed') from None

    def __enter__(self): return self

    def __exit__(self,*args):
        if self._pool is not None: self._pool.shutdown(cancel_futures=True)
