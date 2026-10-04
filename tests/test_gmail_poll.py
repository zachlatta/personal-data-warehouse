"""The Gmail poll loop: one Dagster run holds the mailbox lock for a window and
asks each mailbox for its history every few seconds.

Consistency is the point, not just latency. The history cursor is the only
thing that says "everything before here is in the warehouse", so it advances
only after every change it covers has been written; a failed tick leaves it
where it was and the next tick re-reads the same history. A reconcile pass
lists recent mail independently of history and fetches anything missing, so a
change history never reported still converges.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from personal_data_warehouse import gmail_sync
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.gmail_sync import GmailPollConfig, GmailSyncRunner, SyncState


class FakeLogger:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def info(self, message, *args, **kwargs) -> None:
        self.infos.append(message % args if args else message)

    def warning(self, message, *args, **kwargs) -> None:
        self.warnings.append(message % args if args else message)


def _message(message_id: str, history_id: int) -> dict:
    return {
        "id": message_id,
        "threadId": f"thread-{message_id}",
        "historyId": str(history_id),
        "internalDate": "1713875400000",
        "labelIds": ["INBOX"],
        "snippet": f"snippet {message_id}",
        "payload": {"headers": [{"name": "Subject", "value": f"subject {message_id}"}], "body": {"data": ""}},
    }


@dataclass
class FakeMailbox:
    """A Gmail mailbox: messages, an ordered history log, and the request shapes the sync uses."""

    history_id: int = 100
    messages: dict[str, dict] = field(default_factory=dict)
    history: list[tuple[int, str]] = field(default_factory=list)
    fail_next_gets: int = 0
    history_calls: int = 0
    list_calls: int = 0

    def deliver(self, message_id: str, *, record_history: bool = True) -> None:
        self.history_id += 1
        self.messages[message_id] = _message(message_id, self.history_id)
        if record_history:
            self.history.append((self.history_id, message_id))


class _Request:
    def __init__(self, fn: Callable[[], dict]) -> None:
        self._fn = fn

    def execute(self) -> dict:
        return self._fn()


def _http_error(status: int) -> HttpError:
    return HttpError(Response({"status": status}), b"{}")


class FakeService:
    def __init__(self, mailbox: FakeMailbox) -> None:
        self.mailbox = mailbox

    def users(self):
        return self

    def history(self):
        return _History(self.mailbox)

    def messages(self):
        return _Messages(self.mailbox)

    def getProfile(self, *, userId: str):  # noqa: N802 - Gmail API spelling
        return _Request(lambda: {"historyId": str(self.mailbox.history_id)})


class _History:
    def __init__(self, mailbox: FakeMailbox) -> None:
        self.mailbox = mailbox

    def list(self, *, userId, startHistoryId, maxResults, pageToken):  # noqa: N803
        def run() -> dict:
            self.mailbox.history_calls += 1
            start = int(startHistoryId)
            records = [
                {"id": str(hid), "messagesAdded": [{"message": {"id": mid}}]}
                for hid, mid in self.mailbox.history
                if hid > start
            ]
            return {"history": records, "historyId": str(self.mailbox.history_id)}

        return _Request(run)


class _Messages:
    def __init__(self, mailbox: FakeMailbox) -> None:
        self.mailbox = mailbox

    def get(self, *, userId, id, format):  # noqa: A002
        def run() -> dict:
            if self.mailbox.fail_next_gets > 0:
                self.mailbox.fail_next_gets -= 1
                raise _http_error(403)
            if id not in self.mailbox.messages:
                raise _http_error(404)
            return self.mailbox.messages[id]

        return _Request(run)

    def list(self, *, userId, maxResults, pageToken, includeSpamTrash, q=None):  # noqa: N803
        def run() -> dict:
            self.mailbox.list_calls += 1
            return {"messages": [{"id": mid} for mid in sorted(self.mailbox.messages)]}

        return _Request(run)


class FakeWarehouse:
    def __init__(self, cursors: dict[str, int]) -> None:
        self.state = {
            account: SyncState(
                account=account,
                last_history_id=history_id,
                last_sync_type="partial",
                status="ok",
                error="",
                updated_at=None,
            )
            for account, history_id in cursors.items()
        }
        self.messages: dict[tuple[str, str], dict] = {}
        self.ensure_calls = 0
        self.backfill_requests = 0

    def ensure_tables(self) -> None:
        self.ensure_calls += 1

    def load_sync_state(self):
        return dict(self.state)

    def insert_sync_state(self, *, account, last_history_id, last_sync_type, status, error, updated_at) -> None:
        self.state[account] = SyncState(
            account=account,
            last_history_id=last_history_id,
            last_sync_type=last_sync_type,
            status=status,
            error=error,
            updated_at=updated_at,
        )

    def existing_attachment_keys(self, *, account, message_ids):
        return set()

    def existing_message_ids(self, *, account, message_ids):
        return {mid for mid in message_ids if (account, mid) in self.messages}

    def load_message_payloads(self, *, account, message_ids):
        return {}

    def insert_messages(self, rows) -> None:
        for row in rows:
            self.messages[(row["account"], row["message_id"])] = row

    def insert_attachments(self, rows) -> None:
        pass

    def load_attachment_backfill_candidate_messages(self, **_kwargs):
        self.backfill_requests += 1
        return []


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []
        self.on_sleep: list[Callable[[float], None]] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        for hook in self.on_sleep:
            hook(self.now)


ACCOUNT = "zach@example.com"
OTHER = "other@example.com"


@pytest.fixture
def settings(monkeypatch):
    monkeypatch.setenv("GMAIL_ACCOUNTS", f"{ACCOUNT},{OTHER}")
    monkeypatch.setenv("GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE", "10")
    return load_settings(require_postgres=False, require_gmail_client_secrets=False)


@pytest.fixture(autouse=True)
def _no_real_lock(monkeypatch):
    from contextlib import contextmanager

    @contextmanager
    def acquired():
        yield True

    monkeypatch.setattr(gmail_sync, "exclusive_gmail_sync_lock", acquired)


def _runner(settings, warehouse, mailboxes, *, clock, built=None, logger=None):
    def service_factory(*, account, settings):
        if built is not None:
            built.append(account.email_address)
        return FakeService(mailboxes[account.email_address])

    return GmailSyncRunner(
        settings=settings,
        warehouse=warehouse,
        logger=logger or FakeLogger(),
        service_factory=service_factory,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )


def _config(**overrides) -> GmailPollConfig:
    values = {
        "window_seconds": 60.0,
        "poll_interval_seconds": 15.0,
        "attachment_backfill_interval_seconds": 300.0,
        "reconcile_interval_seconds": 600.0,
        "reconcile_query": "newer_than:2d",
        "max_backoff_seconds": 300.0,
    }
    values.update(overrides)
    return GmailPollConfig(**values)


def test_a_message_delivered_between_ticks_lands_on_the_next_tick(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})
    clock = FakeClock()
    landed: list[float] = []
    clock.on_sleep.append(lambda now: mailboxes[ACCOUNT].deliver("m1") if now == 15.0 else None)

    summary = _runner(settings, warehouse, mailboxes, clock=clock).run(
        config=_config(window_seconds=30.0),
        on_messages_written=lambda: landed.append(clock.now),
    )

    assert (ACCOUNT, "m1") in warehouse.messages
    assert landed == [15.0], "the timeline is landed in the tick that wrote, not at window end"
    assert warehouse.state[ACCOUNT].last_history_id == 101
    assert warehouse.state[ACCOUNT].status == "ok"
    assert summary.ticks == 3
    assert summary.messages_written == 1
    assert clock.sleeps == [15.0, 15.0]


def test_the_cursor_does_not_advance_past_a_change_that_failed_to_write(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    mailboxes[ACCOUNT].deliver("m1")
    mailboxes[ACCOUNT].fail_next_gets = 1
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})
    clock = FakeClock()
    cursors: list[tuple[int, str]] = []
    clock.on_sleep.append(
        lambda _now: cursors.append((warehouse.state[ACCOUNT].last_history_id, warehouse.state[ACCOUNT].status))
    )

    _runner(settings, warehouse, mailboxes, clock=clock).run(config=_config(window_seconds=60.0))

    assert cursors[0] == (100, "failed"), "a failed tick leaves the cursor where it was"
    assert (ACCOUNT, "m1") in warehouse.messages
    assert warehouse.state[ACCOUNT].last_history_id == 101
    assert warehouse.state[ACCOUNT].status == "ok"


def test_a_failing_mailbox_backs_off_without_delaying_the_others(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    mailboxes[ACCOUNT].deliver("m1")
    mailboxes[ACCOUNT].fail_next_gets = 10**6
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})
    clock = FakeClock()

    with pytest.raises(RuntimeError, match=ACCOUNT):
        _runner(settings, warehouse, mailboxes, clock=clock).run(config=_config(window_seconds=300.0))

    assert mailboxes[OTHER].history_calls == 21, "the healthy mailbox is polled every tick"
    # 0, 15, 45, 105, 225: doubling from the poll interval.
    assert mailboxes[ACCOUNT].history_calls == 5
    assert warehouse.state[ACCOUNT].status == "failed"


def test_a_run_that_recovers_before_its_window_ends_succeeds(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    mailboxes[ACCOUNT].deliver("m1")
    mailboxes[ACCOUNT].fail_next_gets = 2
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})

    summary = _runner(settings, warehouse, mailboxes, clock=FakeClock()).run(config=_config(window_seconds=120.0))

    assert summary.failed_ticks == 2
    assert warehouse.state[ACCOUNT].status == "ok"


def test_a_mailbox_service_is_built_once_and_rebuilt_after_an_error(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    mailboxes[ACCOUNT].deliver("m1")
    mailboxes[ACCOUNT].fail_next_gets = 1
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})
    built: list[str] = []

    _runner(settings, warehouse, mailboxes, clock=FakeClock(), built=built).run(config=_config(window_seconds=90.0))

    assert built.count(OTHER) == 1
    assert built.count(ACCOUNT) == 2
    assert warehouse.ensure_calls == 1, "table DDL runs once per window, never per tick"


def test_reconcile_fetches_recent_mail_that_history_never_reported(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    mailboxes[ACCOUNT].deliver("ghost", record_history=False)
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})
    landed: list[int] = []

    summary = _runner(settings, warehouse, mailboxes, clock=FakeClock()).run(
        config=_config(window_seconds=30.0, reconcile_interval_seconds=600.0),
        on_messages_written=lambda: landed.append(1),
    )

    assert (ACCOUNT, "ghost") in warehouse.messages
    assert summary.reconciled_messages == 1
    assert landed == [1]
    assert mailboxes[ACCOUNT].list_calls == 1, "reconcile runs on its own interval, not every tick"


def test_reconcile_and_attachment_backfill_run_on_their_own_intervals(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})

    summary = _runner(settings, warehouse, mailboxes, clock=FakeClock()).run(
        config=_config(window_seconds=900.0, attachment_backfill_interval_seconds=300.0, reconcile_interval_seconds=600.0)
    )

    assert summary.ticks == 61
    # Backfill at 0, 300, 600, 900; reconcile at 0 and 600; per mailbox.
    assert warehouse.backfill_requests == 2 * 4
    assert mailboxes[ACCOUNT].list_calls == 2


def test_a_slow_tick_does_not_sleep_on_top_of_its_own_duration(settings) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})
    clock = FakeClock()
    original = warehouse.load_sync_state

    def slow_load():
        clock.now += 10.0
        return original()

    warehouse.load_sync_state = slow_load

    _runner(settings, warehouse, mailboxes, clock=clock).run(config=_config(window_seconds=30.0))

    assert clock.sleeps[0] == 5.0


def test_a_stale_history_cursor_falls_back_to_a_full_sync(settings, monkeypatch) -> None:
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    mailboxes[ACCOUNT].deliver("m1")
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})

    def expired(*, service, start_history_id, page_size):
        if service.mailbox is mailboxes[ACCOUNT]:
            raise _http_error(404)
        return set(), start_history_id

    monkeypatch.setattr(gmail_sync, "load_incremental_message_ids", expired)

    _runner(settings, warehouse, mailboxes, clock=FakeClock()).run(config=_config(window_seconds=0.0))

    assert (ACCOUNT, "m1") in warehouse.messages
    assert warehouse.state[ACCOUNT].last_sync_type == "full"
    assert warehouse.state[ACCOUNT].last_history_id == 101


def test_the_run_is_a_no_op_when_another_run_holds_the_mailbox_lock(settings, monkeypatch) -> None:
    from contextlib import contextmanager

    @contextmanager
    def held():
        yield False

    monkeypatch.setattr(gmail_sync, "exclusive_gmail_sync_lock", held)
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})

    summary = _runner(settings, warehouse, mailboxes, clock=FakeClock()).run(config=_config())

    assert summary.lock_acquired is False
    assert summary.ticks == 0
    assert mailboxes[ACCOUNT].history_calls == 0


def test_poll_config_reads_its_knobs_from_the_environment(monkeypatch) -> None:
    for name in (
        "GMAIL_POLL_WINDOW_SECONDS",
        "GMAIL_POLL_INTERVAL_SECONDS",
        "GMAIL_ATTACHMENT_BACKFILL_INTERVAL_SECONDS",
        "GMAIL_RECONCILE_INTERVAL_SECONDS",
        "GMAIL_RECONCILE_QUERY",
    ):
        monkeypatch.delenv(name, raising=False)
    default = GmailPollConfig.from_env()
    assert default.poll_interval_seconds == 15.0
    assert default.window_seconds == 2700.0
    assert default.attachment_backfill_interval_seconds == 300.0
    assert default.reconcile_interval_seconds == 600.0
    assert default.reconcile_query == "newer_than:2d"

    monkeypatch.setenv("GMAIL_POLL_INTERVAL_SECONDS", "5")
    assert GmailPollConfig.from_env().poll_interval_seconds == 5.0

    monkeypatch.setenv("GMAIL_POLL_INTERVAL_SECONDS", "0")
    with pytest.raises(ValueError, match="GMAIL_POLL_INTERVAL_SECONDS"):
        GmailPollConfig.from_env()


def test_an_idle_tick_writes_no_log_line(settings) -> None:
    """The loop reads history every 15 s; a line per mailbox per idle tick was
    ~11.5k Dagster event-log rows a day saying nothing."""
    mailboxes = {ACCOUNT: FakeMailbox(), OTHER: FakeMailbox()}
    warehouse = FakeWarehouse({ACCOUNT: 100, OTHER: 100})
    logger = FakeLogger()
    clock = FakeClock()
    runner = _runner(settings, warehouse, mailboxes, clock=clock, logger=logger)

    runner.run(config=_config(window_seconds=60.0))

    assert logger.infos == []
