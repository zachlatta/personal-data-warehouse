"""Notification capture is transactional, insert-only, and never a history replay."""
import os
import json
import subprocess
from pathlib import Path

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


pytestmark = pytest.mark.local_integration


@pytest.fixture
def warehouse():
    wh = PostgresWarehouse(os.environ["POSTGRES_DATABASE_URL"], schema=make_test_schema())
    try:
        wh.ensure_timeline_tables()
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def insert(wh, event_id, priority):
    wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title) "
                "VALUES ('test', %s, %s::" + wh.sql_relation("timeline_priority") + ", 'test', 'Hello')",
                (event_id, priority))


def test_capture_starts_only_on_enable_and_only_for_new_attention_events(warehouse):
    wh = warehouse
    insert(wh, "before", "direct")
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    for priority in ("self", "direct", "cc", "noise", "background", "unclassified"):
        insert(wh, priority, priority)
    wh._command("UPDATE @timeline_events SET title = 'Edited' WHERE event_id = 'direct'")
    wh._command("UPDATE @timeline_events SET priority = 'direct' WHERE event_id = 'noise'")
    assert wh._query("SELECT event_id FROM @notification_events ORDER BY event_id") == [("direct",)]
    wh.ensure_timeline_tables()
    assert wh._query("SELECT count(*) FROM @notification_events") == [(1,)]
    wh._command("UPDATE @notification_state SET enabled = 0 WHERE id = 'timeline'")
    insert(wh, "paused", "direct")
    assert wh._query("SELECT count(*) FROM @notification_events") == [(1,)]


def test_a_recurring_invite_pages_once_per_series_not_once_per_instance(warehouse):
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    for n in range(1, 4):
        wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title, metadata) "
                    "VALUES ('calendar_event', %s, 'direct'::" + wh.sql_relation("timeline_priority")
                    + ", 'calendar', 'Team Sync', %s::jsonb)",
                    (f"a|cal|series_{n}", '{"recurring_event_id": "series"}'))
    wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title, metadata) "
                "VALUES ('calendar_event', 'a|cal|single', 'direct'::" + wh.sql_relation("timeline_priority")
                + ", 'calendar', 'One-off', %s::jsonb)", ('{"recurring_event_id": ""}',))
    assert wh._query("SELECT event_id FROM @notification_events ORDER BY event_id") == [
        ("a|cal|series_1",), ("a|cal|single",)]


def test_capture_carries_the_conversation_keys_the_phone_groups_by(warehouse):
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title, metadata) "
                "VALUES ('gmail_email', 'me|m1', 'direct'::" + wh.sql_relation("timeline_priority")
                + ", 'gmail', 'Re: hello', %s::jsonb)",
                ('{"thread_id": "th1", "thread_ts": "9.9", "chat_id": "c1", "labels": ["INBOX"]}',))
    [(payload,)] = wh._query("SELECT payload->'metadata' FROM @notification_events WHERE event_id = 'me|m1'")
    assert payload == {"thread_id": "th1", "thread_ts": "9.9", "chat_id": "c1"}


def test_capture_carries_a_google_messages_chat_for_the_android_chat_notification(warehouse):
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title, metadata) "
                "VALUES ('android_message', 'pichu|m1', 'direct'::" + wh.sql_relation("timeline_priority")
                + ", 'android_messages', '', %s::jsonb)",
                ('{"chat_key": "rcs-group:G1", "chat_name": "Sam, Alex", "group": true, "protocol": "rcs"}',))
    [(payload,)] = wh._query("SELECT payload->'metadata' FROM @notification_events WHERE event_id = 'pichu|m1'")
    assert payload == {"chat_key": "rcs-group:G1", "chat_name": "Sam, Alex", "group": True}


PUSH_DEVICES_BEFORE_CAPABILITIES = """CREATE TABLE {table} (
    expo_push_token text PRIMARY KEY, client_name text NOT NULL DEFAULT '', device_name text NOT NULL DEFAULT '',
    platform text NOT NULL DEFAULT '', app_version text NOT NULL DEFAULT '', status text NOT NULL DEFAULT 'active',
    error text NOT NULL DEFAULT '', registered_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_sent_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz,
    last_error_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz)"""


def test_ensure_adds_capabilities_to_a_push_device_table_created_before_it(warehouse):
    wh = warehouse
    table = wh.sql_relation("push_devices")
    wh._command(f"CREATE SCHEMA IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    wh._command(PUSH_DEVICES_BEFORE_CAPABILITIES.format(table=table))
    wh._command("INSERT INTO @push_devices (expo_push_token, platform) VALUES ('ExponentPushToken[old]', 'android')")
    wh.ensure_upstream_mutation_tables()
    assert wh._query("SELECT capabilities FROM @push_devices") == [("",)]


def test_go_push_registry_upgrades_and_replaces_capabilities(warehouse):
    wh = warehouse
    table = wh.sql_relation("push_devices")
    wh._command(f"CREATE SCHEMA IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    wh._command(PUSH_DEVICES_BEFORE_CAPABILITIES.format(table=table))
    env = dict(os.environ, PDW_PUSH_TEST_URL=os.environ["POSTGRES_DATABASE_URL"], PDW_PUSH_TEST_TABLE=table)
    result = subprocess.run(["go", "test", "./internal/push", "-run", "TestPostgresStoreCapabilities", "-count=1", "-v"],
                            cwd=Path(__file__).resolve().parents[1] / "app", env=env, text=True, capture_output=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--- SKIP" not in result.stdout


def test_an_old_direct_plus_cc_trigger_is_replaced_by_ensure(warehouse):
    wh = warehouse
    wh._command("DROP TRIGGER timeline_notification_insert ON @timeline_events")
    wh._command("CREATE TRIGGER timeline_notification_insert AFTER INSERT ON @timeline_events "
                "FOR EACH ROW WHEN (NEW.priority IN ('direct','cc')) "
                "EXECUTE FUNCTION @capture_timeline_notification()")
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    insert(wh, "old-cc", "cc")
    assert wh._query("SELECT count(*) FROM @notification_events") == [(1,)]
    wh.ensure_timeline_tables()
    insert(wh, "new-cc", "cc")
    insert(wh, "new-direct", "direct")
    assert wh._query("SELECT event_id FROM @notification_events ORDER BY event_id") == [("new-direct",), ("old-cc",)]


def test_capture_rolls_back_with_source_insert(warehouse):
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    with pytest.raises(Exception):
        wh._command("DO $$ BEGIN INSERT INTO @timeline_events (adapter,event_id,priority) "
                    "VALUES ('test','rollback','direct'); RAISE EXCEPTION 'rollback'; END $$")
    assert wh._query("SELECT count(*) FROM @notification_events") == [(0,)]


def test_go_worker_against_the_same_provisioned_schema(warehouse):
    from personal_data_warehouse.notifications import TABLES
    wh = warehouse
    wh.ensure_upstream_mutation_tables()
    source_tables = ("gmail_messages", "slack_messages", "slack_conversations", "slack_account_identities",
                     "apple_messages", "apple_message_chat_messages", "apple_message_chats",
                     "whatsapp_messages", "whatsapp_chats")
    wh._ensure_table_group(list(source_tables))
    names = (*TABLES, "timeline_events", "push_devices", "marts_notification_health", *source_tables)
    env = dict(os.environ, PDW_NOTIFICATION_TEST_URL=os.environ["POSTGRES_DATABASE_URL"],
               PDW_NOTIFICATION_TEST_RELATIONS=json.dumps({name: wh.sql_relation(name) for name in names}))
    result = subprocess.run(["go", "test", "./internal/notifications", "-run", "TestPostgresNotifications", "-count=1"],
                            cwd=Path(__file__).resolve().parents[1] / "app", env=env, text=True, capture_output=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr

@pytest.mark.parametrize("view,columns", [
    ("marts_notifications", ("occurred_at", "landed_at", "created_at")),
    ("marts_notification_deliveries", ("created_at", "updated_at", "accepted_at", "receipt_at", "opened_at")),
])
def test_notification_marts_translate_every_absent_timestamp(warehouse, view, columns):
    wh = warehouse
    if view == "marts_notifications":
        wh._command("INSERT INTO @notification_events (adapter,event_id,source,priority,event_ts,landed_at,created_at,payload) "
                    "VALUES ('test','epoch','test','direct','epoch','epoch','epoch','{}')")
    else:
        wh._command("INSERT INTO @notification_deliveries (notification_id,device_id,transport,endpoint,created_at,updated_at) "
                    "VALUES ('test','test','expo','{}','epoch','epoch')")
    assert wh._query("SELECT " + ",".join(columns) + " FROM @" + view) == [(None,) * len(columns)]


def test_a_backfilled_direct_event_from_long_ago_never_pages(warehouse):
    """A new source's history lands as fresh INSERTs: years of replies to Zach's
    GitHub PRs are `direct`, and none of them is news. Only recent events page;
    an event with no known time (the epoch) still does."""
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    priority = "'direct'::" + wh.sql_relation("timeline_priority")
    for event_id, ts in (("old", "now() - interval '30 days'"), ("fresh", "now() - interval '2 hours'")):
        wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title, occurred_at) "
                    f"VALUES ('test', %s, {priority}, 'test', 'Hello', {ts})", (event_id,))
    insert(wh, "untimed", "direct")
    assert wh._query("SELECT event_id FROM @notification_events ORDER BY event_id") == [
        ("fresh",), ("untimed",)]
