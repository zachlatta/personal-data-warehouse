"""Notification capture is transactional, insert-only, and never a history replay."""
import os
import json
import re
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


def test_a_direct_trigger_that_pages_phone_notifications_is_replaced_by_ensure(warehouse):
    wh = warehouse
    wh._command("DROP TRIGGER timeline_notification_insert ON @timeline_events")
    wh._command("CREATE TRIGGER timeline_notification_insert AFTER INSERT ON @timeline_events "
                "FOR EACH ROW WHEN (NEW.priority = 'direct') "
                "EXECUTE FUNCTION @capture_timeline_notification()")
    wh.ensure_timeline_tables()
    # A current trigger is left alone: no timeline-wide lock at every sync.
    trigger = ("SELECT oid FROM pg_trigger WHERE tgname = 'timeline_notification_insert' "
               "AND tgrelid = '" + wh.sql_relation("timeline_events") + "'::regclass")
    [(replaced,)] = wh._query(trigger)
    wh.ensure_timeline_tables()
    assert wh._query(trigger) == [(replaced,)]
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title) "
                "VALUES ('android_notification', 'pichu|n1', 'direct'::" + wh.sql_relation("timeline_priority")
                + ", 'android_notifications', 'PDW: Sam')")
    insert(wh, "direct", "direct")
    assert wh._query("SELECT event_id FROM @notification_events") == [("direct",)]


def test_a_captured_phone_notification_never_pages(warehouse):
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    for adapter, event_id in (("android_notification", "pichu|n1"), ("slack_message", "T1|C1|1.0")):
        wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title) "
                    "VALUES (%s, %s, 'direct'::" + wh.sql_relation("timeline_priority") + ", 'test', 'Hi')",
                    (adapter, event_id))
    assert wh._query("SELECT event_id FROM @notification_events") == [("T1|C1|1.0",)]


def insert_text(wh, message_key, chat_key, at, account="me", device="pichu"):
    wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title, occurred_at, source_pk, metadata) "
                "VALUES ('android_message', %s, 'direct'::" + wh.sql_relation("timeline_priority")
                + ", 'android_messages', '', %s::timestamptz, %s::jsonb, %s::jsonb)",
                (f"{account}|{device}|{message_key}", at,
                 json.dumps({"account": account, "device": device, "message_key": message_key}),
                 json.dumps({"device": device, "chat_key": chat_key})))


def test_a_text_uploaded_again_under_its_telephony_key_pages_once(warehouse):
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    [(at, later)] = wh._query("SELECT (now() - interval '2 minutes')::text, (now() - interval '1 minute')::text")
    insert_text(wh, "bugle:7", "+15550100", at)
    insert_text(wh, "content://sms/7", "+15550100", at)
    insert_text(wh, "content://sms/8", "+15550100", later)
    insert_text(wh, "content://sms/9", "+15550111", at)
    insert_text(wh, "content://sms/10", "+15550100", at, device="other")
    assert wh._query("SELECT event_id FROM @notification_events ORDER BY event_id") == [
        ("me|other|content://sms/10",), ("me|pichu|bugle:7",), ("me|pichu|content://sms/8",),
        ("me|pichu|content://sms/9",)]


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
                     "whatsapp_messages", "whatsapp_chats", "android_notification_events")
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


def test_a_scan_pages_although_it_is_zachs_own_act(warehouse):
    wh = warehouse
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    for adapter, event_id in (("scan_document", "me|scan-1"), ("photo", "ph_1")):
        wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title) "
                    "VALUES (%s, %s, 'self'::" + wh.sql_relation("timeline_priority") + ", 'test', 'Color scan, 2 pages')",
                    (adapter, event_id))
    assert wh._query("SELECT event_id, priority FROM @notification_events") == [("me|scan-1", "self")]


def test_an_outbox_made_before_scans_paged_accepts_them_after_ensure(warehouse):
    wh = warehouse
    wh._command("ALTER TABLE @notification_events DROP CONSTRAINT notification_events_priority_check")
    wh._command("ALTER TABLE @notification_events ADD CONSTRAINT notification_events_priority_check "
                "CHECK (priority IN ('direct','cc'))")
    wh._command("DROP TRIGGER timeline_notification_insert ON @timeline_events")
    wh._command("CREATE TRIGGER timeline_notification_insert AFTER INSERT ON @timeline_events "
                "FOR EACH ROW WHEN (NEW.priority = 'direct' AND NEW.adapter <> 'android_notification') "
                "EXECUTE FUNCTION @capture_timeline_notification()")
    wh.ensure_timeline_tables()
    wh._command("UPDATE @notification_state SET enabled = 1 WHERE id = 'timeline'")
    wh._command("INSERT INTO @timeline_events (adapter, event_id, priority, source, title) "
                "VALUES ('scan_document', 'me|scan-2', 'self'::" + wh.sql_relation("timeline_priority")
                + ", 'scans', 'Black & white scan, 1 page')")
    insert(wh, "direct", "direct")
    assert wh._query("SELECT event_id FROM @notification_events ORDER BY event_id") == [("direct",), ("me|scan-2",)]


def _seed_android_delivery(wh, delivery_id, accepted_sql, payload, status="provider_accepted", platform="android"):
    wh._command(
        f"INSERT INTO @notification_deliveries (id, notification_id, device_id, transport, endpoint, payload, status, accepted_at, updated_at) "
        f"VALUES (%s, %s, 'pichu', 'expo', %s::jsonb, %s::jsonb, %s, {accepted_sql}, {accepted_sql})",
        (delivery_id, delivery_id, json.dumps({"platform": platform}), json.dumps(payload), status),
    )


def _seed_phone_event(
    wh,
    event_id,
    event_sql,
    synced_sql="now()",
    package_name="com.zachlatta.pdw",
    channel_id="messages",
    notification_tag="pdw-chat",
    event_type="posted",
    title="",
    text="",
    messages=None,
    raw_json=None,
):
    wh._command(
        f"INSERT INTO @android_notification_events "
        f"(account, device, event_id, event_type, event_at, posted_at, synced_at, package_name, channel_id, notification_tag, title, text, messages, raw_json) "
        f"VALUES ('me', 'pichu', %s, %s, {event_sql}, {event_sql}, {synced_sql}, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)",
        (
            event_id,
            event_type,
            package_name,
            channel_id,
            notification_tag,
            title,
            text,
            json.dumps(messages or []),
            json.dumps(raw_json or {}),
        ),
    )


def test_notification_health_counts_matched_and_missing_android_alerts_after_the_settle_window(warehouse):
    wh = warehouse
    wh._command(
        "UPDATE @notification_state SET enabled = 1, status = 'ok', last_run_at = now(), error = '' WHERE id = 'timeline'"
    )
    # Advance the phone upload watermark to now() so deliveries older than 15m are evaluated.
    _seed_phone_event(
        wh,
        "watermark",
        "now()",
        synced_sql="now()",
        package_name="com.Slack",
        channel_id="other",
        text="heartbeat",
    )
    # 1. Chat matched via pdw.event_ids newline-delimited extra.
    _seed_android_delivery(
        wh,
        "d-chat-ids",
        "now() - interval '2 hours'",
        {"body": "First body", "android_style": {"event_id": "slack|1", "conversation_id": "c1"}},
    )
    _seed_phone_event(
        wh,
        "p-chat-ids",
        "now() - interval '2 hours' + interval '2 seconds'",
        text="Overwritten later text",
        raw_json={"extras": {"pdw.event_ids": "slack|0\nslack|1\nslack|2"}},
    )
    # 2. Chat matched via MessagingStyle messages[] array element.
    _seed_android_delivery(
        wh,
        "d-chat-msgs",
        "now() - interval '90 minutes'",
        {"body": "Stacked reply", "android_style": {"event_id": "slack|3", "conversation_id": "c2"}},
    )
    _seed_phone_event(
        wh,
        "p-chat-msgs",
        "now() - interval '90 minutes' + interval '1 second'",
        text="Newest top text",
        messages=[{"text": "Stacked reply", "time_ms": 1}],
    )
    # 3. Non-chat matched on channel_id = 'default' by title + text.
    _seed_android_delivery(
        wh,
        "d-plain-ok",
        "now() - interval '80 minutes'",
        {"title": "Gmail · Finance", "body": "Quarterly numbers"},
    )
    _seed_phone_event(
        wh,
        "p-plain-ok",
        "now() - interval '80 minutes' + interval '3 seconds'",
        channel_id="default",
        notification_tag="pdw-alert",
        title="Gmail · Finance",
        text="Quarterly numbers",
    )
    # 4. Delivery inside the 15-minute settle window is excluded, not counted as missing.
    _seed_android_delivery(
        wh,
        "d-recent-unsettled",
        "now() - interval '5 minutes'",
        {"body": "Still in flight", "android_style": {"event_id": "slack|recent", "conversation_id": "c3"}},
    )
    assert wh._query(
        "SELECT status, error, android_accepted_24h, android_posted_24h, android_missing_24h FROM @marts_notification_health"
    ) == [("ok", "", 3, 3, 0)]

    # 5. Add a missing chat alert (dropped by Android's 50-notification cap): health turns 'attention' and names the count.
    _seed_android_delivery(
        wh,
        "d-chat-lost",
        "now() - interval '60 minutes'",
        {"body": "Dropped under cap", "android_style": {"event_id": "slack|lost", "conversation_id": "c4"}},
    )
    assert wh._query(
        "SELECT status, error, android_accepted_24h, android_posted_24h, android_missing_24h FROM @marts_notification_health"
    ) == [("attention", "1 of 4 Android alerts accepted in 24h never appeared on the phone", 4, 3, 1)]


HEALTH_COLUMNS = "status, error, android_accepted_24h, android_posted_24h, android_missing_24h"
ONE_LOST = ("attention", "1 of 1 Android alerts accepted in 24h never appeared on the phone", 1, 0, 1)
NOTHING_EVALUATED = ("ok", "", 0, 0, 0)


def _android_health(wh):
    return wh._query(f"SELECT {HEALTH_COLUMNS} FROM @marts_notification_health")[0]


def _start_notifications(wh):
    wh._command(
        "UPDATE @notification_state SET enabled = 1, status = 'ok', last_run_at = now(), error = '' WHERE id = 'timeline'"
    )


def _seed_upload(wh, event_id, event_sql="now()", synced_sql="now()"):
    """A row from another app: it moves the upload watermark and can never match an alert."""
    _seed_phone_event(
        wh, event_id, event_sql, synced_sql=synced_sql, package_name="com.Slack", channel_id="other", text="heartbeat"
    )


CHAT_ALERT = (
    {"body": "Same words", "android_style": {"event_id": "slack|1", "conversation_id": "c1"}},
    {"channel_id": "messages", "notification_tag": "pdw-chat", "text": "Same words"},
    {"channel_id": "default", "notification_tag": "pdw-alert"},
)
PLAIN_ALERT = (
    {"title": "Gmail · Finance", "body": "Same words"},
    {"channel_id": "default", "notification_tag": "pdw-alert", "title": "Gmail · Finance", "text": "Same words"},
    {"channel_id": "messages", "notification_tag": "pdw-chat"},
)
ALERT_KINDS = pytest.mark.parametrize("payload,row,wrong_channel", [CHAT_ALERT, PLAIN_ALERT], ids=["chat", "plain"])
ALERT_ACCEPTED = "now() - interval '20 hours'"
RIGHT_AFTER_ACCEPTED = f"{ALERT_ACCEPTED} + interval '2 seconds'"


@ALERT_KINDS
@pytest.mark.parametrize("event_type", ["posted", "active"])
def test_notification_health_counts_the_alerts_own_phone_row_as_arrival(warehouse, payload, row, wrong_channel, event_type):
    wh = warehouse
    _start_notifications(wh)
    _seed_upload(wh, "upload")
    _seed_android_delivery(wh, "d", ALERT_ACCEPTED, payload)
    _seed_phone_event(wh, "p", RIGHT_AFTER_ACCEPTED, event_type=event_type, **row)
    assert _android_health(wh) == ("ok", "", 1, 1, 0)


@ALERT_KINDS
@pytest.mark.parametrize(
    "not_arrival",
    [
        pytest.param({"package_name": "com.Slack"}, id="another-package"),
        pytest.param({"event_type": "removed"}, id="a-removal"),
        pytest.param({"event_at": f"{ALERT_ACCEPTED} - interval '1 minute'"}, id="before-the-push"),
        pytest.param({"event_at": f"{ALERT_ACCEPTED} + interval '13 hours'"}, id="a-later-message-past-12h"),
    ],
)
def test_notification_health_does_not_take_a_same_text_row_for_the_alert(warehouse, payload, row, wrong_channel, not_arrival):
    wh = warehouse
    _start_notifications(wh)
    _seed_upload(wh, "upload")
    _seed_android_delivery(wh, "d", ALERT_ACCEPTED, payload)
    changes = dict(not_arrival)
    _seed_phone_event(wh, "p", changes.pop("event_at", RIGHT_AFTER_ACCEPTED), **{**row, **changes})
    assert _android_health(wh) == ONE_LOST


@ALERT_KINDS
def test_notification_health_does_not_take_another_channels_row_for_the_alert(warehouse, payload, row, wrong_channel):
    wh = warehouse
    _start_notifications(wh)
    _seed_upload(wh, "upload")
    _seed_android_delivery(wh, "d", ALERT_ACCEPTED, payload)
    _seed_phone_event(wh, "p", RIGHT_AFTER_ACCEPTED, **{**row, **wrong_channel})
    assert _android_health(wh) == ONE_LOST


def test_notification_health_matches_a_plain_alert_on_its_title_too(warehouse):
    wh = warehouse
    _start_notifications(wh)
    _seed_upload(wh, "upload")
    payload, row, _ = PLAIN_ALERT
    _seed_android_delivery(wh, "d", ALERT_ACCEPTED, payload)
    _seed_phone_event(wh, "p", RIGHT_AFTER_ACCEPTED, **{**row, "title": "Gmail · Someone else"})
    assert _android_health(wh) == ONE_LOST


@pytest.mark.parametrize(
    "lagging_upload",
    [
        pytest.param({"event_sql": "now()", "synced_sql": "now() - interval '2 hours'"}, id="synced_at"),
        pytest.param({"event_sql": "now() - interval '2 hours'", "synced_sql": "now()"}, id="event_at"),
    ],
)
def test_an_offline_stretch_is_not_lost_alerts_until_the_phone_uploads_past_them(warehouse, lagging_upload):
    wh = warehouse
    _start_notifications(wh)
    _seed_android_delivery(wh, "d", "now() - interval '1 hour'", CHAT_ALERT[0])
    # A phone that has never uploaded proves nothing about any alert.
    assert _android_health(wh) == NOTHING_EVALUATED
    # The phone's newest upload is older than the alert, by either clock.
    _seed_upload(wh, "lagging", **lagging_upload)
    assert _android_health(wh) == NOTHING_EVALUATED
    # Uploads have moved past it and the alert is still not there: now it counts.
    _seed_upload(wh, "caught-up")
    assert _android_health(wh) == ONE_LOST


def test_an_ios_copy_of_a_chat_alert_is_not_counted_against_the_android_phone(warehouse):
    wh = warehouse
    _start_notifications(wh)
    _seed_upload(wh, "upload")
    _seed_android_delivery(wh, "d", ALERT_ACCEPTED, CHAT_ALERT[0], platform="ios")
    assert _android_health(wh) == NOTHING_EVALUATED


@pytest.mark.parametrize("status", ["accepted", "suppressed"])
def test_a_delivery_expo_has_not_confirmed_is_not_counted_against_the_phone(warehouse, status):
    wh = warehouse
    _start_notifications(wh)
    _seed_upload(wh, "upload")
    _seed_android_delivery(wh, "d", ALERT_ACCEPTED, CHAT_ALERT[0], status=status)
    assert _android_health(wh) == NOTHING_EVALUATED


@pytest.mark.parametrize(
    "surface",
    [
        "mobile/src/app/(tabs)/notifications.tsx",
        "mobile/src/lib/api.ts",
        "app/internal/webapp/static/notifications.js",
        "app/internal/server/pipelines_page.go",
        "app/internal/guide/topics/ops.md",
        "src/personal_data_warehouse/warehouse_catalog.json",
    ],
)
def test_no_surface_says_cc_events_page_when_the_trigger_captures_direct_and_scans(surface):
    text = (Path(__file__).resolve().parent.parent / surface).read_text()
    assert not re.search(r"direct\s*(and|or|/|\+)\s*cc", text, re.IGNORECASE)
