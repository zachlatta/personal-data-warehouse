"""The Slack freshness pass polls: nothing tells PDW which conversations moved."""

from __future__ import annotations


class NullLog:
    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def _settings(monkeypatch):
    from personal_data_warehouse.config import load_settings

    monkeypatch.setenv("SLACK_ACCOUNTS", "zrl")
    monkeypatch.setenv("SLACK_ZRL_TOKEN", "xoxp-test-token")
    return load_settings(require_postgres=False, require_gmail=False, require_slack=True)


class _Warehouse:
    def __init__(self):
        self.deleted = []

    def delete_slack_sync_state(self, account, team_id, object_type, object_id):
        self.deleted.append((account, team_id, object_type, object_id))


def test_the_freshness_pass_polls_and_lists_direct_conversations(monkeypatch):
    """client.counts answered team_is_restricted to a fresh, valid session on
    2026-10-01 and was removed: the pass polls every conversation when it is due
    and finds new DMs and group DMs by listing them, never by a change feed."""
    from personal_data_warehouse.defs import slack_sync as slack_defs

    captured: list[dict] = []

    class _Runner:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def sync_all(self):
            return []

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", _Runner)
    monkeypatch.setenv("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", "0")
    warehouse = _Warehouse()

    slack_defs.run_slack_freshness_sync(settings=_settings(monkeypatch), warehouse=warehouse, logger=NullLog())

    assert captured and all("conversation_ids" not in kwargs for kwargs in captured)
    assert all(kwargs["discover_new_direct_conversations"] is True for kwargs in captured)
    # The retired feed's last verdict (action_required) must not keep the Slack
    # pipeline in attention forever.
    assert warehouse.deleted == [("zrl", "", "change_feed", "client.counts")]
    assert not hasattr(slack_defs, "slack_change_plan")


def test_the_freshness_pass_polls_with_the_pasted_session_when_one_exists(monkeypatch):
    from personal_data_warehouse.defs import slack_sync as slack_defs
    from personal_data_warehouse.slack_sync import SlackSessionFallbackClient, SlackWebApiClient

    captured: list[dict] = []

    class _Runner:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def sync_all(self):
            return []

    class _SessionWarehouse(_Warehouse):
        def load_slack_session(self, *, account, session_key="default"):
            return {"session_token": "xoxc-t", "session_cookie": "xoxd-c", "user_agent": "Mozilla/5.0"}

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", _Runner)
    monkeypatch.setenv("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", "0")
    settings = _settings(monkeypatch)
    slack_defs.run_slack_freshness_sync(settings=settings, warehouse=_SessionWarehouse(), logger=NullLog())

    factory = captured[0]["client_factory"]
    client = factory(settings.slack_accounts[0])
    assert isinstance(client, SlackSessionFallbackClient)
    assert isinstance(client._oauth, SlackWebApiClient)


def test_without_a_session_the_freshness_pass_uses_the_oauth_token(monkeypatch):
    from personal_data_warehouse.defs import slack_sync as slack_defs

    captured: list[dict] = []

    class _Runner:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def sync_all(self):
            return []

    class _NoSession(_Warehouse):
        def load_slack_session(self, *, account, session_key="default"):
            return {}

    monkeypatch.setattr(slack_defs, "SlackSyncRunner", _Runner)
    monkeypatch.setenv("SLACK_ASSET_READ_STATE_WITH_FRESHNESS", "0")
    slack_defs.run_slack_freshness_sync(settings=_settings(monkeypatch), warehouse=_NoSession(), logger=NullLog())
    assert captured[0]["client_factory"] is None
