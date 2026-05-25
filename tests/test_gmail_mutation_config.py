from __future__ import annotations

import sys

import pytest

from personal_data_warehouse import gmail_auth
from personal_data_warehouse.config import (
    CALENDAR_MUTATION_SCOPE,
    CONTACTS_SCOPE,
    GMAIL_COMPOSE_SCOPE,
    GMAIL_MODIFY_SCOPE,
    load_settings,
)


def test_load_settings_requires_gmail_accounts_for_mutation_paths(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "")

    with pytest.raises(ValueError, match="GMAIL_ACCOUNTS"):
        load_settings(require_clickhouse=False, require_gmail=False, require_gmail_mutations=True)

    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_gmail_mutations=True)

    assert settings.gmail_mutation_scopes == (GMAIL_MODIFY_SCOPE,)
    assert settings.gmail_compose_scopes == (GMAIL_COMPOSE_SCOPE,)
    assert GMAIL_MODIFY_SCOPE not in settings.gmail_scopes
    assert GMAIL_COMPOSE_SCOPE not in settings.gmail_scopes
    assert GMAIL_MODIFY_SCOPE not in settings.google_scopes
    assert GMAIL_COMPOSE_SCOPE not in settings.google_scopes


def test_load_settings_requires_contact_accounts_for_contact_mutation_paths(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "")

    with pytest.raises(ValueError, match="CONTACT_GOOGLE_ACCOUNTS"):
        load_settings(require_clickhouse=False, require_gmail=False, require_contact_mutations=True)

    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_clickhouse=False, require_gmail=False, require_contact_mutations=True)

    assert settings.contact_mutation_scopes == (CONTACTS_SCOPE,)
    assert CONTACTS_SCOPE not in settings.contact_scopes
    assert CONTACTS_SCOPE not in settings.google_scopes


def test_run_oauth_flow_includes_gmail_modify_scope_only_when_requested(monkeypatch) -> None:
    scopes_seen = []
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("GOOGLE_ZACH_EXAMPLE_TEST_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"client"}}')

    class FakeCredentials:
        def to_json(self) -> str:
            return '{"token":"token"}'

    class FakeFlow:
        def run_local_server(self, *, port: int):
            assert port == 0
            return FakeCredentials()

    def fake_from_client_config(_client_config, scopes):
        scopes_seen.append(tuple(scopes))
        return FakeFlow()

    monkeypatch.setattr(gmail_auth.InstalledAppFlow, "from_client_config", staticmethod(fake_from_client_config))

    gmail_auth.run_oauth_flow("zach@example.test")
    gmail_auth.run_oauth_flow("zach@example.test", include_gmail_modify=True)
    gmail_auth.run_oauth_flow("zach@example.test", include_contacts_write=True)
    gmail_auth.run_oauth_flow("zach@example.test", include_gmail_compose=True)
    gmail_auth.run_oauth_flow("zach@example.test", include_calendar_write=True)

    assert GMAIL_MODIFY_SCOPE not in scopes_seen[0]
    assert GMAIL_MODIFY_SCOPE in scopes_seen[1]
    assert CONTACTS_SCOPE in scopes_seen[2]
    assert GMAIL_COMPOSE_SCOPE in scopes_seen[3]
    assert CALENDAR_MUTATION_SCOPE not in scopes_seen[0]
    assert CALENDAR_MUTATION_SCOPE in scopes_seen[4]


def test_gmail_auth_cli_passes_include_calendar_write_flag(monkeypatch) -> None:
    calls = []
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("CALENDAR_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("GOOGLE_ZACH_EXAMPLE_TEST_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"client"}}')
    monkeypatch.setattr(sys, "argv", ["prog", "--email", "zach@example.test", "--include-calendar-write"])
    monkeypatch.setattr(
        gmail_auth,
        "run_oauth_flow",
        lambda email_address, *, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write: calls.append(
            (email_address, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write)
        )
        or {},
    )

    gmail_auth.main()

    assert calls == [("zach@example.test", False, False, False, True)]


def test_gmail_auth_cli_passes_include_gmail_modify_flag(monkeypatch, capsys) -> None:
    calls = []
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("GOOGLE_ZACH_EXAMPLE_TEST_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"client"}}')
    monkeypatch.setattr(sys, "argv", ["prog", "--email", "zach@example.test", "--include-gmail-modify"])
    monkeypatch.setattr(
        gmail_auth,
        "run_oauth_flow",
        lambda email_address, *, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write: calls.append(
            (email_address, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write)
        )
        or {},
    )

    gmail_auth.main()

    assert calls == [("zach@example.test", True, False, False, False)]
    assert "Generated OAuth token env var for zach@example.test" in capsys.readouterr().out


def test_gmail_auth_cli_passes_include_gmail_compose_flag(monkeypatch) -> None:
    calls = []
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("GOOGLE_ZACH_EXAMPLE_TEST_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"client"}}')
    monkeypatch.setattr(sys, "argv", ["prog", "--email", "zach@example.test", "--include-gmail-compose"])
    monkeypatch.setattr(
        gmail_auth,
        "run_oauth_flow",
        lambda email_address, *, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write: calls.append(
            (email_address, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write)
        )
        or {},
    )

    gmail_auth.main()

    assert calls == [("zach@example.test", False, True, False, False)]


def test_gmail_auth_cli_passes_include_contacts_write_flag(monkeypatch) -> None:
    calls = []
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("GOOGLE_ZACH_EXAMPLE_TEST_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"client"}}')
    monkeypatch.setattr(sys, "argv", ["prog", "--email", "zach@example.test", "--include-contacts-write"])
    monkeypatch.setattr(
        gmail_auth,
        "run_oauth_flow",
        lambda email_address, *, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write: calls.append(
            (email_address, include_gmail_modify, include_gmail_compose, include_contacts_write, include_calendar_write)
        )
        or {},
    )

    gmail_auth.main()

    assert calls == [("zach@example.test", False, False, True, False)]
