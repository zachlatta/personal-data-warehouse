from __future__ import annotations

import sys

import pytest

from personal_data_warehouse import gmail_auth
from personal_data_warehouse.config import (
    CALENDAR_MUTATION_SCOPE,
    CALENDAR_READONLY_SCOPE,
    CONTACTS_READONLY_SCOPE,
    CONTACTS_SCOPE,
    GMAIL_COMPOSE_SCOPE,
    GMAIL_MODIFY_SCOPE,
    GMAIL_READONLY_SCOPE,
    GMAIL_SETTINGS_BASIC_SCOPE,
    GOOGLE_DRIVE_SCOPE,
    load_settings,
)


def test_load_settings_requires_gmail_accounts_for_mutation_paths(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "")

    with pytest.raises(ValueError, match="GMAIL_ACCOUNTS"):
        load_settings(require_postgres=False, require_gmail=False, require_gmail_mutations=True)

    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_postgres=False, require_gmail=False, require_gmail_mutations=True)

    assert settings.gmail_mutation_scopes == (GMAIL_MODIFY_SCOPE,)
    assert settings.gmail_compose_scopes == (GMAIL_COMPOSE_SCOPE,)
    # Filters need gmail.settings.basic; it must never ride on the scopes
    # every archive and send requests, or a token without it blocks them all.
    assert settings.gmail_settings_scopes == (GMAIL_SETTINGS_BASIC_SCOPE,)
    assert GMAIL_SETTINGS_BASIC_SCOPE not in settings.gmail_mutation_scopes
    assert GMAIL_SETTINGS_BASIC_SCOPE not in settings.gmail_compose_scopes
    assert GMAIL_MODIFY_SCOPE not in settings.gmail_scopes
    assert GMAIL_COMPOSE_SCOPE not in settings.gmail_scopes
    assert GMAIL_MODIFY_SCOPE not in settings.google_scopes
    assert GMAIL_COMPOSE_SCOPE not in settings.google_scopes


def test_load_settings_requires_contact_accounts_for_contact_mutation_paths(monkeypatch) -> None:
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "")

    with pytest.raises(ValueError, match="CONTACT_GOOGLE_ACCOUNTS"):
        load_settings(require_postgres=False, require_gmail=False, require_contact_mutations=True)

    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_postgres=False, require_gmail=False, require_contact_mutations=True)

    assert settings.contact_mutation_scopes == (CONTACTS_SCOPE,)
    assert CONTACTS_SCOPE not in settings.contact_scopes
    assert CONTACTS_SCOPE not in settings.google_scopes


def test_requested_scopes_add_write_scopes_only_when_asked(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_postgres=False, require_gmail=False)
    base = gmail_auth.requested_scopes(settings)
    assert GMAIL_MODIFY_SCOPE not in base and GMAIL_SETTINGS_BASIC_SCOPE not in base
    assert GMAIL_MODIFY_SCOPE in gmail_auth.requested_scopes(settings, include_gmail_modify=True)
    assert CONTACTS_SCOPE in gmail_auth.requested_scopes(settings, include_contacts_write=True)
    assert GMAIL_COMPOSE_SCOPE in gmail_auth.requested_scopes(settings, include_gmail_compose=True)
    assert CALENDAR_MUTATION_SCOPE in gmail_auth.requested_scopes(settings, include_calendar_write=True)
    settings_only = gmail_auth.requested_scopes(settings, include_gmail_settings=True)
    assert GMAIL_SETTINGS_BASIC_SCOPE in settings_only
    assert GMAIL_MODIFY_SCOPE not in settings_only


def test_production_scopes_request_every_scope_prod_uses_whatever_the_local_env(monkeypatch) -> None:
    # A re-consent REPLACES the whole grant. On 2026-07-15 one that omitted the
    # write flags silently dropped every write scope, so --production-scopes is
    # the full set prod holds, independent of which sources this .env enables.
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    settings = load_settings(require_postgres=False, require_gmail=False)
    scopes = gmail_auth.requested_scopes(settings, production=True)
    assert set(scopes) == set(gmail_auth.PRODUCTION_GOOGLE_SCOPES)
    for scope in (
        GMAIL_READONLY_SCOPE, GMAIL_MODIFY_SCOPE, GMAIL_COMPOSE_SCOPE, GMAIL_SETTINGS_BASIC_SCOPE,
        CALENDAR_READONLY_SCOPE, CALENDAR_MUTATION_SCOPE, CONTACTS_READONLY_SCOPE, CONTACTS_SCOPE,
        GOOGLE_DRIVE_SCOPE,
    ):
        assert scope in scopes


def test_run_oauth_flow_requests_exactly_the_given_scopes(monkeypatch) -> None:
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
    values = gmail_auth.run_oauth_flow("zach@example.test", scopes=(GMAIL_READONLY_SCOPE, GMAIL_SETTINGS_BASIC_SCOPE))
    assert scopes_seen == [(GMAIL_READONLY_SCOPE, GMAIL_SETTINGS_BASIC_SCOPE)]
    assert set(values) == {"GOOGLE_ZACH_EXAMPLE_TEST_TOKEN_JSON_B64", "GMAIL_ZACH_EXAMPLE_TEST_TOKEN_JSON_B64"}


@pytest.mark.parametrize(
    ("flag", "expected"),
    [
        ("--include-gmail-modify", {GMAIL_MODIFY_SCOPE}),
        ("--include-gmail-compose", {GMAIL_COMPOSE_SCOPE}),
        ("--include-contacts-write", {CONTACTS_SCOPE}),
        ("--include-calendar-write", {CALENDAR_MUTATION_SCOPE}),
        ("--include-gmail-settings", {GMAIL_SETTINGS_BASIC_SCOPE}),
        ("--production-scopes", set()),
    ],
)
def test_gmail_auth_cli_flags_choose_the_requested_scopes(monkeypatch, capsys, flag, expected) -> None:
    calls = []
    monkeypatch.setenv("GMAIL_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("CONTACT_GOOGLE_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("CALENDAR_ACCOUNTS", "zach@example.test")
    monkeypatch.setenv("GOOGLE_ZACH_EXAMPLE_TEST_OAUTH_CLIENT_SECRETS_JSON", '{"installed":{"client_id":"client"}}')
    monkeypatch.setattr(sys, "argv", ["prog", "--email", "zach@example.test", flag])
    monkeypatch.setattr(gmail_auth, "run_oauth_flow", lambda email_address, *, scopes: calls.append((email_address, scopes)) or {})

    gmail_auth.main()

    assert len(calls) == 1 and calls[0][0] == "zach@example.test"
    scopes = set(calls[0][1])
    if flag == "--production-scopes":
        assert scopes == set(gmail_auth.PRODUCTION_GOOGLE_SCOPES)
    else:
        assert expected <= scopes
        assert GMAIL_SETTINGS_BASIC_SCOPE not in scopes or flag == "--include-gmail-settings"
    out = capsys.readouterr().out
    assert "Generated OAuth token env var for zach@example.test" in out
    # Every run says what the new grant holds, because it replaces the old one.
    assert "replaces the account's whole Google grant" in out
