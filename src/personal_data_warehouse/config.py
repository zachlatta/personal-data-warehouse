from __future__ import annotations

from dataclasses import dataclass
import base64
import os
import re

from dotenv import load_dotenv

GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"
DEFAULT_GMAIL_PAGE_SIZE = 500
DEFAULT_GMAIL_ATTACHMENT_MAX_BYTES = 25 * 1024 * 1024
DEFAULT_GMAIL_ATTACHMENT_TEXT_MAX_CHARS = 1_000_000
DEFAULT_GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE = 100
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL = "http://127.0.0.1:11435"
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_MODEL = "qwen3-vl:8b"
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS = 120
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES = 1
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL = True
DEFAULT_CALENDAR_PAGE_SIZE = 2500
DEFAULT_SLACK_PAGE_SIZE = 200
DEFAULT_SLACK_LOOKBACK_DAYS = 14
DEFAULT_SLACK_THREAD_AUDIT_DAYS = 30


def _parse_csv_env(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _parse_bool_env(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _json_env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    encoded_value = os.getenv(f"{name}_B64")
    if encoded_value:
        return base64.b64decode(encoded_value).decode("utf-8")
    return None


def env_slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").upper() or "ACCOUNT"


def email_domain(email_address: str) -> str:
    _, separator, domain = email_address.strip().lower().partition("@")
    if not separator or not domain:
        raise ValueError(f"{email_address} is not a valid email address")
    return domain


@dataclass(frozen=True)
class GmailAccount:
    email_address: str


@dataclass(frozen=True)
class CalendarAccount:
    email_address: str
    calendar_ids: tuple[str, ...]


@dataclass(frozen=True)
class SlackAccount:
    account: str
    token: str
    team_id: str | None


@dataclass(frozen=True)
class Settings:
    clickhouse_url: str | None
    gmail_accounts: tuple[GmailAccount, ...]
    gmail_oauth_client_secrets_json: str | None
    gmail_scopes: tuple[str, ...]
    gmail_page_size: int
    gmail_include_spam_trash: bool
    gmail_force_full_sync: bool
    gmail_full_sync_query: str | None
    gmail_attachment_max_bytes: int
    gmail_attachment_text_max_chars: int
    gmail_attachment_backfill_batch_size: int
    slack_accounts: tuple[SlackAccount, ...]
    slack_page_size: int
    slack_lookback_days: int
    slack_thread_audit_days: int
    slack_force_full_sync: bool
    google_scopes: tuple[str, ...] = (GMAIL_READONLY_SCOPE, CALENDAR_READONLY_SCOPE)
    calendar_accounts: tuple[CalendarAccount, ...] = ()
    calendar_scopes: tuple[str, ...] = (CALENDAR_READONLY_SCOPE,)
    calendar_page_size: int = DEFAULT_CALENDAR_PAGE_SIZE
    calendar_force_full_sync: bool = False
    gmail_attachment_ai_fallback_enabled: bool = True
    gmail_attachment_ai_fallback_base_url: str = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL
    gmail_attachment_ai_fallback_model: str = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_MODEL
    gmail_attachment_ai_fallback_timeout_seconds: int = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS
    gmail_attachment_ai_fallback_pdf_max_pages: int = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES
    gmail_attachment_ai_fallback_pull_model: bool = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL
    google_oauth_client_secrets_json_by_account: tuple[tuple[str, str], ...] = ()
    google_oauth_client_secrets_json_by_domain: tuple[tuple[str, str], ...] = ()

    def account_for_email(self, email_address: str) -> GmailAccount:
        normalized = email_address.strip().lower()
        for account in self.gmail_accounts:
            if account.email_address.lower() == normalized:
                return account
        configured = ", ".join(account.email_address for account in self.gmail_accounts)
        raise ValueError(f"{email_address} is not configured in GMAIL_ACCOUNTS ({configured})")

    def google_oauth_client_secrets_json_for_email(self, email_address: str) -> str | None:
        normalized = email_address.strip().lower()
        account_secrets = dict(self.google_oauth_client_secrets_json_by_account)
        if normalized in account_secrets:
            return account_secrets[normalized]

        domain = email_domain(normalized)
        domain_secrets = dict(self.google_oauth_client_secrets_json_by_domain)
        if domain in domain_secrets:
            return domain_secrets[domain]

        configured_domains = {
            email_domain(account.email_address)
            for account in [*self.gmail_accounts, *self.calendar_accounts]
        }
        if len(configured_domains) <= 1:
            return self.gmail_oauth_client_secrets_json
        return None


def load_settings(
    *,
    require_clickhouse: bool = True,
    require_gmail: bool = True,
    require_gmail_client_secrets: bool = False,
    require_calendar: bool = False,
    require_slack: bool = False,
) -> Settings:
    load_dotenv()

    clickhouse_url = os.getenv("CLICKHOUSE_URL")
    if require_clickhouse and not clickhouse_url:
        raise ValueError("CLICKHOUSE_URL must be set")

    account_emails = _parse_csv_env(os.getenv("GMAIL_ACCOUNTS"))
    if require_gmail and not account_emails:
        raise ValueError("GMAIL_ACCOUNTS must be set to a comma-separated list of mailbox addresses")

    client_secrets_json = _json_env_value("GMAIL_OAUTH_CLIENT_SECRETS_JSON")

    gmail_accounts = tuple(
        GmailAccount(
            email_address=email_address,
        )
        for email_address in account_emails
    )

    calendar_account_emails = _parse_csv_env(os.getenv("CALENDAR_ACCOUNTS")) or account_emails
    if require_calendar and not calendar_account_emails:
        raise ValueError(
            "CALENDAR_ACCOUNTS or GMAIL_ACCOUNTS must be set to a comma-separated list of Google account addresses"
        )
    calendar_accounts = tuple(
        CalendarAccount(
            email_address=email_address,
            calendar_ids=_parse_csv_env(os.getenv(f"CALENDAR_{env_slug(email_address)}_CALENDAR_IDS"))
            or ("primary",),
        )
        for email_address in calendar_account_emails
    )

    google_oauth_client_secrets_json_by_account = tuple(
        (email_address.lower(), account_client_secrets_json)
        for email_address in dict.fromkeys([*account_emails, *calendar_account_emails])
        if (
            account_client_secrets_json := (
                _json_env_value(f"GOOGLE_{env_slug(email_address)}_OAUTH_CLIENT_SECRETS_JSON")
                or _json_env_value(f"GMAIL_{env_slug(email_address)}_OAUTH_CLIENT_SECRETS_JSON")
            )
        )
    )
    google_oauth_client_secrets_json_by_domain = tuple(
        (domain, domain_client_secrets_json)
        for domain in dict.fromkeys(
            email_domain(email_address)
            for email_address in [*account_emails, *calendar_account_emails]
        )
        if (
            domain_client_secrets_json := (
                _json_env_value(f"GOOGLE_DOMAIN_{env_slug(domain)}_OAUTH_CLIENT_SECRETS_JSON")
                or _json_env_value(f"GMAIL_DOMAIN_{env_slug(domain)}_OAUTH_CLIENT_SECRETS_JSON")
            )
        )
    )
    if (
        require_gmail_client_secrets
        and not client_secrets_json
        and not google_oauth_client_secrets_json_by_account
        and not google_oauth_client_secrets_json_by_domain
    ):
        raise ValueError(
            "GMAIL_OAUTH_CLIENT_SECRETS_JSON, GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64, "
            "or domain-specific GOOGLE_DOMAIN_<DOMAIN_SLUG>_OAUTH_CLIENT_SECRETS_JSON_B64 must be set"
        )

    page_size = int(os.getenv("GMAIL_PAGE_SIZE", str(DEFAULT_GMAIL_PAGE_SIZE)))
    if page_size < 1 or page_size > 500:
        raise ValueError("GMAIL_PAGE_SIZE must be between 1 and 500")

    gmail_attachment_max_bytes = int(
        os.getenv("GMAIL_ATTACHMENT_MAX_BYTES", str(DEFAULT_GMAIL_ATTACHMENT_MAX_BYTES))
    )
    if gmail_attachment_max_bytes < 0:
        raise ValueError("GMAIL_ATTACHMENT_MAX_BYTES must be greater than or equal to 0")

    gmail_attachment_text_max_chars = int(
        os.getenv("GMAIL_ATTACHMENT_TEXT_MAX_CHARS", str(DEFAULT_GMAIL_ATTACHMENT_TEXT_MAX_CHARS))
    )
    if gmail_attachment_text_max_chars < 0:
        raise ValueError("GMAIL_ATTACHMENT_TEXT_MAX_CHARS must be greater than or equal to 0")

    gmail_attachment_backfill_batch_size = int(
        os.getenv(
            "GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE",
            str(DEFAULT_GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE),
        )
    )
    if gmail_attachment_backfill_batch_size < 0:
        raise ValueError("GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE must be greater than or equal to 0")

    gmail_attachment_ai_fallback_timeout_seconds = int(
        os.getenv(
            "GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS",
            str(DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS),
        )
    )
    if gmail_attachment_ai_fallback_timeout_seconds < 1:
        raise ValueError("GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS must be at least 1")

    gmail_attachment_ai_fallback_pdf_max_pages = int(
        os.getenv(
            "GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES",
            str(DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES),
        )
    )
    if gmail_attachment_ai_fallback_pdf_max_pages < 1:
        raise ValueError("GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES must be at least 1")

    calendar_page_size = int(os.getenv("CALENDAR_PAGE_SIZE", str(DEFAULT_CALENDAR_PAGE_SIZE)))
    if calendar_page_size < 1 or calendar_page_size > 2500:
        raise ValueError("CALENDAR_PAGE_SIZE must be between 1 and 2500")

    slack_account_names = _parse_csv_env(os.getenv("SLACK_ACCOUNTS"))
    if require_slack and not slack_account_names:
        raise ValueError("SLACK_ACCOUNTS must be set to a comma-separated list of Slack account names")
    slack_accounts: list[SlackAccount] = []
    for account in slack_account_names:
        slug = env_slug(account)
        token_key = f"SLACK_{slug}_TOKEN"
        token = os.getenv(token_key)
        if require_slack and not token:
            raise ValueError(f"{token_key} must be set")
        if token:
            slack_accounts.append(
                SlackAccount(
                    account=account,
                    token=token,
                    team_id=os.getenv(f"SLACK_{slug}_TEAM_ID") or None,
                )
            )

    slack_page_size = int(os.getenv("SLACK_PAGE_SIZE", str(DEFAULT_SLACK_PAGE_SIZE)))
    if slack_page_size < 1 or slack_page_size > 200:
        raise ValueError("SLACK_PAGE_SIZE must be between 1 and 200")

    return Settings(
        clickhouse_url=clickhouse_url,
        gmail_accounts=gmail_accounts,
        gmail_oauth_client_secrets_json=client_secrets_json,
        google_scopes=(GMAIL_READONLY_SCOPE, CALENDAR_READONLY_SCOPE),
        gmail_scopes=(GMAIL_READONLY_SCOPE,),
        gmail_page_size=page_size,
        gmail_include_spam_trash=_parse_bool_env(os.getenv("GMAIL_INCLUDE_SPAM_TRASH"), True),
        gmail_force_full_sync=_parse_bool_env(os.getenv("GMAIL_FORCE_FULL_SYNC"), False),
        gmail_full_sync_query=os.getenv("GMAIL_FULL_SYNC_QUERY") or None,
        gmail_attachment_max_bytes=gmail_attachment_max_bytes,
        gmail_attachment_text_max_chars=gmail_attachment_text_max_chars,
        gmail_attachment_backfill_batch_size=gmail_attachment_backfill_batch_size,
        gmail_attachment_ai_fallback_enabled=_parse_bool_env(
            os.getenv("GMAIL_ATTACHMENT_AI_FALLBACK_ENABLED"),
            True,
        ),
        gmail_attachment_ai_fallback_base_url=os.getenv("GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL")
        or DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL,
        gmail_attachment_ai_fallback_model=os.getenv("GMAIL_ATTACHMENT_AI_FALLBACK_MODEL")
        or DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_MODEL,
        gmail_attachment_ai_fallback_timeout_seconds=gmail_attachment_ai_fallback_timeout_seconds,
        gmail_attachment_ai_fallback_pdf_max_pages=gmail_attachment_ai_fallback_pdf_max_pages,
        gmail_attachment_ai_fallback_pull_model=_parse_bool_env(
            os.getenv("GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL"),
            DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL,
        ),
        google_oauth_client_secrets_json_by_account=google_oauth_client_secrets_json_by_account,
        google_oauth_client_secrets_json_by_domain=google_oauth_client_secrets_json_by_domain,
        calendar_accounts=calendar_accounts,
        calendar_scopes=(CALENDAR_READONLY_SCOPE,),
        calendar_page_size=calendar_page_size,
        calendar_force_full_sync=_parse_bool_env(os.getenv("CALENDAR_FORCE_FULL_SYNC"), False),
        slack_accounts=tuple(slack_accounts),
        slack_page_size=slack_page_size,
        slack_lookback_days=int(os.getenv("SLACK_LOOKBACK_DAYS", str(DEFAULT_SLACK_LOOKBACK_DAYS))),
        slack_thread_audit_days=int(
            os.getenv("SLACK_THREAD_AUDIT_DAYS", str(DEFAULT_SLACK_THREAD_AUDIT_DAYS))
        ),
        slack_force_full_sync=_parse_bool_env(os.getenv("SLACK_FORCE_FULL_SYNC"), False),
    )
