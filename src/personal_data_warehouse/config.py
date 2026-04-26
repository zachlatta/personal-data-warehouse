from __future__ import annotations

from dataclasses import dataclass
import base64
import os
import re

from dotenv import load_dotenv

GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
DEFAULT_GMAIL_PAGE_SIZE = 500
DEFAULT_GMAIL_ATTACHMENT_MAX_BYTES = 25 * 1024 * 1024
DEFAULT_GMAIL_ATTACHMENT_TEXT_MAX_CHARS = 1_000_000
DEFAULT_GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE = 100
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


@dataclass(frozen=True)
class GmailAccount:
    email_address: str


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

    def account_for_email(self, email_address: str) -> GmailAccount:
        normalized = email_address.strip().lower()
        for account in self.gmail_accounts:
            if account.email_address.lower() == normalized:
                return account
        configured = ", ".join(account.email_address for account in self.gmail_accounts)
        raise ValueError(f"{email_address} is not configured in GMAIL_ACCOUNTS ({configured})")


def load_settings(
    *,
    require_clickhouse: bool = True,
    require_gmail: bool = True,
    require_gmail_client_secrets: bool = False,
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
    if require_gmail_client_secrets and not client_secrets_json:
        raise ValueError("GMAIL_OAUTH_CLIENT_SECRETS_JSON or GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64 must be set")

    gmail_accounts = tuple(
        GmailAccount(
            email_address=email_address,
        )
        for email_address in account_emails
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
        gmail_scopes=(GMAIL_READONLY_SCOPE,),
        gmail_page_size=page_size,
        gmail_include_spam_trash=_parse_bool_env(os.getenv("GMAIL_INCLUDE_SPAM_TRASH"), True),
        gmail_force_full_sync=_parse_bool_env(os.getenv("GMAIL_FORCE_FULL_SYNC"), False),
        gmail_full_sync_query=os.getenv("GMAIL_FULL_SYNC_QUERY") or None,
        gmail_attachment_max_bytes=gmail_attachment_max_bytes,
        gmail_attachment_text_max_chars=gmail_attachment_text_max_chars,
        gmail_attachment_backfill_batch_size=gmail_attachment_backfill_batch_size,
        slack_accounts=tuple(slack_accounts),
        slack_page_size=slack_page_size,
        slack_lookback_days=int(os.getenv("SLACK_LOOKBACK_DAYS", str(DEFAULT_SLACK_LOOKBACK_DAYS))),
        slack_thread_audit_days=int(
            os.getenv("SLACK_THREAD_AUDIT_DAYS", str(DEFAULT_SLACK_THREAD_AUDIT_DAYS))
        ),
        slack_force_full_sync=_parse_bool_env(os.getenv("SLACK_FORCE_FULL_SYNC"), False),
    )
