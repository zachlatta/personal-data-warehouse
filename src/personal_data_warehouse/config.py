from __future__ import annotations

from dataclasses import dataclass
import base64
import os
import re

from dotenv import load_dotenv

from personal_data_warehouse.agent_runner import default_agent_docker_image, default_agent_tool_proxy_public_host

GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"
GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
DEFAULT_GMAIL_PAGE_SIZE = 500
DEFAULT_GMAIL_ATTACHMENT_MAX_BYTES = 25 * 1024 * 1024
DEFAULT_GMAIL_ATTACHMENT_TEXT_MAX_CHARS = 1_000_000
DEFAULT_GMAIL_ATTACHMENT_BACKFILL_BATCH_SIZE = 100
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL = "http://127.0.0.1:11435"
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_MODEL = "qwen3-vl:2b"
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS = 60
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES = 1
DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL = True
DEFAULT_CALENDAR_PAGE_SIZE = 2500
DEFAULT_CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS = 365
DEFAULT_CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS = 365
DEFAULT_CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES = 60
DEFAULT_SLACK_PAGE_SIZE = 200
DEFAULT_SLACK_LOOKBACK_DAYS = 14
DEFAULT_SLACK_THREAD_AUDIT_DAYS = 30
DEFAULT_VOICE_MEMOS_EXTENSIONS = (".m4a", ".qta")
DEFAULT_VOICE_MEMOS_STORAGE_BACKEND = "google_drive"
DEFAULT_VOICE_MEMOS_TRANSCRIPTION_PROVIDER = "assemblyai"
DEFAULT_ASSEMBLYAI_BASE_URL = "https://api.assemblyai.com"
DEFAULT_ASSEMBLYAI_POLL_INTERVAL_SECONDS = 5
DEFAULT_ASSEMBLYAI_TIMEOUT_SECONDS = 1800
DEFAULT_ASSEMBLYAI_MIN_SPEAKERS_EXPECTED = 1
DEFAULT_ASSEMBLYAI_MAX_SPEAKERS_EXPECTED = 8
DEFAULT_AGENT_PROVIDER = "codex"
DEFAULT_AGENT_AUTH_VOLUME = "pdw-agent-auth"
DEFAULT_AGENT_RUNS_VOLUME = "pdw-agent-runs"
DEFAULT_AGENT_RUNS_DIR = "/agent-runs"
DEFAULT_AGENT_TIMEOUT_SECONDS = 1800
DEFAULT_AGENT_DOCKER_NETWORK = "bridge"
DEFAULT_AGENT_DOCKER_MEMORY = "4g"
DEFAULT_AGENT_DOCKER_CPUS = "2"
DEFAULT_AGENT_DOCKER_PIDS_LIMIT = 512
DEFAULT_AGENT_TOOL_PROXY_BIND_HOST = "0.0.0.0"
DEFAULT_AGENT_TOOL_PROXY_PUBLIC_HOST = "host.docker.internal"


def _parse_csv_env(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _parse_bool_env(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_optional_int_env(name: str, default: int | None) -> int | None:
    value = os.getenv(name)
    if value is None:
        return default
    if not value.strip():
        return None
    return int(value)


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
class VoiceMemosConfig:
    account: str
    recordings_path: str
    extensions: tuple[str, ...]
    storage_backend: str
    google_drive_folder_id: str
    transcription_provider: str = DEFAULT_VOICE_MEMOS_TRANSCRIPTION_PROVIDER


@dataclass(frozen=True)
class AssemblyAIConfig:
    api_key: str
    base_url: str = DEFAULT_ASSEMBLYAI_BASE_URL
    poll_interval_seconds: int = DEFAULT_ASSEMBLYAI_POLL_INTERVAL_SECONDS
    timeout_seconds: int = DEFAULT_ASSEMBLYAI_TIMEOUT_SECONDS
    min_speakers_expected: int | None = DEFAULT_ASSEMBLYAI_MIN_SPEAKERS_EXPECTED
    max_speakers_expected: int | None = DEFAULT_ASSEMBLYAI_MAX_SPEAKERS_EXPECTED


@dataclass(frozen=True)
class AgentConfig:
    provider: str
    model: str
    docker_image: str
    auth_volume: str = DEFAULT_AGENT_AUTH_VOLUME
    runs_volume: str = DEFAULT_AGENT_RUNS_VOLUME
    runs_dir: str = DEFAULT_AGENT_RUNS_DIR
    timeout_seconds: int = DEFAULT_AGENT_TIMEOUT_SECONDS
    docker_network: str = DEFAULT_AGENT_DOCKER_NETWORK
    docker_memory: str = DEFAULT_AGENT_DOCKER_MEMORY
    docker_cpus: str = DEFAULT_AGENT_DOCKER_CPUS
    docker_pids_limit: int = DEFAULT_AGENT_DOCKER_PIDS_LIMIT
    tool_proxy_bind_host: str = DEFAULT_AGENT_TOOL_PROXY_BIND_HOST
    tool_proxy_public_host: str = DEFAULT_AGENT_TOOL_PROXY_PUBLIC_HOST


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
    calendar_expanded_sync_lookback_days: int = DEFAULT_CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS
    calendar_expanded_sync_lookahead_days: int = DEFAULT_CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS
    calendar_expanded_sync_interval_minutes: int = DEFAULT_CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES
    gmail_attachment_ai_fallback_enabled: bool = True
    gmail_attachment_ai_fallback_base_url: str = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_BASE_URL
    gmail_attachment_ai_fallback_model: str = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_MODEL
    gmail_attachment_ai_fallback_timeout_seconds: int = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_TIMEOUT_SECONDS
    gmail_attachment_ai_fallback_pdf_max_pages: int = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PDF_MAX_PAGES
    gmail_attachment_ai_fallback_pull_model: bool = DEFAULT_GMAIL_ATTACHMENT_AI_FALLBACK_PULL_MODEL
    google_oauth_client_secrets_json_by_account: tuple[tuple[str, str], ...] = ()
    google_oauth_client_secrets_json_by_domain: tuple[tuple[str, str], ...] = ()
    voice_memos: VoiceMemosConfig | None = None
    assemblyai: AssemblyAIConfig | None = None
    agent: AgentConfig | None = None

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
    require_voice_memos: bool = False,
    require_assemblyai: bool = False,
    require_agent: bool = False,
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
    calendar_expanded_sync_lookback_days = int(
        os.getenv("CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS", str(DEFAULT_CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS))
    )
    if calendar_expanded_sync_lookback_days < 0:
        raise ValueError("CALENDAR_EXPANDED_SYNC_LOOKBACK_DAYS must be at least 0")
    calendar_expanded_sync_lookahead_days = int(
        os.getenv("CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS", str(DEFAULT_CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS))
    )
    if calendar_expanded_sync_lookahead_days < 0:
        raise ValueError("CALENDAR_EXPANDED_SYNC_LOOKAHEAD_DAYS must be at least 0")
    calendar_expanded_sync_interval_minutes = int(
        os.getenv(
            "CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES",
            str(DEFAULT_CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES),
        )
    )
    if calendar_expanded_sync_interval_minutes < 1:
        raise ValueError("CALENDAR_EXPANDED_SYNC_INTERVAL_MINUTES must be at least 1")

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

    default_voice_memos_account = account_emails[0] if account_emails else ""
    voice_memos_account = os.getenv("VOICE_MEMOS_ACCOUNT", default_voice_memos_account).strip()
    voice_memos_storage_backend = os.getenv(
        "VOICE_MEMOS_STORAGE_BACKEND",
        DEFAULT_VOICE_MEMOS_STORAGE_BACKEND,
    ).strip()
    voice_memos_recordings_path = os.path.expanduser(
        os.getenv(
            "VOICE_MEMOS_RECORDINGS_PATH",
            "~/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings",
        )
    )
    voice_memos_extensions = tuple(
        extension if extension.startswith(".") else f".{extension}"
        for extension in (
            _parse_csv_env(os.getenv("VOICE_MEMOS_EXTENSIONS"))
            or DEFAULT_VOICE_MEMOS_EXTENSIONS
        )
    )
    voice_memos_google_drive_folder_id = (
        os.getenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID")
        or os.getenv("VOICE_MEMOS_DRIVE_FOLDER_ID")
        or ""
    ).strip()
    voice_memos_transcription_provider = os.getenv(
        "VOICE_MEMOS_TRANSCRIPTION_PROVIDER",
        DEFAULT_VOICE_MEMOS_TRANSCRIPTION_PROVIDER,
    ).strip()
    voice_memos: VoiceMemosConfig | None = None
    if require_voice_memos or os.getenv("VOICE_MEMOS_ACCOUNT") or os.getenv("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID"):
        if not voice_memos_account:
            raise ValueError("VOICE_MEMOS_ACCOUNT or GMAIL_ACCOUNTS must be set for Voice Memos sync")
        if voice_memos_storage_backend not in {"google_drive"}:
            raise ValueError("VOICE_MEMOS_STORAGE_BACKEND currently supports: google_drive")
        if voice_memos_transcription_provider not in {"assemblyai"}:
            raise ValueError("VOICE_MEMOS_TRANSCRIPTION_PROVIDER currently supports: assemblyai")
        if voice_memos_storage_backend == "google_drive" and not voice_memos_google_drive_folder_id:
            raise ValueError("VOICE_MEMOS_GOOGLE_DRIVE_FOLDER_ID must be set for Google Drive Voice Memos storage")
        if not voice_memos_extensions:
            raise ValueError("VOICE_MEMOS_EXTENSIONS must include at least one extension")
        voice_memos = VoiceMemosConfig(
            account=voice_memos_account,
            recordings_path=voice_memos_recordings_path,
            extensions=tuple(extension.lower() for extension in voice_memos_extensions),
            storage_backend=voice_memos_storage_backend,
            google_drive_folder_id=voice_memos_google_drive_folder_id,
            transcription_provider=voice_memos_transcription_provider,
        )

    assemblyai_api_key = os.getenv("ASSEMBLYAI_API_KEY", "").strip()
    assemblyai: AssemblyAIConfig | None = None
    if require_assemblyai or assemblyai_api_key:
        if not assemblyai_api_key:
            raise ValueError("ASSEMBLYAI_API_KEY must be set for Voice Memos transcription")
        assemblyai_poll_interval_seconds = int(
            os.getenv(
                "ASSEMBLYAI_POLL_INTERVAL_SECONDS",
                str(DEFAULT_ASSEMBLYAI_POLL_INTERVAL_SECONDS),
            )
        )
        if assemblyai_poll_interval_seconds < 1:
            raise ValueError("ASSEMBLYAI_POLL_INTERVAL_SECONDS must be at least 1")
        assemblyai_timeout_seconds = int(
            os.getenv("ASSEMBLYAI_TIMEOUT_SECONDS", str(DEFAULT_ASSEMBLYAI_TIMEOUT_SECONDS))
        )
        if assemblyai_timeout_seconds < 1:
            raise ValueError("ASSEMBLYAI_TIMEOUT_SECONDS must be at least 1")
        assemblyai_min_speakers_expected = _parse_optional_int_env(
            "ASSEMBLYAI_MIN_SPEAKERS_EXPECTED",
            DEFAULT_ASSEMBLYAI_MIN_SPEAKERS_EXPECTED,
        )
        assemblyai_max_speakers_expected = _parse_optional_int_env(
            "ASSEMBLYAI_MAX_SPEAKERS_EXPECTED",
            DEFAULT_ASSEMBLYAI_MAX_SPEAKERS_EXPECTED,
        )
        if assemblyai_min_speakers_expected is not None and assemblyai_min_speakers_expected < 1:
            raise ValueError("ASSEMBLYAI_MIN_SPEAKERS_EXPECTED must be at least 1")
        if assemblyai_max_speakers_expected is not None and assemblyai_max_speakers_expected < 1:
            raise ValueError("ASSEMBLYAI_MAX_SPEAKERS_EXPECTED must be at least 1")
        if (
            assemblyai_min_speakers_expected is not None
            and assemblyai_max_speakers_expected is not None
            and assemblyai_min_speakers_expected > assemblyai_max_speakers_expected
        ):
            raise ValueError("ASSEMBLYAI_MIN_SPEAKERS_EXPECTED must be less than or equal to ASSEMBLYAI_MAX_SPEAKERS_EXPECTED")
        assemblyai = AssemblyAIConfig(
            api_key=assemblyai_api_key,
            base_url=(os.getenv("ASSEMBLYAI_BASE_URL") or DEFAULT_ASSEMBLYAI_BASE_URL).rstrip("/"),
            poll_interval_seconds=assemblyai_poll_interval_seconds,
            timeout_seconds=assemblyai_timeout_seconds,
            min_speakers_expected=assemblyai_min_speakers_expected,
            max_speakers_expected=assemblyai_max_speakers_expected,
        )

    agent: AgentConfig | None = None
    if require_agent:
        agent_provider = (os.getenv("AGENT_PROVIDER") or DEFAULT_AGENT_PROVIDER).strip().lower()
        if agent_provider not in {"codex", "claude"}:
            raise ValueError("AGENT_PROVIDER must be codex or claude")
        agent_timeout_seconds = int(os.getenv("AGENT_TIMEOUT_SECONDS", str(DEFAULT_AGENT_TIMEOUT_SECONDS)))
        if agent_timeout_seconds < 1:
            raise ValueError("AGENT_TIMEOUT_SECONDS must be at least 1")
        agent_pids_limit = int(os.getenv("AGENT_DOCKER_PIDS_LIMIT", str(DEFAULT_AGENT_DOCKER_PIDS_LIMIT)))
        if agent_pids_limit < 1:
            raise ValueError("AGENT_DOCKER_PIDS_LIMIT must be at least 1")
        agent = AgentConfig(
            provider=agent_provider,
            model=os.getenv("AGENT_MODEL", "").strip(),
            docker_image=default_agent_docker_image(),
            auth_volume=os.getenv("AGENT_AUTH_VOLUME", DEFAULT_AGENT_AUTH_VOLUME),
            runs_volume=os.getenv("AGENT_RUNS_VOLUME", DEFAULT_AGENT_RUNS_VOLUME),
            runs_dir=os.getenv("AGENT_RUNS_DIR", DEFAULT_AGENT_RUNS_DIR),
            timeout_seconds=agent_timeout_seconds,
            docker_network=os.getenv("AGENT_DOCKER_NETWORK", DEFAULT_AGENT_DOCKER_NETWORK),
            docker_memory=os.getenv("AGENT_DOCKER_MEMORY", DEFAULT_AGENT_DOCKER_MEMORY),
            docker_cpus=os.getenv("AGENT_DOCKER_CPUS", DEFAULT_AGENT_DOCKER_CPUS),
            docker_pids_limit=agent_pids_limit,
            tool_proxy_bind_host=os.getenv("AGENT_TOOL_PROXY_BIND_HOST", DEFAULT_AGENT_TOOL_PROXY_BIND_HOST),
            tool_proxy_public_host=os.getenv(
                "AGENT_TOOL_PROXY_PUBLIC_HOST",
                default_agent_tool_proxy_public_host(os.getenv("AGENT_DOCKER_NETWORK", DEFAULT_AGENT_DOCKER_NETWORK)),
            ),
        )

    google_scopes = [GMAIL_READONLY_SCOPE, CALENDAR_READONLY_SCOPE]
    if voice_memos and voice_memos.storage_backend == "google_drive":
        google_scopes.append(GOOGLE_DRIVE_SCOPE)

    return Settings(
        clickhouse_url=clickhouse_url,
        gmail_accounts=gmail_accounts,
        gmail_oauth_client_secrets_json=client_secrets_json,
        google_scopes=tuple(dict.fromkeys(google_scopes)),
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
        calendar_expanded_sync_lookback_days=calendar_expanded_sync_lookback_days,
        calendar_expanded_sync_lookahead_days=calendar_expanded_sync_lookahead_days,
        calendar_expanded_sync_interval_minutes=calendar_expanded_sync_interval_minutes,
        slack_accounts=tuple(slack_accounts),
        slack_page_size=slack_page_size,
        slack_lookback_days=int(os.getenv("SLACK_LOOKBACK_DAYS", str(DEFAULT_SLACK_LOOKBACK_DAYS))),
        slack_thread_audit_days=int(
            os.getenv("SLACK_THREAD_AUDIT_DAYS", str(DEFAULT_SLACK_THREAD_AUDIT_DAYS))
        ),
        slack_force_full_sync=_parse_bool_env(os.getenv("SLACK_FORCE_FULL_SYNC"), False),
        voice_memos=voice_memos,
        assemblyai=assemblyai,
        agent=agent,
    )
