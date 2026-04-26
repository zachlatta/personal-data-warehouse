from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

from personal_data_warehouse.config import email_domain, env_slug, load_settings


def run_oauth_flow(email_address: str) -> dict[str, str]:
    settings = load_settings(
        require_clickhouse=False,
        require_gmail=False,
        require_gmail_client_secrets=True,
    )
    client_secrets_json = settings.google_oauth_client_secrets_json_for_email(email_address)
    if not client_secrets_json:
        slug = env_slug(email_address)
        domain_slug = env_slug(email_domain(email_address))
        raise RuntimeError(
            f"No Google OAuth client secrets configured for {email_address}. "
            f"Set GOOGLE_DOMAIN_{domain_slug}_OAUTH_CLIENT_SECRETS_JSON_B64 or "
            f"GMAIL_DOMAIN_{domain_slug}_OAUTH_CLIENT_SECRETS_JSON_B64. "
            f"Account override names GOOGLE_{slug}_OAUTH_CLIENT_SECRETS_JSON_B64 and "
            f"GMAIL_{slug}_OAUTH_CLIENT_SECRETS_JSON_B64 are also supported."
        )
    flow = InstalledAppFlow.from_client_config(
        json.loads(client_secrets_json),
        settings.google_scopes,
    )
    credentials = flow.run_local_server(port=0)
    encoded_token = base64.b64encode(credentials.to_json().encode("utf-8")).decode("ascii")
    google_env_name = f"GOOGLE_{env_slug(email_address)}_TOKEN_JSON_B64"
    env_name = f"GMAIL_{env_slug(email_address)}_TOKEN_JSON_B64"
    return {
        google_env_name: encoded_token,
        env_name: encoded_token,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Authorize one or more Google accounts for the personal data warehouse Gmail and Calendar syncs.",
    )
    parser.add_argument("--email", help="Google account email address from GMAIL_ACCOUNTS or CALENDAR_ACCOUNTS")
    parser.add_argument("--all", action="store_true", help="Authorize every account in GMAIL_ACCOUNTS and CALENDAR_ACCOUNTS")
    parser.add_argument(
        "--write-env",
        action="store_true",
        help="Update the local .env file with generated token values instead of only printing them",
    )
    args = parser.parse_args()

    settings = load_settings(
        require_clickhouse=False,
        require_gmail=False,
        require_gmail_client_secrets=True,
    )
    configured_emails = tuple(
        dict.fromkeys(
            [
                *(account.email_address for account in settings.gmail_accounts),
                *(account.email_address for account in settings.calendar_accounts),
            ]
        )
    )

    if args.all:
        emails = configured_emails
    elif args.email:
        normalized = args.email.strip().lower()
        if normalized not in {email.lower() for email in configured_emails}:
            configured = ", ".join(configured_emails)
            raise ValueError(f"{args.email} is not configured in GMAIL_ACCOUNTS or CALENDAR_ACCOUNTS ({configured})")
        emails = (args.email,)
    else:
        parser.error("pass either --email or --all")
        return

    if not emails:
        parser.error("no accounts configured in GMAIL_ACCOUNTS or CALENDAR_ACCOUNTS")
        return

    for email_address in emails:
        env_values = run_oauth_flow(email_address)
        if args.write_env:
            update_env_file(Path(".env"), env_values)
            print(f"Updated .env token env vars for {email_address}")
        else:
            for name, value in env_values.items():
                print(f"{name}={value}")
        print(f"Generated OAuth token env var for {email_address}")


def update_env_file(path: Path, values: dict[str, str]) -> None:
    existing_lines = path.read_text().splitlines() if path.exists() else []
    remaining = dict(values)
    updated_lines: list[str] = []

    for line in existing_lines:
        key = line.split("=", 1)[0] if "=" in line else ""
        if key in remaining:
            updated_lines.append(f"{key}={remaining.pop(key)}")
        else:
            updated_lines.append(line)

    if remaining and updated_lines and updated_lines[-1].strip():
        updated_lines.append("")
    for key, value in remaining.items():
        updated_lines.append(f"{key}={value}")

    path.write_text("\n".join(updated_lines) + "\n")


if __name__ == "__main__":
    main()
