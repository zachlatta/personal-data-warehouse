from __future__ import annotations

import argparse
import base64
import json

from google_auth_oauthlib.flow import InstalledAppFlow

from personal_data_warehouse.config import GmailAccount, env_slug, load_settings


def run_oauth_flow(account: GmailAccount) -> None:
    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=True)
    if not settings.gmail_oauth_client_secrets_json:
        raise RuntimeError("GMAIL_OAUTH_CLIENT_SECRETS_JSON or GMAIL_OAUTH_CLIENT_SECRETS_JSON_B64 must be set")
    flow = InstalledAppFlow.from_client_config(
        json.loads(settings.gmail_oauth_client_secrets_json),
        settings.gmail_scopes,
    )
    credentials = flow.run_local_server(port=0)
    encoded_token = base64.b64encode(credentials.to_json().encode("utf-8")).decode("ascii")
    env_name = f"GMAIL_{env_slug(account.email_address)}_TOKEN_JSON_B64"
    print(f"{env_name}={encoded_token}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Authorize one or more Gmail mailboxes for the personal data warehouse sync.",
    )
    parser.add_argument("--email", help="Mailbox email address from GMAIL_ACCOUNTS")
    parser.add_argument("--all", action="store_true", help="Authorize every mailbox in GMAIL_ACCOUNTS")
    args = parser.parse_args()

    settings = load_settings(require_clickhouse=False, require_gmail_client_secrets=True)

    if args.all:
        accounts = settings.gmail_accounts
    elif args.email:
        accounts = (settings.account_for_email(args.email),)
    else:
        parser.error("pass either --email or --all")
        return

    for account in accounts:
        run_oauth_flow(account)
        print(f"Generated OAuth token env var for {account.email_address}")


if __name__ == "__main__":
    main()
