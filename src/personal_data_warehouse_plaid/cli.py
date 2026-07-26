from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import html
import json
import logging
import secrets
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse
import webbrowser

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.plaid_sync import (
    PlaidAPIError,
    PlaidClient,
    PlaidSyncRunner,
    plaid_error_code,
)
from personal_data_warehouse.schema import PlaidLinkedItem
from personal_data_warehouse.warehouse import warehouse_from_settings

LOGGER = logging.getLogger(__name__)

# Plaid errors that mean "this Item is already gone on Plaid's side": the
# local rows are then the only thing left to clean up, so /item/remove failing
# with one of these must not block the delete.
PLAID_ALREADY_REMOVED_ERROR_CODES = frozenset({"ITEM_NOT_FOUND", "INVALID_ACCESS_TOKEN"})


@dataclass(frozen=True)
class LinkResult:
    public_token: str
    institution_id: str = ""
    institution_name: str = ""


class LocalPlaidLinkServer:
    def __init__(self, *, link_token: str, client_name: str, host: str = "127.0.0.1", port: int = 0) -> None:
        self.link_token = link_token
        self.client_name = client_name
        self.host = host
        self.port = port
        self.state_token = secrets.token_urlsafe(18)
        self.result: LinkResult | None = None
        self.error: str = ""
        self._httpd: ThreadingHTTPServer | None = None

    def __enter__(self) -> "LocalPlaidLinkServer":
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
                return

            def do_GET(self) -> None:  # noqa: N802 - stdlib callback
                parsed = urlparse(self.path)
                if parsed.path == "/":
                    self._write_html(_link_page(outer.link_token, outer.client_name, outer.state_token))
                    return
                if parsed.path == "/done":
                    self._write_html("<html><body><h1>Plaid Link complete</h1><p>You can close this tab.</p></body></html>")
                    return
                self.send_error(404)

            def do_POST(self) -> None:  # noqa: N802 - stdlib callback
                parsed = urlparse(self.path)
                if parsed.path != "/exchange":
                    self.send_error(404)
                    return
                if parse_qs(parsed.query).get("state", [""])[0] != outer.state_token:
                    self.send_error(403)
                    return
                length = int(self.headers.get("Content-Length") or "0")
                data = json.loads(self.rfile.read(length) or b"{}")
                public_token = str(data.get("public_token") or "")
                if not public_token:
                    outer.error = str(data.get("error") or "Plaid Link did not return a public token")
                    self._write_json({"ok": False, "error": outer.error})
                    self._shutdown_server()
                    return
                metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
                institution = metadata.get("institution") if isinstance(metadata.get("institution"), dict) else {}
                outer.result = LinkResult(
                    public_token=public_token,
                    institution_id=str(institution.get("institution_id") or ""),
                    institution_name=str(institution.get("name") or ""),
                )
                self._write_json({"ok": True})
                self._shutdown_server()

            def _shutdown_server(self) -> None:
                if outer._httpd is not None:
                    # Shutdown must run on another thread to avoid deadlocking
                    # the request handler that is currently serving this POST.
                    import threading

                    threading.Thread(target=outer._httpd.shutdown, daemon=True).start()

            def _write_html(self, body: str) -> None:
                data = body.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _write_json(self, payload: dict[str, Any]) -> None:
                data = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self._httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        self.port = int(self._httpd.server_address[1])
        return self

    def __exit__(self, *exc_info) -> None:
        if self._httpd is not None:
            self._httpd.server_close()
            self._httpd = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/"

    def wait_for_result(self) -> LinkResult:
        if self._httpd is None:
            raise RuntimeError("Plaid Link server is not running")
        self._httpd.serve_forever()
        if self.result is not None:
            return self.result
        raise RuntimeError(self.error or "Plaid Link did not complete")


def _link_page(link_token: str, client_name: str, state_token: str) -> str:
    link_token_json = json.dumps(link_token)
    client_name_html = html.escape(client_name)
    state_json = json.dumps(state_token)
    return f"""<!doctype html>
<html>
<head><meta charset=\"utf-8\"><title>{client_name_html} Plaid Link</title></head>
<body>
  <h1>{client_name_html} Plaid Link</h1>
  <p>Click the button below to open Plaid Link. Complete OAuth/MFA in the Plaid flow, then this local page will return a public token to the CLI.</p>
  <button id=\"link\">Open Plaid Link</button>
  <pre id=\"status\"></pre>
  <script src=\"https://cdn.plaid.com/link/v2/stable/link-initialize.js\"></script>
  <script>
    const status = document.getElementById('status');
    const config = {{
      token: {link_token_json},
      onSuccess: async (public_token, metadata) => {{
        status.textContent = 'Plaid Link completed; returning token to local CLI...';
        const response = await fetch('/exchange?state=' + encodeURIComponent({state_json}), {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{public_token, metadata}}),
        }});
        const payload = await response.json();
        if (!payload.ok) {{ throw new Error(payload.error || 'exchange failed'); }}
        window.location = '/done';
      }},
      onExit: async (err, metadata) => {{
        const message = err
          ? (err.error_message || err.error_code || 'Plaid Link exited with an error')
          : 'Plaid Link exited before an account was linked';
        status.textContent = message;
        await fetch('/exchange?state=' + encodeURIComponent({state_json}), {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{error: message}}),
        }});
      }},
    }};
    // Plaid OAuth redirects back with oauth_state_id. Re-initializing Link with
    // the same token and the exact received URI resumes the institution flow.
    if (new URLSearchParams(window.location.search).has('oauth_state_id')) {{
      config.receivedRedirectUri = window.location.href;
    }}
    const handler = Plaid.create(config);
    document.getElementById('link').onclick = () => handler.open();
    handler.open();
  </script>
</body>
</html>"""


def run_link(args: argparse.Namespace) -> int:
    settings = load_settings(require_gmail=False, require_plaid=True)
    if settings.plaid is None:
        raise ValueError("Plaid is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        warehouse.ensure_plaid_tables()
        client = PlaidClient(settings.plaid)
        link_token_response = client.create_link_token(account=settings.plaid.account)
        link_token = str(link_token_response.get("link_token") or "")
        if not link_token:
            raise RuntimeError("Plaid did not return a link_token")
        with LocalPlaidLinkServer(
            link_token=link_token,
            client_name=settings.plaid.client_name,
            host=args.host,
            port=args.port,
        ) as server:
            print("Open this URL to authorize Plaid accounts:")
            print(server.url)
            if not args.no_browser:
                webbrowser.open(server.url)
            result = server.wait_for_result()
        exchange_response = client.exchange_public_token(result.public_token)
        access_token = str(exchange_response.get("access_token") or "")
        item_id = str(exchange_response.get("item_id") or "")
        if not access_token or not item_id:
            raise RuntimeError("Plaid public token exchange did not return access_token and item_id")
        warehouse.upsert_plaid_item_token(
            account=settings.plaid.account,
            item_id=item_id,
            access_token=access_token,
            institution_id=result.institution_id,
            institution_name=result.institution_name,
            linked_at=datetime.now(tz=UTC),
        )
        print("Plaid institution linked successfully.")
        return 0
    finally:
        warehouse.close()


def run_sync(_args: argparse.Namespace) -> int:
    settings = load_settings(require_gmail=False, require_plaid=True)
    if settings.plaid is None:
        raise ValueError("Plaid is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        summary = PlaidSyncRunner(config=settings.plaid, warehouse=warehouse, logger=LOGGER).sync_all()
    finally:
        warehouse.close()
    print(
        "Plaid sync complete: "
        f"items={summary.items} accounts={summary.accounts} transactions={summary.transactions} "
        f"removed_transactions={summary.removed_transactions} investment_holdings={summary.investment_holdings} "
        f"investment_transactions={summary.investment_transactions} liabilities={summary.liabilities}"
    )
    return 0


def resolve_plaid_item(items: list[PlaidLinkedItem], needle: str) -> PlaidLinkedItem:
    """Find one linked Item by exact id or unambiguous id prefix.

    Plaid item ids are long opaque strings that are usually read off a table
    that truncated them, so a prefix is what an operator actually has in hand.
    Ambiguity is an error, never a guess — this selects rows to delete.
    """
    needle = needle.strip()
    if not needle:
        raise ValueError("an item id is required")
    exact = [item for item in items if item.item_id == needle]
    if exact:
        return exact[0]
    matches = [item for item in items if item.item_id.startswith(needle)]
    if not matches:
        raise ValueError(f"no linked Plaid item matches {needle!r}")
    if len(matches) > 1:
        ids = ", ".join(sorted(item.item_id for item in matches))
        raise ValueError(f"{needle!r} matches {len(matches)} linked Plaid items: {ids}")
    return matches[0]


def _describe_item(item: PlaidLinkedItem) -> str:
    return f"{item.item_id} ({item.institution_name or item.institution_id or 'unknown institution'})"


def unlink_plaid_item(
    *,
    warehouse,
    client,
    item: PlaidLinkedItem,
    confirm,
    out,
    dry_run: bool = False,
    skip_remote: bool = False,
) -> int:
    """Retire one linked Plaid Item: revoke it at Plaid, then delete its rows.

    Re-linking an institution does not always repair the existing Item —
    Plaid can mint a brand new item_id with brand new account ids for the same
    real accounts. Both Items then keep syncing: net worth counts every
    balance twice and the transaction overlap is duplicated. There is no way
    back from Link, so retiring the dead Item is its own operation.

    Plaid is revoked first: if that fails for any reason other than the Item
    already being gone, nothing is deleted, so a retry is safe.
    """

    accounts = warehouse.load_plaid_item_accounts(account=item.account, item_id=item.item_id)
    counts = warehouse.count_plaid_item_rows(account=item.account, item_id=item.item_id)
    print(f"Plaid item {_describe_item(item)}", file=out)
    for account in accounts:
        removed = " [removed]" if int(account.get("is_removed") or 0) else ""
        print(
            f"  account {account['mask'] or '----'} {account['name']} "
            f"({account['type']}/{account['subtype']}) balance {account['current_balance']}{removed}",
            file=out,
        )
    print(
        "  rows to delete: " + " ".join(f"{table}={count}" for table, count in sorted(counts.items())),
        file=out,
    )
    if dry_run:
        print("Dry run: nothing was revoked or deleted.", file=out)
        return 0
    if not confirm(f"Revoke {_describe_item(item)} at Plaid and delete its warehouse rows?"):
        print("Aborted; nothing was revoked or deleted.", file=out)
        return 1

    if skip_remote:
        print("Skipping Plaid /item/remove (--skip-remote).", file=out)
    else:
        try:
            client.item_remove(item.access_token)
        except PlaidAPIError as exc:
            message = _redact(str(exc), item.access_token)
            if plaid_error_code(message) not in PLAID_ALREADY_REMOVED_ERROR_CODES:
                print(f"Plaid refused to remove the item: {message}", file=out)
                print("Nothing was deleted; fix the error and re-run.", file=out)
                return 1
            print(f"Plaid has already forgotten this item ({message}); deleting local rows.", file=out)
        else:
            print("Revoked at Plaid.", file=out)

    deleted = warehouse.delete_plaid_item(account=item.account, item_id=item.item_id)
    print(
        "Deleted: " + " ".join(f"{table}={count}" for table, count in sorted(deleted.items())),
        file=out,
    )
    print(
        "The finance ledger reconciles on its next run: a re-linked account merges back into the "
        "logical account it duplicated, and the duplicated transactions disappear.",
        file=out,
    )
    return 0


def _redact(message: str, *credentials: str) -> str:
    for credential in credentials:
        if credential:
            message = message.replace(credential, "[redacted]")
    return message


def _confirm_on_stdin(prompt: str) -> bool:
    try:
        answer = input(f"{prompt} [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in {"y", "yes"}


def run_items(_args: argparse.Namespace) -> int:
    settings = load_settings(require_gmail=False, require_plaid=True)
    if settings.plaid is None:
        raise ValueError("Plaid is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        warehouse.ensure_plaid_tables()
        items = warehouse.load_plaid_item_tokens()
        if not items:
            print("No linked Plaid items. Run `pdw ingest plaid link` to add one.")
            return 0
        for item in items:
            counts = warehouse.count_plaid_item_rows(account=item.account, item_id=item.item_id)
            print(
                f"{item.item_id}  {item.institution_name or item.institution_id or 'unknown institution'}  "
                f"accounts={counts['plaid_accounts']} transactions={counts['plaid_transactions']}"
            )
    finally:
        warehouse.close()
    return 0


def run_unlink(args: argparse.Namespace) -> int:
    settings = load_settings(require_gmail=False, require_plaid=True)
    if settings.plaid is None:
        raise ValueError("Plaid is not configured")
    warehouse = warehouse_from_settings(settings)
    try:
        warehouse.ensure_plaid_tables()
        try:
            item = resolve_plaid_item(warehouse.load_plaid_item_tokens(), args.item_id)
        except ValueError as exc:
            print(f"pdw ingest plaid unlink: {exc}", file=sys.stderr)
            print("Run `pdw ingest plaid items` to list linked items.", file=sys.stderr)
            return 2
        return unlink_plaid_item(
            warehouse=warehouse,
            client=PlaidClient(settings.plaid),
            item=item,
            confirm=(lambda _prompt: True) if args.yes else _confirm_on_stdin,
            out=sys.stdout,
            dry_run=args.dry_run,
            skip_remote=args.skip_remote,
        )
    finally:
        warehouse.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Link Plaid items and sync Plaid-backed personal finance data.")
    subparsers = parser.add_subparsers(dest="command")
    link = subparsers.add_parser("link", help="create a Plaid Link token, open local Link UI, and persist the exchanged access token")
    link.add_argument("--host", default="127.0.0.1", help="local host for the Plaid Link callback server")
    link.add_argument("--port", type=int, default=0, help="local port for the Plaid Link callback server (0 picks an open port)")
    link.add_argument("--no-browser", action="store_true", help="print the local Link URL without opening a browser")
    sync = subparsers.add_parser("sync", help="sync all linked Plaid items")
    items = subparsers.add_parser("items", help="list linked Plaid items with their row counts")
    unlink = subparsers.add_parser(
        "unlink",
        help="retire a linked Plaid item: revoke it at Plaid and delete its warehouse rows",
    )
    unlink.add_argument("item_id", help="item id, or an unambiguous prefix of one (see `plaid items`)")
    unlink.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    unlink.add_argument("--dry-run", action="store_true", help="print what would be revoked and deleted")
    unlink.add_argument(
        "--skip-remote",
        action="store_true",
        help="do not call Plaid /item/remove (for an item already revoked in the Plaid dashboard)",
    )
    sync.set_defaults(func=run_sync)
    link.set_defaults(func=run_link)
    items.set_defaults(func=run_items)
    unlink.set_defaults(func=run_unlink)
    parser.set_defaults(func=run_sync)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
