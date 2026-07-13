from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import html
import json
import logging
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse
import webbrowser

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.plaid_sync import PlaidClient, PlaidSyncRunner
from personal_data_warehouse.warehouse import warehouse_from_settings

LOGGER = logging.getLogger(__name__)


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Link Plaid items and sync Plaid-backed personal finance data.")
    subparsers = parser.add_subparsers(dest="command")
    link = subparsers.add_parser("link", help="create a Plaid Link token, open local Link UI, and persist the exchanged access token")
    link.add_argument("--host", default="127.0.0.1", help="local host for the Plaid Link callback server")
    link.add_argument("--port", type=int, default=0, help="local port for the Plaid Link callback server (0 picks an open port)")
    link.add_argument("--no-browser", action="store_true", help="print the local Link URL without opening a browser")
    sync = subparsers.add_parser("sync", help="sync all linked Plaid items")
    sync.set_defaults(func=run_sync)
    link.set_defaults(func=run_link)
    parser.set_defaults(func=run_sync)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
