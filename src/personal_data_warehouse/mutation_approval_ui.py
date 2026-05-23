from __future__ import annotations

import base64
from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import hmac
from html import escape
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import secrets
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from personal_data_warehouse.config import load_settings
from personal_data_warehouse.warehouse import warehouse_from_settings

SESSION_COOKIE_NAME = "pdw_mutation_ui_session"
DEFAULT_SESSION_TTL_SECONDS = 12 * 60 * 60


@dataclass(frozen=True)
class MutationApprovalUiConfig:
    bind_host: str
    port: int
    public_base_url: str | None
    password: str
    session_secret: str
    session_ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS


class RunningMutationApprovalUi:
    def __init__(self, *, server: ThreadingHTTPServer, thread: threading.Thread, public_base_url: str) -> None:
        self._server = server
        self._thread = thread
        self.public_base_url = public_base_url.rstrip("/")

    def mutation_url(self, mutation_id: str) -> str:
        return f"{self.public_base_url}/mutations/{mutation_id}"

    def request_url(self, request_id: str) -> str:
        return f"{self.public_base_url}/requests/{request_id}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def mutation_approval_ui_config_from_env() -> MutationApprovalUiConfig:
    password = os.getenv("PDW_MUTATION_UI_PASSWORD")
    if not password:
        raise ValueError("PDW_MUTATION_UI_PASSWORD must be set")
    port = int(os.getenv("PDW_MUTATION_UI_PORT", "0"))
    if port < 0 or port > 65535:
        raise ValueError("PDW_MUTATION_UI_PORT must be between 0 and 65535")
    ttl_seconds = int(os.getenv("PDW_MUTATION_UI_SESSION_TTL_SECONDS", str(DEFAULT_SESSION_TTL_SECONDS)))
    if ttl_seconds <= 0:
        raise ValueError("PDW_MUTATION_UI_SESSION_TTL_SECONDS must be positive")
    return MutationApprovalUiConfig(
        bind_host=os.getenv("PDW_MUTATION_UI_BIND_HOST", "127.0.0.1"),
        port=port,
        public_base_url=os.getenv("PDW_MUTATION_UI_PUBLIC_BASE_URL") or None,
        password=password,
        session_secret=os.getenv("PDW_MUTATION_UI_SESSION_SECRET") or secrets.token_urlsafe(48),
        session_ttl_seconds=ttl_seconds,
    )


def start_mutation_approval_ui(config: MutationApprovalUiConfig) -> RunningMutationApprovalUi:
    handler = mutation_approval_handler(config)
    server = ThreadingHTTPServer((config.bind_host, config.port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    public_base_url = config.public_base_url or f"http://{config.bind_host}:{server.server_port}"
    return RunningMutationApprovalUi(server=server, thread=thread, public_base_url=public_base_url)


def mutation_approval_handler(config: MutationApprovalUiConfig) -> type[BaseHTTPRequestHandler]:
    def with_warehouse(callback: Callable[[Any], Any]) -> Any:
        settings = load_settings(require_gmail=False)
        warehouse = warehouse_from_settings(settings)
        try:
            warehouse.ensure_upstream_mutation_tables()
            return callback(warehouse)
        finally:
            warehouse.close()

    class MutationApprovalHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/login":
                session = self._current_session()
                next_path = safe_next_path(form_value(parse_qs(urlparse(self.path).query), "next") or "/requests")
                if session is not None:
                    self._redirect(next_path)
                    return
                self._write_html(login_page(next_path))
                return
            if path == "/logout":
                self._redirect("/login", clear_session=True)
                return

            session = self._require_session()
            if session is None:
                return
            csrf_token = str(session.get("csrf_token") or "")
            if path in {"", "/", "/mutations", "/requests"}:
                self._write_html(with_warehouse(render_request_list))
                return
            request_id = request_id_from_path(path)
            if request_id:
                self._write_html(with_warehouse(lambda warehouse: render_request_detail(warehouse, request_id, csrf_token)))
                return
            mutation_id = mutation_id_from_path(path)
            if mutation_id:
                self._write_html(with_warehouse(lambda warehouse: render_mutation_detail(warehouse, mutation_id, csrf_token)))
                return
            self._write_html(page("Not found", "<p>Not found.</p>"), status=404)

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            if path == "/login":
                self._handle_login()
                return

            session = self._require_session()
            if session is None:
                return
            request_id = request_id_from_action_path(path)
            mutation_id = mutation_id_from_action_path(path)
            if not request_id and not mutation_id:
                self._write_html(page("Not found", "<p>Not found.</p>"), status=404)
                return
            form = self._read_form()
            if not csrf_token_is_valid(form_value(form, "csrf_token"), session):
                self._write_html(page("Invalid session", "<p>Your session could not be verified. Please sign in again.</p>"), status=403)
                return
            try:
                if request_id and path.endswith("/approve"):
                    with_warehouse(lambda warehouse: warehouse.approve_upstream_mutation_request(request_id, actor_id="approval_ui"))
                elif request_id and path.endswith("/reject"):
                    with_warehouse(
                        lambda warehouse: warehouse.reject_upstream_mutation_request(
                            request_id,
                            actor_id="approval_ui",
                            reason=form_value(form, "reason"),
                        )
                    )
                elif request_id and path.endswith("/remove-mutation"):
                    with_warehouse(
                        lambda warehouse: warehouse.remove_upstream_mutation_from_request(
                            request_id=request_id,
                            mutation_id=form_value(form, "mutation_id"),
                            actor_id="approval_ui",
                        )
                    )
                elif mutation_id and path.endswith("/approve"):
                    with_warehouse(lambda warehouse: warehouse.approve_upstream_mutation(mutation_id, actor_id="approval_ui"))
                elif mutation_id and path.endswith("/reject"):
                    with_warehouse(
                        lambda warehouse: warehouse.reject_upstream_mutation(
                            mutation_id,
                            actor_id="approval_ui",
                            reason=form_value(form, "reason"),
                        )
                    )
                elif mutation_id and path.endswith("/remove-thread"):
                    with_warehouse(
                        lambda warehouse: warehouse.remove_threads_from_gmail_archive_mutation(
                            mutation_id=mutation_id,
                            thread_ids=[form_value(form, "thread_id")],
                            actor_id="approval_ui",
                        )
                    )
                elif mutation_id and path.endswith("/remove-operation"):
                    with_warehouse(
                        lambda warehouse: warehouse.remove_operations_from_contact_mutation(
                            mutation_id=mutation_id,
                            operation_indexes=[int(form_value(form, "op_index"))],
                            actor_id="approval_ui",
                        )
                    )
                elif mutation_id and path.endswith("/update-email"):
                    with_warehouse(
                        lambda warehouse: warehouse.update_gmail_email_mutation(
                            mutation_id=mutation_id,
                            message=email_message_from_form(form),
                            delivery_mode=form_value(form, "delivery_mode"),
                            actor_id="approval_ui",
                        )
                    )
                elif mutation_id and path.endswith("/approve-email"):
                    def update_and_approve_email(warehouse):
                        warehouse.update_gmail_email_mutation(
                            mutation_id=mutation_id,
                            message=email_message_from_form(form),
                            delivery_mode=email_delivery_mode_from_form(form),
                            actor_id="approval_ui",
                        )
                        return warehouse.approve_upstream_mutation(mutation_id, actor_id="approval_ui")

                    with_warehouse(update_and_approve_email)
                else:
                    self._write_html(page("Not found", "<p>Not found.</p>"), status=404)
                    return
            except Exception as exc:
                self._write_html(page("Mutation error", f"<p>{escape(str(exc))}</p>"), status=400)
                return
            self.send_response(303)
            self.send_header("location", f"/requests/{request_id}" if request_id else f"/mutations/{mutation_id}")
            self.end_headers()

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def _handle_login(self) -> None:
            form = self._read_form()
            next_path = safe_next_path(form_value(form, "next") or "/requests")
            if not secrets.compare_digest(form_value(form, "password"), config.password):
                self._write_html(login_page(next_path, error="Incorrect password."), status=403)
                return
            token, _session = create_session_token(config)
            self._redirect(next_path, session_token=token)

        def _current_session(self) -> dict[str, Any] | None:
            token = session_token_from_cookie(self.headers.get("cookie", ""))
            if not token:
                return None
            return verify_session_token(token, config)

        def _require_session(self) -> dict[str, Any] | None:
            session = self._current_session()
            if session is not None:
                return session
            if self.command == "GET":
                self._redirect("/login?" + urlencode({"next": safe_next_path(self.path)}))
            else:
                self._write_html(page("Authentication required", "<p>Please sign in before reviewing mutation requests.</p>"), status=403)
            return None

        def _redirect(
            self,
            location: str,
            *,
            status: int = 303,
            session_token: str | None = None,
            clear_session: bool = False,
        ) -> None:
            self.send_response(status)
            self.send_header("location", location)
            if session_token is not None:
                self.send_header("set-cookie", session_cookie_header(session_token, config))
            if clear_session:
                self.send_header("set-cookie", clear_session_cookie_header(config))
            self.end_headers()

        def _read_form(self) -> dict[str, list[str]]:
            body = self.rfile.read(int(self.headers.get("content-length", "0") or "0"))
            return parse_qs(body.decode("utf-8"), keep_blank_values=True)

        def _write_html(self, html: str, *, status: int = 200) -> None:
            encoded = html.encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "text/html; charset=utf-8")
            self.send_header("content-length", str(len(encoded)))
            self.send_header("cache-control", "no-store")
            self.end_headers()
            self.wfile.write(encoded)

    return MutationApprovalHandler


def create_session_token(config: MutationApprovalUiConfig) -> tuple[str, dict[str, Any]]:
    now = int(time.time())
    session = {
        "iat": now,
        "exp": now + config.session_ttl_seconds,
        "csrf_token": secrets.token_urlsafe(32),
        "nonce": secrets.token_urlsafe(16),
    }
    return sign_session_payload(session, config.session_secret), session


def sign_session_payload(payload: dict[str, Any], secret: str) -> str:
    body_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    body = base64_url_encode(body_bytes)
    signature = hmac.new(secret.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return f"{body}.{base64_url_encode(signature)}"


def verify_session_token(token: str, config: MutationApprovalUiConfig) -> dict[str, Any] | None:
    parts = token.split(".")
    if len(parts) != 2:
        return None
    try:
        signed_body = parts[0].encode("ascii")
    except UnicodeEncodeError:
        return None
    expected_signature = hmac.new(config.session_secret.encode("utf-8"), signed_body, hashlib.sha256).digest()
    try:
        actual_signature = base64_url_decode(parts[1])
        payload_data = base64_url_decode(parts[0])
    except ValueError:
        return None
    if not hmac.compare_digest(actual_signature, expected_signature):
        return None
    try:
        payload = json.loads(payload_data.decode("utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        expires_at = int(payload.get("exp") or 0)
    except (TypeError, ValueError):
        return None
    if expires_at < int(time.time()):
        return None
    if not isinstance(payload.get("csrf_token"), str) or not payload["csrf_token"]:
        return None
    return payload


def csrf_token_is_valid(token: str, session: dict[str, Any]) -> bool:
    expected = session.get("csrf_token")
    return isinstance(expected, str) and secrets.compare_digest(token, expected)


def session_token_from_cookie(cookie_header: str) -> str:
    cookie = SimpleCookie()
    try:
        cookie.load(cookie_header)
    except Exception:
        return ""
    morsel = cookie.get(SESSION_COOKIE_NAME)
    return morsel.value if morsel is not None else ""


def session_cookie_header(token: str, config: MutationApprovalUiConfig) -> str:
    parts = [
        f"{SESSION_COOKIE_NAME}={token}",
        "Path=/",
        "HttpOnly",
        "SameSite=Lax",
        f"Max-Age={config.session_ttl_seconds}",
    ]
    if secure_cookies(config):
        parts.append("Secure")
    return "; ".join(parts)


def clear_session_cookie_header(config: MutationApprovalUiConfig) -> str:
    parts = [
        f"{SESSION_COOKIE_NAME}=",
        "Path=/",
        "HttpOnly",
        "SameSite=Lax",
        "Max-Age=0",
    ]
    if secure_cookies(config):
        parts.append("Secure")
    return "; ".join(parts)


def secure_cookies(config: MutationApprovalUiConfig) -> bool:
    return bool(config.public_base_url and config.public_base_url.lower().startswith("https://"))


def safe_next_path(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme or parsed.netloc or not value.startswith("/"):
        return "/requests"
    return value


def base64_url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def base64_url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(value + padding)
    except Exception as exc:
        raise ValueError("invalid base64") from exc


def render_request_list(warehouse) -> str:
    requests = warehouse.list_upstream_mutation_requests(limit=100)
    rows = []
    for request in requests:
        rows.append(
            "<tr>"
            f"<td><a href=\"/requests/{escape(str(request['id']))}\">{escape(str(request['title']))}</a></td>"
            f"<td>{escape(str(request['status']))}</td>"
            f"<td>{escape(str(request.get('mutation_count', '')))}</td>"
            f"<td>{escape(str(request['created_at']))}</td>"
            "</tr>"
        )
    table = (
        "<table><thead><tr><th>Request</th><th>Status</th><th>Mutations</th><th>Created</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )
    return page("Mutation Requests", table)


def render_request_detail(warehouse, request_id: str, csrf_token: str) -> str:
    request = warehouse.get_upstream_mutation_request(request_id)
    if request is None:
        return page("Request not found", f"<p>No mutation request exists for <code>{escape(request_id)}</code>.</p>")
    mutations = request.get("mutations") or warehouse.list_upstream_mutations_for_request(request_id)
    events = warehouse.list_upstream_mutation_request_events(request_id)
    forms = ""
    if request["status"] == "pending_review":
        forms = (
            f"<form method=\"post\" action=\"/requests/{escape(request_id)}/approve\">"
            f"{csrf_input(csrf_token)}"
            "<button type=\"submit\">Approve all remaining</button></form>"
            f"<form method=\"post\" action=\"/requests/{escape(request_id)}/reject\">"
            f"{csrf_input(csrf_token)}"
            "<input name=\"reason\" type=\"text\" placeholder=\"Reason\">"
            "<button type=\"submit\">Reject request</button></form>"
        )
    body = f"""
        <p><a href="/mutations">All requests</a></p>
        <dl>
          <dt>ID</dt><dd><code>{escape(str(request['id']))}</code></dd>
          <dt>Status</dt><dd>{escape(str(request['status']))}</dd>
          <dt>Reason</dt><dd>{escape(str(request['reason']))}</dd>
          <dt>Revision</dt><dd>{escape(str(request['revision']))}</dd>
        </dl>
        {render_request_overview(mutations)}
        {render_request_mutation_groups(request, mutations, csrf_token)}
        <h2>Review</h2>
        <div class="actions">{forms or '<p>No pending review actions.</p>'}</div>
        <h2>Audit</h2>
        <pre>{escape(json.dumps(events, indent=2, sort_keys=True, default=str))}</pre>
    """
    return page(str(request["title"]), body)


def render_request_overview(mutations: list[dict[str, Any]]) -> str:
    valid_mutations = [mutation for mutation in mutations if isinstance(mutation, dict)]
    status_counts = counts_by(valid_mutations, lambda mutation: str(mutation.get("status") or ""))
    operation_counts = counts_by(valid_mutations, mutation_group_label)
    status_text = ", ".join(format_count(count, status) for status, count in sorted(status_counts.items())) or "0 mutations"
    operation_text = ", ".join(format_count(count, label) for label, count in sorted(operation_counts.items())) or "0 mutations"
    return (
        "<h2>Overview</h2>"
        "<div class=\"summary-strip\">"
        f"<div><strong>{len(valid_mutations)}</strong><span>mutations</span></div>"
        f"<div><strong>{escape(status_text)}</strong><span>status</span></div>"
        f"<div><strong>{escape(operation_text)}</strong><span>types</span></div>"
        "</div>"
    )


def render_request_mutation_groups(request: dict[str, Any], mutations: list[dict[str, Any]], csrf_token: str) -> str:
    valid_mutations = [mutation for mutation in mutations if isinstance(mutation, dict)]
    sections = [
        render_gmail_thread_group(
            request=request,
            mutations=[mutation for mutation in valid_mutations if mutation.get("operation") == "gmail.archive_threads"],
            title="Archive Gmail Threads",
            csrf_token=csrf_token,
        ),
        render_gmail_thread_group(
            request=request,
            mutations=[mutation for mutation in valid_mutations if mutation.get("operation") == "gmail.unarchive_threads"],
            title="Unarchive Gmail Threads",
            csrf_token=csrf_token,
        ),
        render_email_group(
            request=request,
            mutations=[mutation for mutation in valid_mutations if mutation.get("operation") == "gmail.send_email"],
            csrf_token=csrf_token,
        ),
        render_contact_group(
            request=request,
            mutations=[mutation for mutation in valid_mutations if mutation.get("operation") == "contacts.batch_mutation"],
            csrf_token=csrf_token,
        ),
    ]
    known_operations = {"gmail.archive_threads", "gmail.unarchive_threads", "gmail.send_email", "contacts.batch_mutation"}
    unknown = [mutation for mutation in valid_mutations if mutation.get("operation") not in known_operations]
    if unknown:
        sections.append(render_unknown_group(request=request, mutations=unknown, csrf_token=csrf_token))
    return "<h2>Changes</h2>" + "".join(section for section in sections if section)


def render_gmail_thread_group(*, request: dict[str, Any], mutations: list[dict[str, Any]], title: str, csrf_token: str) -> str:
    if not mutations:
        return ""
    status_counts = counts_by(mutations, lambda mutation: str(mutation.get("status") or ""))
    summary = ", ".join(format_count(count, short_status(status)) for status, count in sorted(status_counts.items()))
    rows = []
    for mutation in mutations:
        preview = json_object(mutation.get("preview_json"))
        threads = [thread for thread in preview.get("threads", []) if isinstance(thread, dict)] or [{}]
        for thread in threads:
            rows.append(
                "<tr>"
                f"<td>{status_badge(str(mutation.get('status') or ''))}</td>"
                f"<td>{escape(str(mutation.get('account') or ''))}</td>"
                f"<td><code>{escape(str(thread.get('thread_id') or ''))}</code></td>"
                f"<td>{escape(str(thread.get('subject') or mutation.get('title') or ''))}</td>"
                f"<td>{escape(str(thread.get('latest_from_address') or ''))}</td>"
                f"<td>{escape(str(thread.get('message_count') or thread.get('inbox_message_count') or ''))}</td>"
                f"<td>{escape(str(mutation.get('reason') or ''))}</td>"
                f"<td>{render_request_row_actions(request, mutation, csrf_token)}</td>"
                "</tr>"
            )
    return (
        "<section class=\"mutation-group\">"
        f"<h3>{escape(title)}</h3>"
        f"<p class=\"group-summary\">{escape(summary)}</p>"
        "<table><thead><tr><th>Status</th><th>Account</th><th>Thread</th><th>Subject</th><th>From</th><th>Messages</th><th>Reason</th><th></th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
        "</section>"
    )


def render_email_group(*, request: dict[str, Any], mutations: list[dict[str, Any]], csrf_token: str) -> str:
    if not mutations:
        return ""
    mode_counts = counts_by(mutations, email_delivery_mode)
    summary = ", ".join(format_count(count, mode) for mode, count in sorted(mode_counts.items()))
    rows = []
    detail_sections = []
    for mutation in mutations:
        payload = json_object(mutation.get("payload_json"))
        preview = json_object(mutation.get("preview_json"))
        email = json_object(preview.get("email")) or json_object(payload.get("message"))
        rows.append(
            "<tr>"
            f"<td>{status_badge(str(mutation.get('status') or ''))}</td>"
            f"<td>{escape(str(mutation.get('account') or ''))}</td>"
            f"<td>{escape(email_delivery_mode(mutation))}</td>"
            f"<td>{escape(str(email.get('mode') or 'new_thread'))}</td>"
            f"<td>{escape(', '.join(str(item) for item in email.get('to', [])))}</td>"
            f"<td>{escape(str(email.get('subject') or ''))}</td>"
            f"<td>{render_request_row_actions(request, mutation, csrf_token)}</td>"
            "</tr>"
        )
        detail_sections.append(render_email_preview(mutation, allow_edits=True, heading_level=4, csrf_token=csrf_token))
    return (
        "<section class=\"mutation-group\">"
        "<h3>Emails</h3>"
        f"<p class=\"group-summary\">{escape(summary)}</p>"
        "<table><thead><tr><th>Status</th><th>Account</th><th>Delivery</th><th>Mode</th><th>To</th><th>Subject</th><th></th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
        f"{''.join(detail_sections)}"
        "</section>"
    )


def render_contact_group(*, request: dict[str, Any], mutations: list[dict[str, Any]], csrf_token: str) -> str:
    if not mutations:
        return ""
    operation_rows = []
    detail_sections = []
    op_counts: dict[str, int] = {}
    for mutation in mutations:
        preview = json_object(mutation.get("preview_json"))
        for operation in preview.get("operations", []):
            if not isinstance(operation, dict):
                continue
            op = str(operation.get("op") or "")
            op_counts[contact_op_short_label(op)] = op_counts.get(contact_op_short_label(op), 0) + 1
            op_index = int(operation.get("op_index") or 0)
            summary = json_object(operation.get("summary"))
            operation_rows.append(
                "<tr>"
                f"<td>{status_badge(str(mutation.get('status') or ''))}</td>"
                f"<td>{escape(str(mutation.get('account') or ''))}</td>"
                f"<td>{escape(contact_op_short_label(op))}</td>"
                f"<td><code>{escape(str(operation.get('resource_name') or ''))}</code></td>"
                f"<td>{contact_operation_effect(operation)}</td>"
                f"<td>{escape(str(summary.get('display_name') or summary.get('primary_email') or ''))}</td>"
                f"<td>{render_contact_row_actions(request, mutation, op_index, csrf_token)}</td>"
                "</tr>"
            )
            detail_sections.append(render_contact_operation_detail(operation, op_index=op_index))
    summary = ", ".join(format_count(count, label) for label, count in sorted(op_counts.items()))
    return (
        "<section class=\"mutation-group\">"
        "<h3>Contact Changes</h3>"
        f"<p class=\"group-summary\">{escape(summary)}</p>"
        "<table><thead><tr><th>Status</th><th>Account</th><th>Change</th><th>Resource</th><th>Effect</th><th>Name</th><th></th></tr></thead>"
        f"<tbody>{''.join(operation_rows)}</tbody></table>"
        f"{''.join(detail_sections)}"
        "</section>"
    )


def render_unknown_group(*, request: dict[str, Any], mutations: list[dict[str, Any]], csrf_token: str) -> str:
    rows = []
    for mutation in mutations:
        rows.append(
            "<tr>"
            f"<td>{status_badge(str(mutation.get('status') or ''))}</td>"
            f"<td>{escape(str(mutation.get('provider') or ''))}</td>"
            f"<td>{escape(str(mutation.get('operation') or ''))}</td>"
            f"<td>{escape(str(mutation.get('title') or ''))}</td>"
            f"<td>{render_request_row_actions(request, mutation, csrf_token)}</td>"
            "</tr>"
        )
    return (
        "<section class=\"mutation-group\">"
        "<h3>Other Changes</h3>"
        "<table><thead><tr><th>Status</th><th>Provider</th><th>Operation</th><th>Title</th><th></th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
        "</section>"
    )


def render_request_row_actions(request: dict[str, Any], mutation: dict[str, Any], csrf_token: str) -> str:
    mutation_id = str(mutation.get("id") or "")
    parts = [f"<a href=\"/mutations/{escape(mutation_id)}\">Open</a>"]
    if request["status"] == "pending_review" and mutation["status"] == "pending_review":
        parts.append(
            f"<form method=\"post\" action=\"/requests/{escape(str(request['id']))}/remove-mutation\" class=\"inline\">"
            f"<input type=\"hidden\" name=\"mutation_id\" value=\"{escape(mutation_id)}\">"
            f"{csrf_input(csrf_token)}"
            "<button type=\"submit\">Remove</button></form>"
        )
    return " ".join(parts)


def render_contact_row_actions(request: dict[str, Any], mutation: dict[str, Any], op_index: int, csrf_token: str) -> str:
    mutation_id = str(mutation.get("id") or "")
    parts = [f"<a href=\"/mutations/{escape(mutation_id)}\">Open</a>"]
    if request["status"] == "pending_review" and mutation["status"] == "pending_review":
        parts.append(
            f"<form method=\"post\" action=\"/mutations/{escape(mutation_id)}/remove-operation\" class=\"inline\">"
            f"<input type=\"hidden\" name=\"op_index\" value=\"{op_index}\">"
            f"{csrf_input(csrf_token)}"
            "<button type=\"submit\">Remove</button></form>"
        )
    return " ".join(parts)


def render_mutation_detail(warehouse, mutation_id: str, csrf_token: str) -> str:
    mutation = warehouse.get_upstream_mutation(mutation_id)
    if mutation is None:
        return page("Mutation not found", f"<p>No mutation exists for <code>{escape(mutation_id)}</code>.</p>")
    events = warehouse.list_upstream_mutation_events(mutation_id)
    payload = json_object(mutation.get("payload_json"))
    preview = json_object(mutation.get("preview_json"))
    details = ""
    thread_rows = []
    for thread in preview.get("threads", []):
        if not isinstance(thread, dict):
            continue
        thread_id = str(thread.get("thread_id") or "")
        remove_form = ""
        if mutation["status"] == "pending_review":
            remove_form = (
                "<form method=\"post\" action=\"/mutations/"
                f"{escape(mutation_id)}/remove-thread\" class=\"inline\">"
                f"<input type=\"hidden\" name=\"thread_id\" value=\"{escape(thread_id)}\">"
                f"{csrf_input(csrf_token)}"
                "<button type=\"submit\">Remove</button></form>"
            )
        thread_rows.append(
            "<tr>"
            f"<td><code>{escape(thread_id)}</code></td>"
            f"<td>{escape(str(thread.get('subject') or ''))}</td>"
            f"<td>{escape(str(thread.get('latest_from_address') or ''))}</td>"
            f"<td>{escape(str(thread.get('message_count') or thread.get('inbox_message_count') or ''))}</td>"
            f"<td>{remove_form}</td>"
            "</tr>"
        )
    if thread_rows:
        details += (
            "<h2>Threads</h2>"
            "<table><thead><tr><th>Thread ID</th><th>Subject</th><th>Latest From</th><th>Messages</th><th></th></tr></thead>"
            f"<tbody>{''.join(thread_rows)}</tbody></table>"
        )
    operation_rows = []
    operation_detail_sections = []
    for operation in preview.get("operations", []):
        if not isinstance(operation, dict):
            continue
        op_index = int(operation.get("op_index") or 0)
        remove_form = ""
        if mutation["status"] == "pending_review":
            remove_form = (
                "<form method=\"post\" action=\"/mutations/"
                f"{escape(mutation_id)}/remove-operation\" class=\"inline\">"
                f"<input type=\"hidden\" name=\"op_index\" value=\"{op_index}\">"
                f"{csrf_input(csrf_token)}"
                "<button type=\"submit\">Remove</button></form>"
            )
        summary = json_object(operation.get("summary"))
        operation_rows.append(
            "<tr>"
            f"<td>{op_index + 1}</td>"
            f"<td>{escape(str(operation.get('op') or ''))}</td>"
            f"<td><code>{escape(str(operation.get('resource_name') or ''))}</code></td>"
            f"<td>{contact_operation_effect(operation)}</td>"
            f"<td>{escape(str(summary.get('display_name') or summary.get('primary_email') or ''))}</td>"
            f"<td>{escape(str(summary.get('primary_email') or ''))}</td>"
            f"<td>{remove_form}</td>"
            "</tr>"
        )
        operation_detail_sections.append(render_contact_operation_detail(operation, op_index=op_index))
    if operation_rows:
        details += (
            "<h2>Contact Operations</h2>"
            "<table><thead><tr><th>#</th><th>Operation</th><th>Resource</th><th>Effect</th><th>Name</th><th>Email</th><th></th></tr></thead>"
            f"<tbody>{''.join(operation_rows)}</tbody></table>"
            f"{''.join(operation_detail_sections)}"
        )
    details += render_email_preview(mutation, allow_edits=True, heading_level=2, csrf_token=csrf_token)
    forms = ""
    if mutation["status"] == "pending_review":
        forms = (
            f"<form method=\"post\" action=\"/mutations/{escape(mutation_id)}/approve\">"
            f"{csrf_input(csrf_token)}"
            "<button type=\"submit\">Approve</button></form>"
            f"<form method=\"post\" action=\"/mutations/{escape(mutation_id)}/reject\">"
            f"{csrf_input(csrf_token)}"
            "<input name=\"reason\" type=\"text\" placeholder=\"Reason\">"
            "<button type=\"submit\">Reject</button></form>"
        )
    body = f"""
        <p><a href="/mutations">All mutations</a></p>
        <dl>
          <dt>ID</dt><dd><code>{escape(str(mutation['id']))}</code></dd>
          <dt>Status</dt><dd>{escape(str(mutation['status']))}</dd>
          <dt>Account</dt><dd>{escape(str(mutation['account']))}</dd>
          <dt>Reason</dt><dd>{escape(str(mutation['reason']))}</dd>
          <dt>Revision</dt><dd>{escape(str(mutation['revision']))}</dd>
        </dl>
        {details}
        <h2>Review</h2>
        <div class="actions">{forms or '<p>No pending review actions.</p>'}</div>
        <h2>Payload</h2>
        <pre>{escape(json.dumps(payload, indent=2, sort_keys=True, default=str))}</pre>
        <h2>Audit</h2>
        <pre>{escape(json.dumps(events, indent=2, sort_keys=True, default=str))}</pre>
    """
    return page(str(mutation["title"]), body)


def render_email_preview(mutation: dict[str, Any], *, allow_edits: bool, heading_level: int, csrf_token: str = "") -> str:
    if mutation.get("provider") != "gmail" or mutation.get("operation") != "gmail.send_email":
        return ""
    mutation_id = str(mutation["id"])
    payload = json_object(mutation.get("payload_json"))
    preview = json_object(mutation.get("preview_json"))
    message = json_object(payload.get("message"))
    email = json_object(preview.get("email")) or message
    delivery_mode = str(payload.get("delivery_mode") or email.get("delivery_mode") or "send")
    heading = f"h{heading_level}"
    readonly = not (allow_edits and mutation["status"] == "pending_review")
    rows = (
        "<dl>"
        f"<dt>Mode</dt><dd>{escape(str(email.get('mode') or 'new_thread'))}</dd>"
        f"<dt>Delivery</dt><dd>{escape(delivery_mode)}</dd>"
        f"<dt>To</dt><dd>{escape(', '.join(str(item) for item in email.get('to', [])))}</dd>"
        f"<dt>Cc</dt><dd>{escape(', '.join(str(item) for item in email.get('cc', [])))}</dd>"
        f"<dt>Bcc</dt><dd>{escape(', '.join(str(item) for item in email.get('bcc', [])))}</dd>"
        f"<dt>Subject</dt><dd>{escape(str(email.get('subject') or ''))}</dd>"
        "</dl>"
    )
    body = str(email.get("body_text") or "")
    if readonly:
        return f"<{heading}>Email</{heading}>{rows}<pre>{escape(body)}</pre>"
    action = f"/mutations/{escape(mutation_id)}/update-email"
    approve_action = f"/mutations/{escape(mutation_id)}/approve-email"
    return f"""
        <{heading}>Email</{heading}>
        <form method="post" action="{action}">
          {csrf_input(csrf_token)}
          <label>Delivery
            <select name="delivery_mode">
              <option value="send"{' selected' if delivery_mode == 'send' else ''}>send</option>
              <option value="draft"{' selected' if delivery_mode == 'draft' else ''}>draft</option>
            </select>
          </label>
          <label>To<input class="wide" name="to" value="{escape(', '.join(str(item) for item in email.get('to', [])))}"></label>
          <label>Cc<input class="wide" name="cc" value="{escape(', '.join(str(item) for item in email.get('cc', [])))}"></label>
          <label>Bcc<input class="wide" name="bcc" value="{escape(', '.join(str(item) for item in email.get('bcc', [])))}"></label>
          <label>Subject<input class="wide" name="subject" value="{escape(str(email.get('subject') or ''))}"></label>
          <label>Body<textarea name="body_text">{escape(body)}</textarea></label>
          <input type="hidden" name="body_html" value="{escape(str(email.get('body_html') or ''))}">
          <button type="submit">Save email</button>
          <button type="submit" formaction="{approve_action}" name="approve_delivery_mode" value="draft">Approve as draft</button>
          <button type="submit" formaction="{approve_action}" name="approve_delivery_mode" value="send">Approve and send</button>
        </form>
    """


def csrf_input(csrf_token: str) -> str:
    return f"<input type=\"hidden\" name=\"csrf_token\" value=\"{escape(csrf_token)}\">"


def login_page(next_path: str, *, error: str = "") -> str:
    error_html = f"<p class=\"error\">{escape(error)}</p>" if error else ""
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Sign in</title>
  <style>
    body {{ font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; min-height: 100vh; display: grid; place-items: center; color: #111827; background: #f8fafc; }}
    main {{ width: min(360px, calc(100vw - 32px)); background: white; border: 1px solid #d8dee8; border-radius: 8px; padding: 24px; box-sizing: border-box; }}
    label {{ display: block; font-weight: 600; margin-bottom: 8px; }}
    input {{ width: 100%; box-sizing: border-box; font: inherit; padding: 8px 10px; margin: 0 0 12px; }}
    button {{ font: inherit; border: 1px solid #0f766e; background: #0f766e; color: white; border-radius: 6px; padding: 8px 10px; cursor: pointer; }}
    .error {{ color: #b91c1c; }}
  </style>
</head>
<body><main><h1>Sign in</h1>{error_html}<form method="post" action="/login">
  <input type="hidden" name="next" value="{escape(next_path)}">
  <label for="password">Password</label>
  <input id="password" name="password" type="password" autocomplete="current-password" autofocus required>
  <button type="submit">Sign in</button>
</form></main></body>
</html>"""


def page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{escape(title)}</title>
  <style>
    body {{ font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111827; background: #f8fafc; }}
    main {{ max-width: 1120px; margin: 0 auto; background: white; border: 1px solid #d8dee8; border-radius: 8px; padding: 24px; }}
    h3 {{ margin-top: 28px; }}
    h4 {{ margin: 18px 0 8px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 20px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ color: #4b5563; font-size: 12px; text-transform: uppercase; }}
    input, textarea, button {{ font: inherit; padding: 6px 8px; margin-right: 6px; }}
    textarea {{ width: 100%; min-height: 160px; box-sizing: border-box; display: block; margin: 4px 0 10px; }}
    input.wide {{ width: 100%; box-sizing: border-box; display: block; margin: 4px 0 10px; }}
    button {{ border: 1px solid #0f766e; background: #0f766e; color: white; border-radius: 6px; cursor: pointer; }}
    form {{ margin: 0 0 8px; }}
    form.inline {{ display: inline; }}
    pre {{ overflow: auto; background: #f3f4f6; padding: 12px; border-radius: 6px; }}
    pre.compact {{ margin: 0; max-height: 220px; }}
    details {{ border: 1px solid #e5e7eb; border-radius: 6px; padding: 10px 12px; margin: 10px 0 16px; }}
    summary {{ cursor: pointer; font-weight: 600; }}
    .muted {{ color: #6b7280; }}
    .summary-strip {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin: 12px 0 24px; }}
    .summary-strip div {{ border: 1px solid #e5e7eb; border-radius: 6px; padding: 10px 12px; }}
    .summary-strip strong {{ display: block; font-size: 18px; }}
    .summary-strip span, .group-summary {{ color: #6b7280; }}
    .mutation-group {{ border-top: 1px solid #e5e7eb; padding-top: 4px; }}
    .status {{ display: inline-block; border-radius: 999px; padding: 2px 8px; background: #eef2ff; color: #3730a3; }}
    dt {{ font-weight: 600; float: left; clear: left; width: 96px; }}
    dd {{ margin-left: 112px; margin-bottom: 6px; }}
  </style>
</head>
<body><main><nav><a href="/requests">Requests</a> <a href="/logout">Log out</a></nav><h1>{escape(title)}</h1>{body}</main></body>
</html>"""


def mutation_id_from_path(path: str) -> str | None:
    parts = [part for part in path.split("/") if part]
    if len(parts) == 2 and parts[0] == "mutations":
        return parts[1]
    return None


def request_id_from_path(path: str) -> str | None:
    parts = [part for part in path.split("/") if part]
    if len(parts) == 2 and parts[0] == "requests":
        return parts[1]
    return None


def mutation_id_from_action_path(path: str) -> str | None:
    parts = [part for part in path.split("/") if part]
    if len(parts) == 3 and parts[0] == "mutations":
        return parts[1]
    return None


def request_id_from_action_path(path: str) -> str | None:
    parts = [part for part in path.split("/") if part]
    if len(parts) == 3 and parts[0] == "requests":
        return parts[1]
    return None


def form_value(form: dict[str, list[str]], name: str) -> str:
    values = form.get(name) or [""]
    return values[0]


def form_list(form: dict[str, list[str]], name: str) -> list[str]:
    raw = form_value(form, name)
    values = raw.replace("\n", ",").split(",")
    return [value.strip() for value in values if value.strip()]


def email_message_from_form(form: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "to": form_list(form, "to"),
        "cc": form_list(form, "cc"),
        "bcc": form_list(form, "bcc"),
        "subject": form_value(form, "subject").strip(),
        "body_text": form_value(form, "body_text"),
        "body_html": form_value(form, "body_html"),
    }


def email_delivery_mode_from_form(form: dict[str, list[str]]) -> str:
    return form_value(form, "approve_delivery_mode") or form_value(form, "delivery_mode")


def counts_by(items: list[dict[str, Any]], key_fn: Callable[[dict[str, Any]], str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        key = key_fn(item)
        if key:
            counts[key] = counts.get(key, 0) + 1
    return counts


def format_count(count: int, label: str) -> str:
    suffix = "" if count == 1 or label.endswith("s") else "s"
    return f"{count} {label}{suffix}"


def mutation_group_label(mutation: dict[str, Any]) -> str:
    operation = str(mutation.get("operation") or "")
    if operation == "gmail.archive_threads":
        return "archive"
    if operation == "gmail.unarchive_threads":
        return "unarchive"
    if operation == "gmail.send_email":
        return "email"
    if operation == "contacts.batch_mutation":
        return "contact"
    return operation or "unknown"


def short_status(status: str) -> str:
    return {
        "pending_review": "pending",
        "failed_retryable": "retryable failure",
        "failed_terminal": "terminal failure",
        "blocked_missing_credentials": "blocked",
    }.get(status, status.replace("_", " "))


def status_badge(status: str) -> str:
    return f"<span class=\"status\">{escape(short_status(status))}</span>"


def email_delivery_mode(mutation: dict[str, Any]) -> str:
    payload = json_object(mutation.get("payload_json"))
    return str(payload.get("delivery_mode") or "send")


def contact_op_short_label(op: str) -> str:
    return {
        "create_contact": "create",
        "update_contact": "update",
        "delete_contact": "delete",
    }.get(op, op)


def json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    return {}


def contact_operation_effect(operation: dict[str, Any]) -> str:
    op = str(operation.get("op") or "")
    if op == "update_contact":
        fields = [str(field) for field in operation.get("update_person_fields", []) if str(field)]
        return "Replaces " + ", ".join(fields) if fields else "Replaces selected fields"
    if op == "delete_contact":
        return "Deletes this contact"
    if op == "create_contact":
        return "Creates a contact"
    return ""


def render_contact_operation_detail(operation: dict[str, Any], *, op_index: int) -> str:
    op = str(operation.get("op") or "")
    if op == "update_contact":
        before = json_object(operation.get("before"))
        after = json_object(operation.get("after"))
        fields = [str(field) for field in operation.get("update_person_fields", []) if str(field)]
        rows = []
        for field in fields:
            rows.append(
                "<tr>"
                f"<td><code>{escape(field)}</code></td>"
                f"<td><pre class=\"compact\">{escape(json.dumps(before.get(field), indent=2, sort_keys=True, default=str))}</pre></td>"
                f"<td><pre class=\"compact\">{escape(json.dumps(after.get(field), indent=2, sort_keys=True, default=str))}</pre></td>"
                "</tr>"
            )
        return (
            f"<details open><summary>Operation {op_index + 1}: update replaces listed contact fields</summary>"
            "<p class=\"muted\">Fields not listed here are not part of this update.</p>"
            "<table><thead><tr><th>Field</th><th>Before</th><th>After</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></details>"
        )
    if op == "delete_contact":
        before = json_object(operation.get("before"))
        return (
            f"<details><summary>Operation {op_index + 1}: delete contact</summary>"
            f"<pre class=\"compact\">{escape(json.dumps(before, indent=2, sort_keys=True, default=str))}</pre></details>"
        )
    if op == "create_contact":
        after = json_object(operation.get("after"))
        return (
            f"<details><summary>Operation {op_index + 1}: create contact</summary>"
            f"<pre class=\"compact\">{escape(json.dumps(after, indent=2, sort_keys=True, default=str))}</pre></details>"
        )
    return ""
