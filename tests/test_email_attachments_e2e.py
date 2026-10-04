"""Real proposal/review API -> Postgres -> Python worker -> Gmail HTTP/MIME.

Only Gmail is substituted, with a loopback HTTP server. No real email, credentials,
or production data is used. The isolated database is removed even on failure.
"""
from __future__ import annotations

import base64
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
from threading import Thread

from googleapiclient.discovery import build
import httplib2
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn
import pytest

from personal_data_warehouse.gmail_mutations import GmailMutationExecutor
from personal_data_warehouse.gmail_sync import attachment_rows_for_message, message_to_row
from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import make_test_schema


@contextmanager
def _isolated_warehouse() -> Iterator[tuple[PostgresWarehouse, str]]:
    admin = psycopg2.connect(os.environ["POSTGRES_DATABASE_URL"])
    admin.autocommit = True
    database = make_test_schema("email")
    warehouse = None
    with admin.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        url = make_dsn(os.environ["POSTGRES_DATABASE_URL"], dbname=database)
        warehouse = PostgresWarehouse(url)
        warehouse.ensure_upstream_mutation_tables()
        warehouse._ensure_table_group(["gmail_messages", "gmail_attachments"])
        yield warehouse, url
    finally:
        if warehouse is not None:
            warehouse.close()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        admin.close()


def _run_go_test(name: str, env: dict[str, str]) -> None:
    result = subprocess.run(
        ["go", "test", "./internal/mutations", "-run", f"^{name}$", "-count=1", "-v"],
        cwd=Path(__file__).resolve().parents[1] / "app",
        env={**os.environ, **env},
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    # A skipped Go test exits 0 too; this half of the test must actually run.
    assert f"--- PASS: {name}" in result.stdout, result.stdout + result.stderr


@contextmanager
def _fake_gmail(originals: dict[str, dict] | None = None) -> Iterator[tuple[object, list]]:
    """A loopback Gmail API: records every send/draft, serves `originals` by id."""
    received: list[tuple[str, dict]] = []
    originals = originals or {}

    class GmailHandler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def _reply(self, status: int, response: dict) -> None:
            encoded = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):
            message_id = self.path.split("?")[0].rsplit("/", 1)[-1]
            if message_id in originals and "format=raw" in self.path:
                self._reply(200, originals[message_id])
            else:
                self._reply(404, {"error": {"code": 404, "message": "Requested entity was not found."}})

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received.append((self.path, body))
            gmail_message = body["message"] if "/drafts" in self.path else body
            thread_id = gmail_message.get("threadId", "thread-1")
            self._reply(
                200,
                {"id": "draft-1", "message": {"id": "draft-message-1", "threadId": thread_id}}
                if "/drafts" in self.path
                else {"id": "sent-message-1", "threadId": thread_id},
            )

    with ThreadingHTTPServer(("127.0.0.1", 0), GmailHandler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        service = build("gmail", "v1", http=httplib2.Http(proxy_info=None), static_discovery=True)
        service._baseUrl = f"http://127.0.0.1:{server.server_port}/"
        try:
            yield service, received
        finally:
            service.close()
            server.shutdown()
            thread.join(timeout=5)


def _execute_claimed(warehouse: PostgresWarehouse, service, *, expected: int, actor: str) -> None:
    claimed = warehouse.claim_approved_upstream_mutations(limit=10, claimed_by=actor)
    assert len(claimed) == expected
    executor = GmailMutationExecutor(settings=object(), service_factory=lambda _: service)
    for mutation in claimed:
        result = executor.execute(mutation)
        assert result.status == "succeeded", result.error
        warehouse.complete_upstream_mutation(mutation["id"], result_json=result.result_json, actor_id=actor)
        assert warehouse.get_upstream_mutation(mutation["id"])["status"] == "succeeded"
    assert warehouse.claim_approved_upstream_mutations(limit=10, claimed_by=actor) == []


def _parse_raw(raw: str):
    return BytesParser(policy=policy.default).parsebytes(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))


@pytest.mark.local_integration
def test_email_attachment_proposal_review_approval_delivery():
    with _isolated_warehouse() as (warehouse, url):
        _run_go_test("TestEmailAttachmentsProposalReviewApproval", {"PDW_EMAIL_ATTACHMENT_TEST_URL": url})
        with _fake_gmail() as (service, received):
            _execute_claimed(warehouse, service, expected=2, actor="attachment-e2e")
        assert {path for path, _ in received} == {"/gmail/v1/users/me/messages/send?alt=json", "/gmail/v1/users/me/drafts?alt=json"}
        for path, body in received:
            gmail_message = body["message"] if "/drafts" in path else body
            email = _parse_raw(gmail_message["raw"])
            assert email["Subject"].startswith("Reviewed ")
            assert email.get_body(preferencelist=("plain",)).get_content().strip() == "Final plain"
            assert email.get_body(preferencelist=("html",)).get_content().strip() == "<p>Final HTML</p>"
            attachments = list(email.iter_attachments())
            assert len(attachments) == 1
            assert attachments[0].get_filename() == "résumé.bin"
            assert attachments[0].get_payload(decode=True) == bytes([0, 255, 1, 128]) * 25000


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii")


def _invoice_original() -> tuple[bytes, dict]:
    """One received message as Gmail holds it: its raw MIME and its API "full" form."""
    headers = {
        "From": "Vendor <billing@vendor.test>",
        "To": "forwarder@example.test",
        "Subject": "Invoice 4831",
        "Message-ID": "<invoice-4831@vendor.test>",
        "Date": "Tue, 29 Sep 2026 15:04:00 +0000",
    }
    mime = EmailMessage()
    for name, value in headers.items():
        mime[name] = value
    mime.set_content("Your invoice is attached.")
    mime.add_alternative("<p>Your invoice is attached.</p>", subtype="html")
    mime.add_attachment(b"%PDF-invoice", maintype="application", subtype="pdf", filename="invoice-4831.pdf")
    full = {
        "id": "orig-1",
        "threadId": "thread-orig",
        "historyId": "7",
        "internalDate": str(int(datetime(2026, 9, 29, 15, 4, tzinfo=UTC).timestamp() * 1000)),
        "labelIds": ["INBOX"],
        "snippet": "Your invoice is attached.",
        "payload": {
            "partId": "",
            "mimeType": "multipart/mixed",
            "headers": [{"name": name, "value": value} for name, value in headers.items()],
            "body": {"size": 0},
            "parts": [
                {
                    "partId": "0",
                    "mimeType": "multipart/alternative",
                    "filename": "",
                    "body": {"size": 0},
                    "parts": [
                        {"partId": "0.0", "mimeType": "text/plain", "filename": "",
                         "body": {"size": 25, "data": _b64url(b"Your invoice is attached.")}},
                        {"partId": "0.1", "mimeType": "text/html", "filename": "",
                         "body": {"size": 32, "data": _b64url(b"<p>Your invoice is attached.</p>")}},
                    ],
                },
                {
                    "partId": "1",
                    "mimeType": "application/pdf",
                    "filename": "invoice-4831.pdf",
                    "headers": [{"name": "Content-Disposition", "value": 'attachment; filename="invoice-4831.pdf"'}],
                    "body": {"attachmentId": "att-1", "size": 12},
                },
            ],
        },
    }
    return mime.as_bytes(), full


@pytest.mark.local_integration
def test_email_forward_proposal_review_approval_delivery():
    raw, full = _invoice_original()
    with _isolated_warehouse() as (warehouse, url):
        synced_at = datetime(2026, 9, 29, 15, 5, tzinfo=UTC)
        account = "forwarder@example.test"
        warehouse.insert_messages([message_to_row(account=account, message=full, synced_at=synced_at)])
        warehouse.insert_attachments(
            attachment_rows_for_message(
                account=account, service=None, message=full, synced_at=synced_at,
                existing_keys=set(), max_bytes=0, text_max_chars=0,
            )
        )
        _run_go_test("TestGmailForwardProposalReviewApproval", {"PDW_EMAIL_FORWARD_TEST_URL": url})
        originals = {"orig-1": {"id": "orig-1", "threadId": "thread-orig", "raw": _b64url(raw)}}
        with _fake_gmail(originals) as (service, received):
            _execute_claimed(warehouse, service, expected=1, actor="forward-e2e")

    assert [path for path, _ in received] == ["/gmail/v1/users/me/messages/send?alt=json"]
    sent = received[0][1]
    # Filed in the original's conversation, as Gmail's own Forward is.
    assert sent["threadId"] == "thread-orig"
    email = _parse_raw(sent["raw"])
    assert email["Subject"] == "Fwd: Invoice 4831"
    assert email["To"] == "accountant@example.test"
    assert email["In-Reply-To"] == "<invoice-4831@vendor.test>"
    assert email["References"] == "<invoice-4831@vendor.test>"
    text = email.get_body(preferencelist=("plain",)).get_content()
    assert text.startswith("Please file this one.")
    assert text.count("---------- Forwarded message ---------") == 1
    assert "From: Vendor <billing@vendor.test>" in text
    html = email.get_body(preferencelist=("html",)).get_content()
    assert html.count("---------- Forwarded message ---------") == 1
    assert "<p>Your invoice is attached.</p>" in html
    attachments = list(email.iter_attachments())
    assert [part.get_filename() for part in attachments] == ["invoice-4831.pdf"]
    assert attachments[0].get_payload(decode=True) == b"%PDF-invoice"
