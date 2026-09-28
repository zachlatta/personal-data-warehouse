"""Real proposal/review API -> Postgres -> Python worker -> Gmail HTTP/MIME.

Only Gmail is substituted, with a loopback HTTP server. No real email, credentials,
or production data is used. The isolated database is removed even on failure.
"""
from __future__ import annotations

import base64
from email import policy
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
from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import make_test_schema


@pytest.mark.local_integration
def test_email_attachment_proposal_review_approval_delivery():
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
        warehouse._ensure_table_group(["gmail_messages"])
        result = subprocess.run(
            ["go", "test", "./internal/mutations", "-run", "^TestEmailAttachmentsProposalReviewApproval$", "-count=1"],
            cwd=Path(__file__).resolve().parents[1] / "app",
            env={**os.environ, "PDW_EMAIL_ATTACHMENT_TEST_URL": url},
            capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        claimed = warehouse.claim_approved_upstream_mutations(limit=10, claimed_by="attachment-e2e")
        assert len(claimed) == 2
        received = []

        class GmailHandler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                received.append((self.path, body))
                response = (
                    {"id": "draft-1", "message": {"id": "draft-message-1", "threadId": "thread-1"}}
                    if "/drafts" in self.path
                    else {"id": "sent-message-1", "threadId": "thread-1"}
                )
                encoded = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        with ThreadingHTTPServer(("127.0.0.1", 0), GmailHandler) as server:
            thread = Thread(target=server.serve_forever, daemon=True)
            thread.start()
            service = build("gmail", "v1", http=httplib2.Http(proxy_info=None), static_discovery=True)
            service._baseUrl = f"http://127.0.0.1:{server.server_port}/"
            try:
                executor = GmailMutationExecutor(settings=object(), service_factory=lambda _: service)
                for mutation in claimed:
                    result = executor.execute(mutation)
                    assert result.status == "succeeded", result.error
                    warehouse.complete_upstream_mutation(
                        mutation["id"], result_json=result.result_json, actor_id="attachment-e2e"
                    )
                    assert warehouse.get_upstream_mutation(mutation["id"])["status"] == "succeeded"
            finally:
                service.close()
                server.shutdown()
                thread.join(timeout=5)
        assert {path for path, _ in received} == {"/gmail/v1/users/me/messages/send?alt=json", "/gmail/v1/users/me/drafts?alt=json"}
        for path, body in received:
            gmail_message = body["message"] if "/drafts" in path else body
            email = BytesParser(policy=policy.default).parsebytes(base64.urlsafe_b64decode(gmail_message["raw"]))
            assert email["Subject"].startswith("Reviewed ")
            assert email.get_body(preferencelist=("plain",)).get_content().strip() == "Final plain"
            assert email.get_body(preferencelist=("html",)).get_content().strip() == "<p>Final HTML</p>"
            attachments = list(email.iter_attachments())
            assert len(attachments) == 1
            assert attachments[0].get_filename() == "résumé.bin"
            assert attachments[0].get_payload(decode=True) == bytes([0, 255, 1, 128]) * 25000
        assert warehouse.claim_approved_upstream_mutations(limit=10, claimed_by="attachment-e2e") == []
    finally:
        if warehouse is not None:
            warehouse.close()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        admin.close()
