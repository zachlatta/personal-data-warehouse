from hashlib import sha256
from unittest.mock import MagicMock
import pytest

from personal_data_warehouse.google_drive_mutations import GoogleDriveMutationExecutor


def executor(service, data=b"hello"):
    return GoogleDriveMutationExecutor(
        settings=None, service_factory=lambda account: service, read_upload=lambda payload: data
    )


def test_upload_verifies_bytes_before_touching_drive():
    service = MagicMock()
    result = executor(service, b"wrong").execute(
        {
            "id": "mut-1",
            "account": "you@example.com",
            "operation": "google_drive.upload_file",
            "payload_json": {
                "sha256": sha256(b"hello").hexdigest(),
                "size_bytes": 5,
                "name": "report.txt",
                "parent_id": "folder",
                "mime_type": "text/plain",
            },
        }
    )
    assert result.status == "failed_terminal"
    service.files.return_value.create.assert_not_called()


def test_permission_create_does_not_retry_an_uncertain_write():
    service = MagicMock()
    service.permissions.return_value.create.return_value.execute.side_effect = TimeoutError()
    result = executor(service).execute(
        {
            "id": "mut-1",
            "account": "you@example.com",
            "operation": "google_drive.add_permission",
            "payload_json": {
                "file_id": "f",
                "permission_type": "user",
                "email_address": "person@example.com",
                "role": "reader",
                "send_notification_email": False,
            },
        }
    )
    assert result.status == "failed_terminal"
    assert "may have" in result.error


def test_remove_permission_calls_drive_after_approval():
    service = MagicMock()
    service.permissions.return_value.get.return_value.execute.return_value = {
        "type": "user",
        "role": "reader",
        "emailAddress": "person@example.com",
    }
    result = executor(service).execute(
        {
            "id": "mut-1",
            "account": "you@example.com",
            "operation": "google_drive.remove_permission",
            "payload_json": {
                "file_id": "f",
                "permission_id": "p",
                "permission_type": "user",
                "expected_role": "reader",
                "email_address": "person@example.com",
            },
        }
    )
    assert result.status == "succeeded"
    service.permissions.return_value.delete.assert_called_once_with(
        fileId="f", permissionId="p", supportsAllDrives=True
    )


@pytest.mark.parametrize(
    "kind,recipient",
    [
        ("user", {"email_address": "person@example.com"}),
        ("group", {"email_address": "team@example.com"}),
        ("domain", {"domain": "example.com"}),
        ("anyone", {}),
    ],
)
def test_add_permission_preserves_explicit_notification_choice(kind, recipient):
    service = MagicMock()
    service.permissions.return_value.create.return_value.execute.return_value = {"id": "permission"}
    payload = {"file_id": "f", "permission_type": kind, "role": "reader", "send_notification_email": False, **recipient}
    result = executor(service).execute(
        {"id": "m", "account": "you@example.com", "operation": "google_drive.add_permission", "payload_json": payload}
    )
    assert result.status == "succeeded"
    args = service.permissions.return_value.create.call_args.kwargs
    assert args["body"]["type"] == kind
    assert args["sendNotificationEmail"] is False


@pytest.mark.parametrize(
    "current",
    [
        {"type": "user", "role": "writer", "emailAddress": "person@example.com"},
        {"type": "user", "role": "reader", "emailAddress": "different@example.com"},
        {
            "type": "user",
            "role": "reader",
            "emailAddress": "person@example.com",
            "permissionDetails": [{"inherited": True}],
        },
    ],
)
def test_permission_changes_and_inheritance_are_refused(current):
    service = MagicMock()
    service.permissions.return_value.get.return_value.execute.return_value = current
    result = executor(service).execute(
        {
            "id": "m",
            "account": "you@example.com",
            "operation": "google_drive.update_permission",
            "payload_json": {
                "file_id": "f",
                "permission_id": "p",
                "permission_type": "user",
                "email_address": "person@example.com",
                "expected_role": "reader",
                "role": "writer",
            },
        }
    )
    assert result.status == "failed_terminal"
    service.permissions.return_value.update.assert_not_called()


def test_upload_sends_the_hash_checked_bytes():
    service = MagicMock()
    service.files.return_value.get.return_value.execute.return_value = {
        "mimeType": "application/vnd.google-apps.folder"
    }
    service.files.return_value.create.return_value.execute.return_value = {"id": "new-file"}
    result = executor(service).execute(
        {
            "id": "m",
            "account": "you@example.com",
            "operation": "google_drive.upload_file",
            "payload_json": {
                "parent_id": "folder",
                "name": "report.txt",
                "mime_type": "text/plain",
                "size_bytes": 5,
                "sha256": sha256(b"hello").hexdigest(),
            },
        }
    )
    assert result.status == "succeeded"
    args = service.files.return_value.create.call_args.kwargs
    assert args["body"]["name"] == "report.txt"
    assert args["media_body"].getbytes(0, 5) == b"hello"
