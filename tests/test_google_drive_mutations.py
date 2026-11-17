from hashlib import sha256
from unittest.mock import MagicMock
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from googleapiclient.errors import HttpError
from httplib2 import Response

from personal_data_warehouse.google_drive_mutations import GoogleDriveMutationExecutor
from personal_data_warehouse import google_drive_mutations


@pytest.mark.parametrize(
    "endpoint,allowed",
    [
        ("http://100.64.0.1:3900", True),
        ("http://nas.example-tailnet.ts.net:3900", True),
        ("https://storage.example.test", True),
        ("http://storage.example.test", False),
        ("http://100.128.0.1:3900", False),
        ("http://nas.example-tailnet.ts.net/path", False),
    ],
)
def test_staging_endpoint_and_region_are_explicit(monkeypatch, endpoint, allowed):
    for name in ("BUCKET", "ACCESS_KEY", "SECRET_KEY"):
        monkeypatch.setenv("PDW_MUTATION_S3_" + name, "test-placeholder")
    monkeypatch.setenv("PDW_MUTATION_S3_ENDPOINT_URL", endpoint)
    monkeypatch.setenv("PDW_MUTATION_S3_REGION", "test-region")
    client = MagicMock()
    monkeypatch.setattr(google_drive_mutations.boto3, "client", client)
    if not allowed:
        with pytest.raises(ValueError):
            google_drive_mutations.staging_client()
        client.assert_not_called()
    else:
        google_drive_mutations.staging_client()
        assert client.call_args.kwargs["region_name"] == "test-region"
    monkeypatch.delenv("PDW_MUTATION_S3_REGION")
    with pytest.raises(RuntimeError, match="configuration is missing"):
        google_drive_mutations.staging_client()


def test_folder_observation_requires_initial_permissions():
    service = MagicMock()
    service.files.return_value.get.return_value.execute.return_value = {
        "id": "folder",
        "name": "Project",
        "parents": ["parent"],
        "mimeType": "application/vnd.google-apps.folder",
    }
    mutation = {
        "account": "you@example.com",
        "operation": "google_drive.create_folder",
        "payload_json": {
            "name": "Project",
            "parent_id": "parent",
            "initial_permissions": [{"permission_type": "anyone", "role": "reader", "send_notification_email": False}],
        },
        "result_json": {"id": "folder", "initial_permissions": [{"id": "permission"}]},
    }
    service.permissions.return_value.get.return_value.execute.return_value = {
        "id": "permission",
        "type": "anyone",
        "role": "writer",
    }
    assert not executor(service).observed(mutation)
    service.permissions.return_value.get.return_value.execute.return_value["role"] = "reader"
    assert executor(service).observed(mutation)


@pytest.mark.parametrize(
    "error,status,message",
    [
        (EndpointConnectionError(endpoint_url="http://storage.invalid"), "failed_retryable", "staging"),
        (
            ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject"),
            "failed_terminal",
            "staged bytes expired; propose again",
        ),
    ],
)
def test_staging_errors_before_drive_writes(error, status, message):
    service = MagicMock()

    def read(_payload):
        raise error

    result = GoogleDriveMutationExecutor(
        settings=None, service_factory=lambda account: service, read_upload=read
    ).execute({"id": "m", "account": "you@example.com", "operation": "google_drive.upload_file", "payload_json": {}})
    assert result.status == status
    assert message in result.error
    service.files.assert_not_called()


def test_create_and_share_folder_in_one_execution():
    service = MagicMock()
    service.files.return_value.get.return_value.execute.return_value = {
        "mimeType": "application/vnd.google-apps.folder"
    }
    service.files.return_value.create.return_value.execute.return_value = {"id": "created-folder", "name": "Project"}
    service.permissions.return_value.create.return_value.execute.return_value = {"id": "permission-1"}
    mutation = {
        "id": "m",
        "account": "you@example.com",
        "operation": "google_drive.create_folder",
        "payload_json": {
            "name": "Project",
            "parent_id": "parent",
            "initial_permissions": [
                {
                    "permission_type": "user",
                    "email_address": "person@example.com",
                    "role": "reader",
                    "send_notification_email": False,
                }
            ],
        },
    }
    result = executor(service).execute(mutation)
    assert result.status == "succeeded"
    args = service.permissions.return_value.create.call_args.kwargs
    assert args["fileId"] == "created-folder"
    assert args["sendNotificationEmail"] is False
    assert result.result_json["initial_permissions"][0]["id"] == "permission-1"


def test_partial_folder_sharing_failure_preserves_created_folder():
    service = MagicMock()
    service.files.return_value.get.return_value.execute.return_value = {
        "mimeType": "application/vnd.google-apps.folder"
    }
    service.files.return_value.create.return_value.execute.return_value = {"id": "created-folder"}
    service.permissions.return_value.create.return_value.execute.side_effect = TimeoutError()
    result = executor(service).execute(
        {
            "id": "m",
            "account": "you@example.com",
            "operation": "google_drive.create_folder",
            "payload_json": {
                "name": "Project",
                "parent_id": "parent",
                "initial_permissions": [
                    {"permission_type": "anyone", "role": "reader", "send_notification_email": False}
                ],
            },
        }
    )
    assert result.status == "failed_terminal"
    assert result.result_json["id"] == "created-folder"
    assert "may have" in result.error


def test_folder_sharing_retry_keeps_progress_even_when_parent_read_fails():
    service = MagicMock()
    service.files.return_value.get.return_value.execute.return_value = {
        "mimeType": "application/vnd.google-apps.folder"
    }
    service.files.return_value.create.return_value.execute.return_value = {"id": "created-folder"}
    service.permissions.return_value.create.return_value.execute.side_effect = [
        {"id": "permission-1"},
        HttpError(Response({"status": "403"}), b"{}"),
    ]
    mutation = {
        "id": "m",
        "account": "you@example.com",
        "operation": "google_drive.create_folder",
        "payload_json": {
            "name": "Project",
            "parent_id": "parent",
            "initial_permissions": [
                {
                    "permission_type": "user",
                    "email_address": "one@example.com",
                    "role": "reader",
                    "send_notification_email": False,
                },
                {
                    "permission_type": "user",
                    "email_address": "two@example.com",
                    "role": "reader",
                    "send_notification_email": False,
                },
            ],
        },
    }
    runner = executor(service)
    first = runner.execute(mutation)
    assert first.status == "blocked_missing_credentials"
    mutation["result_json"] = first.result_json
    service.files.return_value.get.return_value.execute.side_effect = ConnectionError("unreachable")
    second = runner.execute(mutation)
    assert second.status == "failed_retryable"
    assert second.result_json["id"] == "created-folder"
    mutation["result_json"] = second.result_json
    service.files.return_value.get.return_value.execute.side_effect = None
    service.permissions.return_value.create.return_value.execute.side_effect = None
    service.permissions.return_value.create.return_value.execute.return_value = {"id": "permission-2"}
    third = runner.execute(mutation)
    assert third.status == "succeeded"
    assert len(third.result_json["initial_permissions"]) == 2
    service.files.return_value.create.assert_called_once()
    assert service.permissions.return_value.create.call_count == 3


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
