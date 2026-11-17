"""Google Drive writes through the existing human-approved mutation queue.

Creates and sharing notifications are never automatically replayed after an
uncertain response. Stale executing claims are deliberately not reclaimable.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO

import boto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    ConnectTimeoutError,
    ReadTimeoutError,
)
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from personal_data_warehouse.calendar_mutations import calendar_mutation_failure_status
from personal_data_warehouse.config import Settings
from personal_data_warehouse.objectstore.google_drive import build_google_drive_service


GOOGLE_DRIVE_PROVIDER = "google_drive"
GOOGLE_DRIVE_UPLOAD_FILE_OPERATION = "google_drive.upload_file"
GOOGLE_DRIVE_CREATE_FOLDER_OPERATION = "google_drive.create_folder"
GOOGLE_DRIVE_ADD_PERMISSION_OPERATION = "google_drive.add_permission"
GOOGLE_DRIVE_UPDATE_PERMISSION_OPERATION = "google_drive.update_permission"
GOOGLE_DRIVE_REMOVE_PERMISSION_OPERATION = "google_drive.remove_permission"
GOOGLE_DRIVE_OPERATIONS = frozenset(
    {
        GOOGLE_DRIVE_UPLOAD_FILE_OPERATION,
        GOOGLE_DRIVE_CREATE_FOLDER_OPERATION,
        GOOGLE_DRIVE_ADD_PERMISSION_OPERATION,
        GOOGLE_DRIVE_UPDATE_PERMISSION_OPERATION,
        GOOGLE_DRIVE_REMOVE_PERMISSION_OPERATION,
    }
)


@dataclass(frozen=True)
class DriveMutationResult:
    status: str
    result_json: dict
    error: str = ""


def staging_client(settings: Settings):
    staging = settings.mutation_staging
    if staging is None:
        raise RuntimeError("PDW_MUTATION_S3 configuration is missing")
    return boto3.client(
        "s3",
        endpoint_url=staging.endpoint_url,
        aws_access_key_id=staging.access_key,
        aws_secret_access_key=staging.secret_key,
        region_name=staging.region,
        config=Config(
            signature_version="s3v4",
            connect_timeout=15,
            read_timeout=60,
            s3={"addressing_style": staging.addressing_style},
        ),
    )


def read_staged_upload(payload, *, settings: Settings):
    key = payload["staging_key"]
    if not key.startswith("pending/sha256/" + payload["sha256"] + "/"):
        raise ValueError("invalid staging key")
    response = staging_client(settings).get_object(Bucket=settings.mutation_staging.bucket, Key=key)
    with response["Body"] as body:
        return body.read((20 << 20) + 1)


class GoogleDriveMutationExecutor:
    def __init__(self, *, settings, service_factory=None, read_upload=None):
        self.settings = settings
        self.service_factory = service_factory or (
            lambda account: build_google_drive_service(account=account, settings=settings)
        )
        self.read_upload = read_upload or (lambda payload: read_staged_upload(payload, settings=self.settings))

    def observed(self, mutation):
        """Read upstream only; a successful write is observed independently."""
        try:
            service = self.service_factory(mutation["account"])
            p, result = mutation["payload_json"], mutation["result_json"]
            op = mutation["operation"]
            if op in {GOOGLE_DRIVE_UPLOAD_FILE_OPERATION, GOOGLE_DRIVE_CREATE_FOLDER_OPERATION}:
                current = (
                    service.files()
                    .get(fileId=result["id"], fields="id,name,mimeType,parents,size,trashed", supportsAllDrives=True)
                    .execute()
                )
                file_matches = (
                    not current.get("trashed")
                    and current.get("name") == p["name"]
                    and p["parent_id"] in current.get("parents", [])
                    and current.get("mimeType") == p.get("mime_type", "application/vnd.google-apps.folder")
                    and (op != GOOGLE_DRIVE_UPLOAD_FILE_OPERATION or int(current.get("size", -1)) == p["size_bytes"])
                )
                permissions = p.get("initial_permissions", [])
                completed = result.get("initial_permissions", [])
                return (
                    file_matches
                    and len(completed) == len(permissions)
                    and all(
                        self.observed(
                            {
                                "account": mutation["account"],
                                "operation": GOOGLE_DRIVE_ADD_PERMISSION_OPERATION,
                                "payload_json": {"file_id": result["id"], **permission},
                                "result_json": receipt,
                            }
                        )
                        for permission, receipt in zip(permissions, completed)
                    )
                )
            permission_id = result.get("id") or p.get("permission_id")
            try:
                current = (
                    service.permissions()
                    .get(
                        fileId=p["file_id"],
                        permissionId=permission_id,
                        fields="id,type,role,emailAddress,domain",
                        supportsAllDrives=True,
                    )
                    .execute()
                )
            except HttpError as exc:
                return exc.resp.status == 404 and op == GOOGLE_DRIVE_REMOVE_PERMISSION_OPERATION
            if op == GOOGLE_DRIVE_REMOVE_PERMISSION_OPERATION:
                return False
            return (
                current.get("type") == p["permission_type"]
                and current.get("role") == p["role"]
                and all(
                    current.get(api_key) == p[key]
                    for key, api_key in (("email_address", "emailAddress"), ("domain", "domain"))
                    if key in p
                )
            )
        except Exception:
            return False

    def execute(self, mutation):
        writing = False
        result = dict(mutation.get("result_json") or {})
        try:
            op = mutation["operation"]
            if op not in GOOGLE_DRIVE_OPERATIONS:
                raise ValueError("unsupported Drive operation")
            p = mutation["payload_json"]
            data = None
            if op == GOOGLE_DRIVE_UPLOAD_FILE_OPERATION:
                data = self.read_upload(p)
                if len(data) != p["size_bytes"] or sha256(data).hexdigest() != p["sha256"]:
                    raise ValueError("staged upload differs from approved bytes")
            service = self.service_factory(mutation["account"])
            if "parent_id" in p:
                parent = (
                    service.files()
                    .get(fileId=p["parent_id"], fields="id,mimeType,trashed", supportsAllDrives=True)
                    .execute()
                )
                if parent.get("trashed") or parent.get("mimeType") != "application/vnd.google-apps.folder":
                    raise ValueError("destination is no longer a folder")
            if op in {GOOGLE_DRIVE_UPLOAD_FILE_OPERATION, GOOGLE_DRIVE_CREATE_FOLDER_OPERATION}:
                body = {
                    "name": p["name"],
                    "parents": [p["parent_id"]],
                    "mimeType": p.get("mime_type", "application/vnd.google-apps.folder"),
                    "appProperties": {"pdw_mutation_id": mutation["id"]},
                }
                args = dict(body=body, fields="id,name,mimeType,webViewLink,parents,size", supportsAllDrives=True)
                if data is not None:
                    args["media_body"] = MediaIoBaseUpload(BytesIO(data), mimetype=p["mime_type"], resumable=False)
                writing = True
                # An interrupted sharing step retains the already-created folder
                # in result_json; a credential repair never creates a second one.
                if not result.get("id"):
                    result = service.files().create(**args).execute(num_retries=0)
                if op == GOOGLE_DRIVE_CREATE_FOLDER_OPERATION:
                    completed = result.setdefault("initial_permissions", [])
                    for permission in p.get("initial_permissions", [])[len(completed) :]:
                        shared = self._add_permission(service, result["id"], permission)
                        completed.append(shared)
            elif op == GOOGLE_DRIVE_ADD_PERMISSION_OPERATION:
                writing = True
                result = self._add_permission(service, p["file_id"], p)
            else:
                args = dict(fileId=p["file_id"], permissionId=p["permission_id"], supportsAllDrives=True)
                try:
                    current = (
                        service.permissions()
                        .get(**args, fields="id,type,role,emailAddress,domain,permissionDetails")
                        .execute()
                    )
                except HttpError as exc:
                    if exc.resp.status == 404 and op == GOOGLE_DRIVE_REMOVE_PERMISSION_OPERATION:
                        return DriveMutationResult("succeeded", {"already_removed": True})
                    raise
                expected = {"type": p["permission_type"], "role": p["expected_role"]}
                for key, api_key in (("email_address", "emailAddress"), ("domain", "domain")):
                    if key in p:
                        expected[api_key] = p[key]
                if any(current.get(k) != v for k, v in expected.items()):
                    raise ValueError("permission changed since review; propose again")
                if any(item.get("inherited") for item in current.get("permissionDetails", [])):
                    raise ValueError("inherited permissions must be changed on their parent")
                writing = True
                if op == GOOGLE_DRIVE_UPDATE_PERMISSION_OPERATION:
                    result = (
                        service.permissions()
                        .update(**args, body={"role": p["role"]}, fields="id,type,role,emailAddress,domain")
                        .execute(num_retries=0)
                    )
                else:
                    service.permissions().delete(**args).execute(num_retries=0)
                    result = {"removed_permission_id": p["permission_id"]}
            # The durable result is recorded by the queue before any cleanup.
            # Expiry also handles rejected/withdrawn proposals and abandoned puts.
            return DriveMutationResult("succeeded", dict(result or {}))
        except Exception as exc:
            if not writing:
                if isinstance(
                    exc, (EndpointConnectionError, ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError)
                ):
                    return DriveMutationResult(
                        "failed_retryable", {}, "upload staging is unreachable; retrying before any Drive write"
                    )
                if isinstance(exc, ClientError):
                    code = exc.response.get("Error", {}).get("Code")
                    if code in {"NoSuchKey", "NotFound", "404"}:
                        return DriveMutationResult("failed_terminal", {}, "staged bytes expired; propose again")
                    if code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}:
                        return DriveMutationResult(
                            "blocked_missing_credentials", {}, "upload staging credentials need repair"
                        )
                    return DriveMutationResult(
                        "failed_retryable", {}, "upload staging failed before any Drive write; retrying"
                    )
            status = calendar_mutation_failure_status(exc)
            if writing and status == "failed_retryable":
                return DriveMutationResult(
                    "failed_terminal", result, "Drive may have applied the change; check Drive before proposing again"
                )
            # Do not include provider errors: they can contain upload bytes or tokens.
            return DriveMutationResult(
                status,
                result,
                str(exc)
                if isinstance(exc, ValueError)
                else "Drive mutation failed; verify credentials, staging configuration and upstream access",
            )

    @staticmethod
    def _add_permission(service, file_id, p):
        body = {"type": p["permission_type"], "role": p["role"]}
        for key, api_key in (("email_address", "emailAddress"), ("domain", "domain")):
            if key in p:
                body[api_key] = p[key]
        return (
            service.permissions()
            .create(
                fileId=file_id,
                body=body,
                sendNotificationEmail=p["send_notification_email"],
                supportsAllDrives=True,
                fields="id,type,role,emailAddress,domain",
            )
            .execute(num_retries=0)
        )
