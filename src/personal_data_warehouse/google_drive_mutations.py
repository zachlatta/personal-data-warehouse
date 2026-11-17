"""Google Drive writes through the existing human-approved mutation queue.

Creates and sharing notifications are never automatically replayed after an
uncertain response. Stale executing claims are deliberately not reclaimable.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
import os

import boto3
from botocore.config import Config
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from personal_data_warehouse.calendar_mutations import calendar_mutation_failure_status
from personal_data_warehouse.objectstore.google_drive import build_google_drive_service


@dataclass(frozen=True)
class DriveMutationResult:
    status: str
    result_json: dict
    error: str = ""


def staging_client():
    names = ("ENDPOINT_URL", "BUCKET", "ACCESS_KEY", "SECRET_KEY")
    if any(not os.getenv("PDW_MUTATION_S3_" + name) for name in names):
        raise RuntimeError("PDW_MUTATION_S3 configuration is missing")
    return boto3.client(
        "s3",
        endpoint_url=os.environ["PDW_MUTATION_S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["PDW_MUTATION_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["PDW_MUTATION_S3_SECRET_KEY"],
        region_name=os.getenv("PDW_MUTATION_S3_REGION") or "us-east-1",
        config=Config(signature_version="s3v4", connect_timeout=15, read_timeout=60, s3={"addressing_style": "path"}),
    )


def read_staged_upload(payload):
    key = payload["staging_key"]
    if not key.startswith("pending/sha256/" + payload["sha256"] + "/"):
        raise ValueError("invalid staging key")
    response = staging_client().get_object(Bucket=os.environ["PDW_MUTATION_S3_BUCKET"], Key=key)
    with response["Body"] as body:
        return body.read((20 << 20) + 1)


class GoogleDriveMutationExecutor:
    def __init__(self, *, settings, service_factory=None, read_upload=read_staged_upload):
        self.settings = settings
        self.service_factory = service_factory or (
            lambda account: build_google_drive_service(account=account, settings=settings)
        )
        self.read_upload = read_upload

    def observed(self, mutation):
        """Read upstream only; a successful write is observed independently."""
        try:
            service = self.service_factory(mutation["account"])
            p, result = mutation["payload_json"], mutation["result_json"]
            op = mutation["operation"]
            if op in {"google_drive.upload_file", "google_drive.create_folder"}:
                current = (
                    service.files()
                    .get(fileId=result["id"], fields="id,name,mimeType,parents,size,trashed", supportsAllDrives=True)
                    .execute()
                )
                return (
                    not current.get("trashed")
                    and current.get("name") == p["name"]
                    and p["parent_id"] in current.get("parents", [])
                    and current.get("mimeType") == p.get("mime_type", "application/vnd.google-apps.folder")
                    and (op != "google_drive.upload_file" or int(current.get("size", -1)) == p["size_bytes"])
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
                return exc.resp.status == 404 and op == "google_drive.remove_permission"
            if op == "google_drive.remove_permission":
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
        try:
            op = mutation["operation"]
            if op not in {
                "google_drive." + suffix
                for suffix in (
                    "upload_file",
                    "create_folder",
                    "add_permission",
                    "update_permission",
                    "remove_permission",
                )
            }:
                raise ValueError("unsupported Drive operation")
            p = mutation["payload_json"]
            data = None
            if op == "google_drive.upload_file":
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
            if op in {"google_drive.upload_file", "google_drive.create_folder"}:
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
                result = service.files().create(**args).execute(num_retries=0)
            elif op == "google_drive.add_permission":
                body = {"type": p["permission_type"], "role": p["role"]}
                for key, api_key in (("email_address", "emailAddress"), ("domain", "domain")):
                    if key in p:
                        body[api_key] = p[key]
                writing = True
                result = (
                    service.permissions()
                    .create(
                        fileId=p["file_id"],
                        body=body,
                        sendNotificationEmail=p["send_notification_email"],
                        supportsAllDrives=True,
                        fields="id,type,role,emailAddress,domain",
                    )
                    .execute(num_retries=0)
                )
            else:
                args = dict(fileId=p["file_id"], permissionId=p["permission_id"], supportsAllDrives=True)
                try:
                    current = (
                        service.permissions()
                        .get(**args, fields="id,type,role,emailAddress,domain,permissionDetails")
                        .execute()
                    )
                except HttpError as exc:
                    if exc.resp.status == 404 and op == "google_drive.remove_permission":
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
                if op == "google_drive.update_permission":
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
            status = calendar_mutation_failure_status(exc)
            if writing and status == "failed_retryable":
                return DriveMutationResult(
                    "failed_terminal", {}, "Drive may have applied the change; check Drive before proposing again"
                )
            # Do not include provider errors: they can contain upload bytes or tokens.
            return DriveMutationResult(
                status,
                {},
                str(exc)
                if isinstance(exc, ValueError)
                else "Drive mutation failed; verify credentials, staging configuration and upstream access",
            )
