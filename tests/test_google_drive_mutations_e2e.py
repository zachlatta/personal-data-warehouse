"""Real proposal, preview, approval, queue claim and worker with only Drive substituted."""

from unittest.mock import MagicMock
from dataclasses import replace

from personal_data_warehouse.config import GoogleDriveSourceConfig
from personal_data_warehouse.google_drive_source_sync import (
    GoogleApiDriveClient,
    GoogleDriveSourceSyncRunner,
    FOLDER_MIME,
)
from tests.test_google_drive_source_sync import _settings

from personal_data_warehouse.defs.upstream_mutations import (
    process_upstream_mutation_batch,
    observe_drive_mutation_batch,
)
from personal_data_warehouse.google_drive_mutations import GoogleDriveMutationExecutor
from tests.test_gmail_settings_e2e import _isolated_warehouse, _run_go_test


def _sync_drive_folders(warehouse):
    """Use the real Drive API adapter and source runner, never seed SQL folder rows."""
    service = MagicMock()
    root = {
        "id": "parent",
        "name": "Destination",
        "mimeType": FOLDER_MIME,
        "parents": [],
        "modifiedTime": "2026-06-01T00:00:00Z",
    }
    shared = {
        "id": "shared-root",
        "name": "Team drive",
        "driveId": "shared-root",
        "mimeType": FOLDER_MIME,
        "parents": [],
        "modifiedTime": "2026-06-01T00:00:00Z",
    }
    folders = [
        {
            "id": "empty-folder",
            "name": "Empty",
            "mimeType": FOLDER_MIME,
            "parents": ["parent"],
            "modifiedTime": "2026-06-01T00:00:00Z",
        }
    ]

    def get(**kwargs):
        request = MagicMock()
        request.execute.return_value = root if kwargs["fileId"] == "root" else shared
        return request

    def listing(**kwargs):
        request = MagicMock()
        request.execute.return_value = {"files": list(folders) if "mimeType =" in kwargs["q"] else []}
        return request

    service.files.return_value.get.side_effect = get
    service.files.return_value.list.side_effect = listing
    service.drives.return_value.list.return_value.execute.return_value = {
        "drives": [{"id": "shared-root", "name": "Team drive"}]
    }
    service.changes.return_value.getStartPageToken.return_value.execute.return_value = {"startPageToken": "first-token"}
    service.changes.return_value.list.return_value.execute.return_value = {
        "changes": [],
        "newStartPageToken": "next-token",
    }
    config = GoogleDriveSourceConfig(accounts=("drive@example.test",), include_shared_drives=True)
    settings = replace(_settings(), google_drive_source=config)
    runner = GoogleDriveSourceSyncRunner(
        settings=settings,
        warehouse=warehouse,
        client_factory=lambda account: GoogleApiDriveClient(service, config=config),
    )
    assert runner.sync_all()[0].status == "ok"
    return runner, folders


def test_upload_can_be_reproposed_after_terminal_state_or_expiration():
    with _isolated_warehouse() as (warehouse, url):
        _sync_drive_folders(warehouse)
        _run_go_test("TestDriveUploadReproposalAfterTerminalOrExpiry", {"PDW_DRIVE_TEST_URL": url})


def test_drive_proposal_review_approval_and_worker():
    with _isolated_warehouse() as (warehouse, url):
        runner, folders = _sync_drive_folders(warehouse)
        _run_go_test("TestDriveProposalReviewApproval", {"PDW_DRIVE_TEST_URL": url})
        drive = MagicMock()
        drive.files.return_value.get.return_value.execute.return_value = {
            "id": "parent",
            "mimeType": "application/vnd.google-apps.folder",
            "trashed": False,
        }
        drive.files.return_value.create.return_value.execute.return_value = {"id": "new-folder", "name": "Project"}
        drive.permissions.return_value.create.return_value.execute.return_value = {"id": "new-permission"}
        drive.permissions.return_value.get.return_value.execute.return_value = {
            "id": "new-permission",
            "type": "user",
            "emailAddress": "person@example.com",
            "role": "reader",
        }
        summary = process_upstream_mutation_batch(
            warehouse=warehouse,
            gmail_executor=None,
            contact_executor=None,
            calendar_executor=None,
            slack_executor=None,
            screen_timelapse_executor=None,
            resideo_executor=None,
            drive_executor=GoogleDriveMutationExecutor(settings=None, service_factory=lambda account: drive),
            limit=10,
            claimed_by="test:drive",
            on_gmail_settings_changed=None,
        )
        assert summary.claimed == summary.succeeded == 1
        drive.files.return_value.create.assert_called_once()
        drive.permissions.return_value.create.assert_called_once()
        assert drive.permissions.return_value.create.call_args.kwargs["fileId"] == "new-folder"
        assert drive.files.return_value.create.call_args.kwargs["body"]["parents"] == ["parent"]
        # A second worker pass never repeats the successful create.
        again = process_upstream_mutation_batch(
            warehouse=warehouse,
            gmail_executor=None,
            contact_executor=None,
            calendar_executor=None,
            slack_executor=None,
            screen_timelapse_executor=None,
            resideo_executor=None,
            drive_executor=GoogleDriveMutationExecutor(settings=None, service_factory=lambda account: drive),
            limit=10,
            claimed_by="test:drive",
            on_gmail_settings_changed=None,
        )
        assert again.claimed == 0
        folders.append(
            {
                "id": "new-folder",
                "name": "Project",
                "mimeType": FOLDER_MIME,
                "parents": ["parent"],
                "modifiedTime": "2026-06-02T00:00:00Z",
            }
        )
        assert runner.sync_all()[0].sync_type == "incremental"
        _run_go_test("TestDriveSyncedFoldersAcceptMutations", {"PDW_DRIVE_TEST_URL": url})
        drive.files.return_value.get.return_value.execute.return_value = {
            "id": "new-folder",
            "name": "Project",
            "parents": ["parent"],
            "mimeType": "application/vnd.google-apps.folder",
            "trashed": False,
        }
        # Source read-back advances the same queue record to observed.
        observed = observe_drive_mutation_batch(
            warehouse=warehouse,
            drive_executor=GoogleDriveMutationExecutor(settings=None, service_factory=lambda account: drive),
        )
        assert observed == 1
