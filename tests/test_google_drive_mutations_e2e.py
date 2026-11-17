"""Real proposal, preview, approval, queue claim and worker with only Drive substituted."""

from unittest.mock import MagicMock

from personal_data_warehouse.defs.upstream_mutations import (
    process_upstream_mutation_batch,
    observe_drive_mutation_batch,
)
from personal_data_warehouse.google_drive_mutations import GoogleDriveMutationExecutor
from tests.test_gmail_settings_e2e import _isolated_warehouse, _run_go_test


def test_upload_can_be_reproposed_after_terminal_state_or_expiration():
    with _isolated_warehouse() as (warehouse, url):
        warehouse._ensure_table_group(["google_drive_files"])
        warehouse._command("""INSERT INTO @google_drive_files (account,file_id,name,mime_type)
            VALUES ('drive@example.test','parent','Destination','application/vnd.google-apps.folder')""")
        _run_go_test("TestDriveUploadReproposalAfterTerminalOrExpiry", {"PDW_DRIVE_TEST_URL": url})


def test_drive_proposal_review_approval_and_worker():
    with _isolated_warehouse() as (warehouse, url):
        warehouse._ensure_table_group(["google_drive_files"])
        warehouse._command("""INSERT INTO @google_drive_files (account, file_id, name, mime_type)
            VALUES ('drive@example.test', 'parent', 'Destination', 'application/vnd.google-apps.folder')""")
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
