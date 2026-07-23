from pathlib import Path
import plistlib


REPO_ROOT = Path(__file__).resolve().parents[1]
LABEL = "com.zachlatta.personal-data-warehouse.apple-contacts-upload"


def test_apple_contacts_launchd_runs_direct_python_uploader() -> None:
    wrapper = (REPO_ROOT / "bin" / "apple-contacts-upload-launchd").read_text()

    assert "uv" in wrapper
    assert "python -m personal_data_warehouse_apple_contacts.cli --mode incremental" in wrapper
    assert "pdw ingest" not in wrapper
    assert "apple-contacts-upload.heartbeat" in wrapper


def test_apple_contacts_launch_agent_runs_every_five_minutes() -> None:
    plist_path = (
        REPO_ROOT
        / "ops"
        / "launchd"
        / "com.zachlatta.personal-data-warehouse.apple-contacts-upload.plist"
    )
    with plist_path.open("rb") as file:
        plist = plistlib.load(file)

    assert plist["Label"] == LABEL
    assert plist["StartInterval"] == 300
    assert plist["RunAtLoad"] is True
    assert plist["ProgramArguments"] == [
        "/Users/zrl/dev/zachlatta/personal-data-warehouse/bin/apple-contacts-upload-launchd"
    ]
