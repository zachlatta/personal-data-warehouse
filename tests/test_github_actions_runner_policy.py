from pathlib import Path


WORKFLOW = (
    Path(__file__).resolve().parent.parent
    / ".github"
    / "workflows"
    / "postgres-pgbackrest-image.yml"
)


def test_pgbackrest_image_uses_rotom_only_for_main_ref() -> None:
    workflow = WORKFLOW.read_text()

    assert (
        "runs-on: ${{ github.ref == 'refs/heads/main' "
        "&& 'rotom-builder' || 'ubuntu-latest' }}"
    ) in workflow


def test_pull_request_trigger_remains_enabled() -> None:
    workflow = WORKFLOW.read_text()

    assert "\n  pull_request:\n" in workflow
