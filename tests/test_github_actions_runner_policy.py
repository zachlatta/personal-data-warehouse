from pathlib import Path


PG_BACKREST_WORKFLOW = (
    Path(__file__).resolve().parent.parent
    / ".github"
    / "workflows"
    / "postgres-pgbackrest-image.yml"
)
CLI_WORKFLOW = (
    Path(__file__).resolve().parent.parent
    / ".github"
    / "workflows"
    / "pdw-cli-release.yml"
)

TRUSTED_IMAGE_BUILD_RUNNER = """runs-on: >-
      ${{ github.event_name == 'pull_request' &&
          github.event.pull_request.head.repo.full_name != github.repository &&
          'ubuntu-latest' || 'rotom-builder' }}"""
TRUSTED_CLI_BUILD_RUNNER = """runs-on: >-
      ${{ github.event_name == 'pull_request' &&
          github.event.pull_request.head.repo.full_name != github.repository &&
          'ubuntu-latest' ||
          format('rotom-builder-pdw-cli-{0}-{1}', matrix.goos, matrix.goarch) }}"""


def test_every_trusted_artifact_build_uses_rotom() -> None:
    assert TRUSTED_IMAGE_BUILD_RUNNER in PG_BACKREST_WORKFLOW.read_text()
    assert TRUSTED_CLI_BUILD_RUNNER in CLI_WORKFLOW.read_text()


def test_fork_pull_request_builds_remain_github_hosted() -> None:
    for workflow_path in (PG_BACKREST_WORKFLOW, CLI_WORKFLOW):
        workflow = workflow_path.read_text()

        assert "\n  pull_request:\n" in workflow
        assert "'ubuntu-latest' ||" in workflow


def test_cli_build_matrix_uses_distinct_rotom_labels() -> None:
    workflow = CLI_WORKFLOW.read_text()

    assert "format('rotom-builder-pdw-cli-{0}-{1}', matrix.goos, matrix.goarch)" in workflow
