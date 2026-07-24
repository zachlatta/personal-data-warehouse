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

TRUSTED_BUILD_RUNNER = """runs-on: >-
      ${{ github.event_name == 'pull_request' &&
          github.event.pull_request.head.repo.full_name != github.repository &&
          'ubuntu-latest' || 'rotom-builder' }}"""


def test_every_trusted_artifact_build_uses_rotom() -> None:
    assert TRUSTED_BUILD_RUNNER in PG_BACKREST_WORKFLOW.read_text()
    assert TRUSTED_BUILD_RUNNER in CLI_WORKFLOW.read_text()


def test_fork_pull_request_builds_remain_github_hosted() -> None:
    for workflow_path in (PG_BACKREST_WORKFLOW, CLI_WORKFLOW):
        workflow = workflow_path.read_text()

        assert "\n  pull_request:\n" in workflow
        assert "'ubuntu-latest' || 'rotom-builder'" in workflow


def test_cli_build_matrix_queues_only_one_rotom_job_at_a_time() -> None:
    workflow = CLI_WORKFLOW.read_text()

    assert "strategy:\n      fail-fast: false\n      max-parallel: 1\n" in workflow
