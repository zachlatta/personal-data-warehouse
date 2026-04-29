from __future__ import annotations

import os
from dataclasses import dataclass, field

from dagster import DagsterInstance, DagsterRunStatus, RunsFilter

BOOT_CLEAR_STATUSES = (
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
)


@dataclass(frozen=True)
class BootRunCleanupSummary:
    failed_run_ids: list[str] = field(default_factory=list)
    canceled_run_ids: list[str] = field(default_factory=list)
    skipped_run_ids: list[str] = field(default_factory=list)

    @property
    def cleared_count(self) -> int:
        return len(self.failed_run_ids) + len(self.canceled_run_ids)


def clear_in_progress_runs_at_boot(instance: DagsterInstance) -> BootRunCleanupSummary:
    run_records = instance.get_run_records(
        filters=RunsFilter(statuses=BOOT_CLEAR_STATUSES),
        ascending=True,
    )
    failed_run_ids: list[str] = []
    canceled_run_ids: list[str] = []
    skipped_run_ids: list[str] = []

    for record in run_records:
        run = instance.get_run_by_id(record.dagster_run.run_id)
        if run is None or run.status not in BOOT_CLEAR_STATUSES:
            skipped_run_ids.append(record.dagster_run.run_id)
            continue

        if run.status == DagsterRunStatus.CANCELING:
            instance.report_run_canceled(
                run,
                message=(
                    "Marked canceled during Dagster boot cleanup because this run was still "
                    "canceling before the container started."
                ),
            )
            canceled_run_ids.append(run.run_id)
        else:
            instance.report_run_failed(
                run,
                message=(
                    "Marked failed during Dagster boot cleanup because this run was still "
                    "in progress before the container started."
                ),
            )
            failed_run_ids.append(run.run_id)

    return BootRunCleanupSummary(
        failed_run_ids=failed_run_ids,
        canceled_run_ids=canceled_run_ids,
        skipped_run_ids=skipped_run_ids,
    )


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def main() -> None:
    if not _env_bool("DAGSTER_BOOT_CLEAR_IN_PROGRESS_RUNS", True):
        print("Dagster boot cleanup is disabled by DAGSTER_BOOT_CLEAR_IN_PROGRESS_RUNS")
        return

    summary = clear_in_progress_runs_at_boot(DagsterInstance.get())
    print(
        "Dagster boot cleanup cleared "
        f"{summary.cleared_count} in-progress runs "
        f"({len(summary.failed_run_ids)} failed, {len(summary.canceled_run_ids)} canceled)."
    )


if __name__ == "__main__":
    main()
