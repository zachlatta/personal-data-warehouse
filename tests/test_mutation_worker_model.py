from __future__ import annotations

from dataclasses import replace

from personal_data_warehouse.defs.upstream_mutations import (
    CLOUD_MUTATION_PROVIDERS,
    NON_RECLAIMABLE_SEND_OPERATIONS,
)
from tests.formal import mutation_worker_model as model


def test_the_current_worker_design_satisfies_every_invariant() -> None:
    assert model.check(model.current_design()) == []


def test_the_model_replays_the_resident_worker_claiming_slack_rows_it_could_not_run() -> None:
    # de20a4b, 2026-10-03: the resident worker claimed every provider not
    # local-only, but was built without the Slack executor.
    resident = model.WorkerDesign(claims=None, executes=frozenset(CLOUD_MUTATION_PROVIDERS) - {"slack"})
    dagster = model.WorkerDesign(claims=None, executes=frozenset(CLOUD_MUTATION_PROVIDERS))
    design = replace(model.current_design(), workers=(resident, dagster))

    violations = model.check(design)

    assert {violation.invariant for violation in violations} == {"claim_needs_executor"}
    assert {violation.row[0] for violation in violations} == {"slack", "future_provider"}
    slack = next(violation for violation in violations if violation.row[0] == "slack")
    assert slack.trace[-1] == "resident claims the row"


def test_the_model_finds_a_duplicate_send_when_a_send_is_made_reclaimable() -> None:
    send = ("slack", "slack.send_message")
    assert send in NON_RECLAIMABLE_SEND_OPERATIONS
    design = model.current_design()
    design = replace(design, reclaimable=design.reclaimable | {send})

    violations = model.check(design)

    assert [(violation.invariant, violation.row) for violation in violations] == [("send_runs_once", send)]
    assert "loses its lock mid-execution" in " ".join(violations[0].trace)


def test_every_cloud_row_can_succeed_so_the_invariants_are_not_vacuous() -> None:
    design = model.current_design()
    for row in model.rows_to_check():
        statuses = model.reachable_statuses(design, row)
        if row[0] in CLOUD_MUTATION_PROVIDERS:
            assert "succeeded" in statuses, row
        else:
            assert statuses == {"approved"}, row
