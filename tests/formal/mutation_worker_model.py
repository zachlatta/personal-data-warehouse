"""Exhaustive state-space model of the cloud mutation workers.

Two workers (the resident one and the Dagster fallback) share one advisory
lock and one ``upstream_mutations`` row. Each step a worker can take mirrors
``process_upstream_mutation_batch``: reclaim a stale ``executing`` row, claim
a claimable row, write upstream, record the result, release the lock. A
worker can lose its lock at any point (its connection drops, or it hangs past
``UPSTREAM_MUTATION_RECLAIM_AFTER_SECONDS``) and still finish what it started.

Breadth-first search visits every reachable state and checks two invariants:

* ``claim_needs_executor`` — a worker claims only a row it can execute. On
  2026-10-03 (de20a4b) the resident worker claimed approved Slack rows it had
  no executor for, deferred them, and claimed them again: 68 times in 12 s.
* ``send_runs_once`` — an operation in NON_RECLAIMABLE_SEND_OPERATIONS writes
  upstream at most once, even when a stale claim is reclaimed while its
  worker is still mid-write.

The design under test is read from the real code: the providers each worker
claims and holds executors for (CloudMutationExecutors) and the operations it
reclaims (RECLAIMABLE_IDEMPOTENT_OPERATIONS). ``python -m
tests.formal.mutation_worker_model`` exits non-zero on a violation.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
import sys

from personal_data_warehouse.defs.upstream_mutations import (
    CLOUD_MUTATION_PROVIDERS,
    LOCAL_ONLY_MUTATION_PROVIDERS,
    NON_RECLAIMABLE_SEND_OPERATIONS,
    RECLAIMABLE_IDEMPOTENT_OPERATIONS,
)
from personal_data_warehouse.postgres import UPSTREAM_MUTATION_CLAIMABLE_STATUSES

WORKERS = ("resident", "dagster")
MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class WorkerDesign:
    claims: frozenset[str] | None
    """Providers the claim is scoped to; None means every provider not local-only."""
    executes: frozenset[str]


@dataclass(frozen=True)
class Design:
    workers: tuple[WorkerDesign, WorkerDesign]
    reclaimable: frozenset[tuple[str, str]]


@dataclass(frozen=True)
class Worker:
    phase: str = "idle"
    """idle | locked | claimed | wrote | deferred."""
    has_lock: bool = False


@dataclass(frozen=True)
class State:
    status: str
    claimed_by: int
    attempts: int
    writes: int
    workers: tuple[Worker, Worker]
    lock: int


@dataclass(frozen=True)
class Violation:
    invariant: str
    row: tuple[str, str]
    trace: tuple[str, ...]

    def __str__(self) -> str:
        steps = "\n  ".join(self.trace)
        return f"{self.invariant} violated for {self.row[0]}/{self.row[1]}:\n  {steps}"


def current_design() -> Design:
    cloud = frozenset(CLOUD_MUTATION_PROVIDERS)
    worker = WorkerDesign(claims=cloud, executes=cloud)
    return Design(workers=(worker, worker), reclaimable=frozenset(RECLAIMABLE_IDEMPOTENT_OPERATIONS))


def rows_to_check() -> list[tuple[str, str]]:
    rows = set(RECLAIMABLE_IDEMPOTENT_OPERATIONS) | set(NON_RECLAIMABLE_SEND_OPERATIONS)
    rows |= {(provider, f"{provider}.any") for provider in CLOUD_MUTATION_PROVIDERS}
    rows |= {(provider, f"{provider}.any") for provider in LOCAL_ONLY_MUTATION_PROVIDERS}
    rows.add(("future_provider", "future_provider.any"))
    return sorted(rows)


def _claims(worker: WorkerDesign, provider: str) -> bool:
    if worker.claims is None:
        return provider not in LOCAL_ONLY_MUTATION_PROVIDERS
    return provider in worker.claims


def _set_worker(state: State, index: int, worker: Worker) -> tuple[Worker, Worker]:
    workers = list(state.workers)
    workers[index] = worker
    return (workers[0], workers[1])


def _successors(design: Design, row: tuple[str, str], state: State):
    provider, _ = row
    for index, name in enumerate(WORKERS):
        me = state.workers[index]
        spec = design.workers[index]
        if me.phase == "idle" and state.lock < 0:
            yield f"{name} takes the lock", replace(
                state, lock=index, workers=_set_worker(state, index, Worker("locked", True))
            )
        if me.phase == "locked":
            if state.status == "executing" and row in design.reclaimable and state.workers[state.claimed_by].has_lock is False:
                yield f"{name} reclaims the stale claim of {WORKERS[state.claimed_by]}", replace(
                    state, status="approved", claimed_by=-1
                )
            if (
                state.status in UPSTREAM_MUTATION_CLAIMABLE_STATUSES
                and _claims(spec, provider)
                and state.attempts < MAX_ATTEMPTS
            ):
                yield f"{name} claims the row", replace(
                    state,
                    status="executing",
                    claimed_by=index,
                    attempts=state.attempts + 1,
                    workers=_set_worker(state, index, replace(me, phase="claimed")),
                )
            yield f"{name} releases the lock", replace(
                state, lock=-1, workers=_set_worker(state, index, Worker())
            )
        if me.phase == "claimed":
            if provider in spec.executes:
                yield f"{name} writes upstream", replace(
                    state, writes=state.writes + 1, workers=_set_worker(state, index, replace(me, phase="wrote"))
                )
                yield f"{name} fails before writing (retryable)", replace(
                    state, status="failed_retryable", workers=_set_worker(state, index, replace(me, phase="locked"))
                )
            else:
                yield f"{name} defers a provider it cannot execute", replace(
                    state, status="failed_retryable", workers=_set_worker(state, index, replace(me, phase="locked"))
                )
        if me.phase == "wrote":
            yield f"{name} records success", replace(
                state, status="succeeded", workers=_set_worker(state, index, replace(me, phase="locked"))
            )
        if me.has_lock and me.phase in {"claimed", "wrote"}:
            yield f"{name} loses its lock mid-execution", replace(
                state, lock=-1, workers=_set_worker(state, index, replace(me, has_lock=False))
            )
        if not me.has_lock and me.phase == "locked":
            yield f"{name} finishes without its lock", replace(state, workers=_set_worker(state, index, Worker()))


def _violation(design: Design, row: tuple[str, str], state: State, step: str) -> str | None:
    provider, _ = row
    if step.endswith("claims the row"):
        claimer = design.workers[state.claimed_by]
        if provider not in claimer.executes:
            return "claim_needs_executor"
    if row in NON_RECLAIMABLE_SEND_OPERATIONS and state.writes > 1:
        return "send_runs_once"
    return None


def check_row(design: Design, row: tuple[str, str]) -> Violation | None:
    initial = State(status="approved", claimed_by=-1, attempts=0, writes=0, workers=(Worker(), Worker()), lock=-1)
    parents: dict[State, tuple[State | None, str]] = {initial: (None, "row approved")}
    queue = deque([initial])
    while queue:
        state = queue.popleft()
        for step, nxt in _successors(design, row, state):
            if nxt in parents:
                continue
            parents[nxt] = (state, step)
            invariant = _violation(design, row, nxt, step)
            if invariant:
                trace = []
                cursor: State | None = nxt
                while cursor is not None:
                    previous, label = parents[cursor]
                    trace.append(label)
                    cursor = previous
                return Violation(invariant, row, tuple(reversed(trace)))
            queue.append(nxt)
    return None


def reachable_statuses(design: Design, row: tuple[str, str]) -> set[str]:
    initial = State(status="approved", claimed_by=-1, attempts=0, writes=0, workers=(Worker(), Worker()), lock=-1)
    seen = {initial}
    queue = deque([initial])
    while queue:
        for _, nxt in _successors(design, row, queue.popleft()):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return {state.status for state in seen}


def check(design: Design) -> list[Violation]:
    return [violation for row in rows_to_check() if (violation := check_row(design, row))]


def main() -> int:
    violations = check(current_design())
    for violation in violations:
        print(violation)
    print(f"checked {len(rows_to_check())} rows x {len(WORKERS)} workers: {len(violations)} violation(s)")
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
