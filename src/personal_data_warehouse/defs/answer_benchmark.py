from __future__ import annotations

import os

from dagster import (
    DefaultScheduleStatus, Definitions, MaterializeResult, asset,
    define_asset_job, definitions, schedule,
)

from personal_data_warehouse.agent_resource import AgentResource
from personal_data_warehouse.answer_benchmark import (
    DEFAULT_QUESTION_TIMEOUT_SECONDS, ManagedAnswerAgent, run_recorded_benchmark,
)
from personal_data_warehouse.config import load_settings
from personal_data_warehouse.schedule_guards import skip_if_job_active
from personal_data_warehouse.sync_locks import exclusive_sync_lock
from personal_data_warehouse.warehouse import warehouse_from_settings

ANSWER_BENCHMARK_POSTGRES_LOCK_ID = 8_407_112_990
ANSWER_BENCHMARK_CRON = "17 6 * * 2"


def benchmark_model() -> tuple[str, str]:
    return (
        os.getenv("ANSWER_BENCHMARK_AGENT_PROVIDER", "").strip() or "codex",
        os.getenv("ANSWER_BENCHMARK_AGENT_MODEL", "").strip() or "gpt-6.1-sol",
    )


def benchmark_agent_resource(settings) -> AgentResource:
    if settings.agent is None:
        raise RuntimeError("Agent runner is not configured")
    resource = AgentResource.from_config(settings.agent)
    # Keep deployment isolation/auth settings, but use the benchmark's own model
    # and the CLI's ten-minute per-question budget.
    provider, model = benchmark_model()
    return resource.model_copy(update={
        "provider": provider, "model": model,
        "timeout_seconds": DEFAULT_QUESTION_TIMEOUT_SECONDS,
    })


@asset(group_name="search")
def answer_benchmark(context) -> MaterializeResult:
    """Grade real questions with the managed agent and persist C16's results."""
    settings = load_settings(require_gmail=False)
    warehouse = warehouse_from_settings(settings)
    try:
        with exclusive_sync_lock(
            name="answer_benchmark", postgres_lock_id=ANSWER_BENCHMARK_POSTGRES_LOCK_ID,
        ) as acquired:
            if not acquired:
                return MaterializeResult(metadata={"skipped": "another answer benchmark is active"})
            provider, model = benchmark_model()
            resource = benchmark_agent_resource(settings)
            summary, _ = run_recorded_benchmark(
                warehouse=warehouse,
                agent=ManagedAnswerAgent(runner=resource.runner(), provider=provider, model=model),
                provider=provider, model=model, logger_=context.log,
            )
            return MaterializeResult(metadata={
                "questions": summary.questions, "correct": summary.correct,
                "elapsed_p50_ms": summary.elapsed_p50_ms,
                "pdw_calls_p50": summary.pdw_calls_p50, "errors": summary.errors,
                "provider": provider, "model": model,
            })
    finally:
        warehouse.close()


answer_benchmark_job = define_asset_job(
    "answer_benchmark_job", selection=[answer_benchmark],
    tags={"dagster/max_runtime": "21600"},
)


@schedule(
    cron_schedule=ANSWER_BENCHMARK_CRON, execution_timezone="UTC",
    job=answer_benchmark_job, default_status=DefaultScheduleStatus.RUNNING,
)
def answer_benchmark_weekly(context):
    return skip_if_job_active(context, job_name="answer_benchmark_job")


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[answer_benchmark], jobs=[answer_benchmark_job],
        schedules=[answer_benchmark_weekly],
    )
