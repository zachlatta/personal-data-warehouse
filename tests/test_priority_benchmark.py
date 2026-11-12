"""Fictional labels only; real labels belong in the ignored private file."""

import json
from unittest.mock import Mock

import pytest

from personal_data_warehouse import priority_benchmark as benchmark
from scripts import priority_benchmark as script
from scripts.priority_benchmark import proposed_sql, replace_required
from tests.test_timeline import (
    warehouse,
    _ensure_all_source_tables,
    _seed_sources,
    _engine,
    EXPECTED_SEEDED_PRIORITIES,
)


def payload(rows):
    requirements = {}
    for row in rows:
        strata = requirements.setdefault(row["adapter"], {})
        strata[row["stratum"]] = strata.get(row["stratum"], 0) + 1
    return dict(requirements=requirements, labels=rows)


def test_private_label_paths_and_incomplete_labels_fail(tmp_path):
    with pytest.raises(ValueError, match="priority-eval"):
        benchmark.assert_private_path(tmp_path / "public.json")
    with pytest.raises(ValueError, match="unmeasured"):
        benchmark.validate_payload(dict(requirements={}, labels=[]))
    data = payload(
        [dict(adapter="example", event_id="one", expected="self", stratum="all")]
    )
    data["requirements"]["example"]["all"] = 2
    with pytest.raises(ValueError, match="Missing labels"):
        benchmark.validate_payload(data)
    with pytest.raises(ValueError, match="SQL shape changed"):
        replace_required("SELECT 1", "nonexistent target", "replacement")
    unknown = payload(
        [
            dict(
                adapter="unsupported",
                event_id="fictional",
                expected="self",
                stratum="all",
            )
        ]
    )
    with pytest.raises(ValueError, match="No proposed adapter SQL"):
        proposed_sql(unknown)


def test_pipeline_health_owns_priority_schema_without_timeline_tables(warehouse):
    assert not warehouse._relation_exists("timeline_events")
    warehouse.ensure_pipeline_health_tables()
    assert warehouse._relation_exists("priority_benchmark_labels")
    assert warehouse._relation_exists("priority_benchmark_results")
    rows = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")
    assert len(rows) == 1 and rows[0]["adapter"] == "unpublished"
    assert rows[0]["status"] == "failing"
    assert not warehouse._relation_exists("timeline_events")


@pytest.mark.parametrize("command", ["publish-labels", "measure"])
def test_cli_uses_only_benchmark_schema_setup(monkeypatch, command):
    warehouse = Mock()
    warehouse._query_dicts.return_value = []
    monkeypatch.setattr("personal_data_warehouse.config.load_settings", Mock())
    monkeypatch.setattr(
        "personal_data_warehouse.warehouse.warehouse_from_settings",
        Mock(return_value=warehouse),
    )
    monkeypatch.setattr(script.benchmark, "load_labels", Mock())
    monkeypatch.setattr(script.benchmark, "publish_labels", Mock(return_value=1))
    monkeypatch.setattr(script.benchmark, "measure", Mock())
    monkeypatch.setattr("sys.argv", ["priority_benchmark.py", command])
    script.main()
    warehouse.ensure_priority_benchmark_tables.assert_called_once()
    warehouse.ensure_timeline_tables.assert_not_called()
    warehouse.close.assert_called_once()


def test_private_label_roundtrip_and_missing_measurements_fail_loudly(warehouse):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_priority_benchmark_tables()

    def verdict():
        return warehouse._query_dicts(
            "SELECT * FROM @marts_timeline_priority_correctness"
        )[0]

    assert verdict()["status"] == "failing" and verdict()["adapter"] == "unpublished"
    data = payload(
        [
            dict(adapter="example", event_id="one", expected="self", stratum="all"),
            dict(adapter="example", event_id="two", expected="cc", stratum="all"),
        ]
    )
    assert benchmark.publish_labels(warehouse, data) == 2
    assert benchmark.pull_labels(warehouse) == data
    assert verdict()["status"] == "failing"  # published but never measured
    for event_id, tier in (("one", "self"), ("two", "noise")):
        warehouse._command(
            "INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at) "
            "VALUES ('example',%s,'example','example',%s,now())",
            (event_id, tier),
        )
    benchmark.measure(warehouse)
    assert verdict()["accuracy"] == 0.5 and verdict()["status"] == "failing"
    warehouse._command("UPDATE @timeline_events SET priority='cc' WHERE event_id='two'")
    benchmark.measure(warehouse)
    assert verdict()["status"] == "ok"
    warehouse._command("DELETE FROM @timeline_events WHERE event_id='one'")
    benchmark.measure(warehouse)
    assert verdict()["missing"] == 1 and verdict()["status"] == "failing"
    # Labels changed after a passing measurement cannot reuse its old verdict.
    benchmark.publish_labels(warehouse, data)
    assert verdict()["status"] == "failing" and verdict()["missing"] == 2
    warehouse._command(
        "UPDATE @priority_benchmark_labels SET payload_json=%s",
        (json.dumps(dict(requirements=data["requirements"], labels=[])),),
    )
    assert verdict()["status"] == "failing" and verdict()["missing"] == 2


def test_proposed_adapter_sql_executes_on_test_warehouse(warehouse):
    _ensure_all_source_tables(warehouse)
    _seed_sources(warehouse)
    engine = _engine(warehouse)
    try:
        engine.run()
    finally:
        engine.close()
    rows = warehouse._query_dicts(
        "SELECT adapter,event_id FROM @timeline_events "
        "WHERE adapter IN ('agent_session','agent_session_turn','slack_message','gmail_email') "
        "AND (adapter<>'agent_session_turn' OR actor='user') ORDER BY adapter,event_id"
    )
    selected = {}
    for row in rows:
        selected.setdefault(
            row["adapter"],
            dict(
                **row,
                expected=EXPECTED_SEEDED_PRIORITIES[row["adapter"]],
                stratum="all",
            ),
        )
    assert len(selected) == 4
    data = payload(list(selected.values()))
    result = warehouse._query_dicts(
        proposed_sql(data, namespace=warehouse.schema_namespace)
    )
    assert len(result) == 4
    assert all(r["correct"] == r["expected_count"] == 1 for r in result)
    # A hard-coded background classifier cannot clear the same gold coverage.
    background = (
        "(SELECT adapter,event_id,'background'::text priority FROM @timeline_events)"
    )
    baseline = warehouse._query_dicts(
        benchmark.benchmark_sql(data, actual_relation=background)
    )
    assert all(r["correct"] == 0 for r in baseline)


def test_missing_stratum_measurement_cannot_pass_at_ninety_percent(warehouse):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_priority_benchmark_tables()
    data = payload(
        [
            dict(
                adapter="example",
                event_id=f"event-{i}",
                expected="self",
                stratum="all" if i < 9 else "self",
            )
            for i in range(10)
        ]
    )
    warehouse._command(
        "INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at) "
        "SELECT 'example','event-'||i,'example','example','self',now() FROM generate_series(0,9) i"
    )
    benchmark.publish_labels(warehouse, data)
    scores = benchmark.measure(warehouse)
    assert (
        warehouse._query_dicts(
            "SELECT status FROM @marts_timeline_priority_correctness"
        )[0]["status"]
        == "ok"
    )
    warehouse._command(
        "UPDATE @priority_benchmark_results SET results_json=%s",
        (json.dumps([r for r in scores if r["stratum"] == "all"]),),
    )
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert float(row["accuracy"]) == pytest.approx(0.9) and row["status"] == "failing"


def test_gold_self_floor_rejects_always_background_despite_high_overall_accuracy(
    warehouse,
):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_priority_benchmark_tables()
    data = payload(
        [
            dict(
                adapter="example",
                event_id=f"event-{i}",
                expected="self" if i == 10 else "background",
                stratum="all",
            )
            for i in range(11)
        ]
    )
    warehouse._command(
        "INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at) "
        "SELECT 'example','event-'||i,'example','example','background',now() FROM generate_series(0,10) i"
    )
    benchmark.publish_labels(warehouse, data)
    benchmark.measure(warehouse)
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert (
        row["accuracy"] > 0.9
        and row["gold_self_recall"] == 0
        and row["status"] == "failing"
    )
    warehouse._command(
        "UPDATE @timeline_events SET priority='self' WHERE event_id='event-10'"
    )
    benchmark.measure(warehouse)
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert row["gold_self_labeled"] == row["gold_self_correct"] == 1
    assert row["gold_self_recall"] == 1 and row["status"] == "ok"
    warehouse._command(
        "UPDATE @priority_benchmark_results SET collected_at=now()-interval '8 days'"
    )
    assert (
        warehouse._query_dicts(
            "SELECT status FROM @marts_timeline_priority_correctness"
        )[0]["status"]
        == "failing"
    )


def test_no_gold_self_labels_need_no_self_recall_but_legacy_measurements_fail(
    warehouse,
):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_priority_benchmark_tables()
    data = payload(
        [
            dict(
                adapter="example",
                event_id="background-event",
                expected="background",
                stratum="all",
            )
        ]
    )
    warehouse._command(
        "INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at) "
        "VALUES ('example','background-event','example','example','background',now())"
    )
    benchmark.publish_labels(warehouse, data)
    scores = benchmark.measure(warehouse)
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert row["gold_self_recall"] is None and row["status"] == "ok"
    for score in scores:
        del score["self_labeled"]
        del score["self_correct"]
    warehouse._command(
        "UPDATE @priority_benchmark_results SET results_json=%s", (json.dumps(scores),)
    )
    assert (
        warehouse._query_dicts(
            "SELECT status FROM @marts_timeline_priority_correctness"
        )[0]["status"]
        == "failing"
    )


def test_self_precision_sample_cannot_hide_behind_other_strata(warehouse):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_priority_benchmark_tables()
    data = payload(
        [
            dict(
                adapter="example",
                event_id=f"event-{i}",
                expected="background" if i == 1 else "self",
                stratum="blind_self_precision" if i < 2 else "all",
            )
            for i in range(12)
        ]
    )
    warehouse._command(
        "INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at) "
        "SELECT 'example','event-'||i,'example','example','self',now() FROM generate_series(0,11) i"
    )
    benchmark.publish_labels(warehouse, data)
    benchmark.measure(warehouse)
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert row["accuracy"] > 0.9 and row["gold_self_recall"] == 1
    assert row["self_precision"] == 0.5 and row["status"] == "failing"
    assert row["precision_sample_count"] == 2
    warehouse._command(
        "UPDATE @timeline_events SET priority='background' WHERE event_id='event-1'"
    )
    scores = benchmark.measure(warehouse)
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert row["self_precision"] == 1 and row["status"] == "ok"
    # Old result JSON cannot silently omit the new precision evidence.
    for score in scores:
        del score["predicted_self"]
        del score["true_self"]
    warehouse._command(
        "UPDATE @priority_benchmark_results SET results_json=%s", (json.dumps(scores),)
    )
    assert (
        warehouse._query_dicts(
            "SELECT status FROM @marts_timeline_priority_correctness"
        )[0]["status"]
        == "failing"
    )


def test_self_precision_sample_with_no_predictions_fails(warehouse):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_priority_benchmark_tables()
    data = payload(
        [
            dict(
                adapter="example",
                event_id="one",
                expected="background",
                stratum="blind_self_precision",
            )
        ]
    )
    warehouse._command(
        "INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at) VALUES ('example','one','example','example','background',now())"
    )
    benchmark.publish_labels(warehouse, data)
    benchmark.measure(warehouse)
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert (
        row["accuracy"] == 1
        and row["self_precision"] is None
        and row["status"] == "failing"
    )


def test_each_independent_precision_sample_must_meet_its_floor(warehouse):
    warehouse.ensure_timeline_tables()
    warehouse.ensure_priority_benchmark_tables()
    data = payload(
        [
            dict(
                adapter="example",
                event_id=f"event-{i}",
                expected="background" if i == 11 else "self",
                stratum=(
                    "blind_self_precision"
                    if i < 10
                    else "blind_self_precision_delivery"
                ),
            )
            for i in range(12)
        ]
    )
    warehouse._command(
        "INSERT INTO @timeline_events (adapter,event_id,source,kind,priority,occurred_at) "
        "SELECT 'example','event-'||i,'example','example','self',now() FROM generate_series(0,11) i"
    )
    benchmark.publish_labels(warehouse, data)
    benchmark.measure(warehouse)
    row = warehouse._query_dicts("SELECT * FROM @marts_timeline_priority_correctness")[
        0
    ]
    assert row["accuracy"] > 0.9 and row["self_precision"] > 0.9
    assert row["gold_self_recall"] == 1 and row["precision_sample_count"] == 12
    assert row["status"] == "failing"  # Second independent cohort scores only 1/2.
