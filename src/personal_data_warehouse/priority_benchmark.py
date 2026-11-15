"""Private priority labels, measured results, and the public aggregate view."""

from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

from personal_data_warehouse.schema import (
    PRIORITY_BENCHMARK_LABEL_COLUMNS,
    PRIORITY_BENCHMARK_RESULT_COLUMNS,
)

DEFAULT_LABELS = Path(".priority-eval/ground_truth.json")
PASS_THRESHOLD = 0.9
SELF_RECALL_THRESHOLD = 0.9
SELF_PRECISION_THRESHOLD = 0.9
SELF_PRECISION_STRATUM = "blind_self_precision"


class LabelsUnpublishedError(ValueError):
    """No labels have been published; leave the mart failing without a job error."""


def assert_private_path(path):
    path = Path(path)
    if ".priority-eval" not in path.resolve().parts:
        raise ValueError("Priority labels/reports must live in .priority-eval/")
    return path


def validate_payload(payload):
    requirements, labels = payload["requirements"], payload["labels"]
    if not requirements or not labels:
        raise ValueError("No priority labels/requirements: benchmark is unmeasured")
    counts = Counter()
    seen = set()
    for row in labels:
        key = (row["adapter"], row["stratum"], row["event_id"])
        if key in seen or row["expected"] not in {
            "self",
            "direct",
            "cc",
            "noise",
            "background",
        }:
            raise ValueError("Duplicate label or invalid expected priority")
        seen.add(key)
        counts[key[:2]] += 1
    required = {
        (a, s): n for a, strata in requirements.items() for s, n in strata.items()
    }
    if (
        not required
        or set(counts) != set(required)
        or any(n < 1 or counts[k] != n for k, n in required.items())
    ):
        raise ValueError("Missing labels or undeclared strata")
    return payload


def load_labels(path=DEFAULT_LABELS):
    return validate_payload(json.loads(assert_private_path(path).read_text()))


def label_digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def benchmark_sql(
    payload, *, actual_relation="@timeline_events", ctes=(), output="summary"
):
    """Compose SQL explicitly: details and summaries share the same measured CTE."""
    validate_payload(payload)
    requirements = [
        (a, s, n)
        for a, strata in payload["requirements"].items()
        for s, n in strata.items()
    ]
    values = ",".join(
        "("
        + ",".join(
            map(quote, (r["adapter"], r["event_id"], r["expected"], r["stratum"]))
        )
        + ")"
        for r in payload["labels"]
    )
    required = ",".join(f"({quote(a)},{quote(s)},{n})" for a, s, n in requirements)
    clauses = [
        *ctes,
        f"labels(adapter,event_id,expected,stratum) AS (VALUES {values})",
        f"required(adapter,stratum,expected_count) AS (VALUES {required})",
        f"""measured AS (
                   SELECT l.*,e.priority::text AS actual FROM labels l
                   LEFT JOIN {actual_relation} e ON e.adapter=l.adapter AND e.event_id=l.event_id
               )""",
    ]
    if output == "summary":
        select = """SELECT r.adapter,r.stratum,r.expected_count::bigint,
            count(m.event_id)::bigint AS labeled,count(m.actual)::bigint AS resolved,
            count(*) FILTER(WHERE m.actual=m.expected)::bigint AS correct,
            (r.expected_count-count(m.actual))::bigint AS missing,
            (r.expected_count-count(*) FILTER(WHERE m.actual=m.expected))::bigint AS failures,
            count(*) FILTER(WHERE m.expected='self')::bigint AS self_labeled,
            count(*) FILTER(WHERE m.expected='self' AND m.actual='self')::bigint AS self_correct,
            count(*) FILTER(WHERE m.actual='self')::bigint AS predicted_self,
            count(*) FILTER(WHERE m.expected='self' AND m.actual='self')::bigint AS true_self
            FROM required r LEFT JOIN measured m USING(adapter,stratum)
            GROUP BY r.adapter,r.stratum,r.expected_count"""
    elif output in ("rows", "details"):
        select = "SELECT adapter,event_id,stratum,expected,actual FROM measured"
        if output == "details":
            select += " WHERE actual IS DISTINCT FROM expected"
    else:
        raise ValueError("Unknown benchmark output")
    return "WITH " + ",\n".join(clauses) + "\n" + select


def publish_labels(warehouse, payload):
    validate_payload(payload)
    now = datetime.now(UTC)
    warehouse._insert_rows(
        "priority_benchmark_labels",
        [
            dict(
                label_set="current",
                payload_json=json.dumps(payload),
                label_sha256=label_digest(payload),
                updated_at=now,
                sync_version=int(now.timestamp() * 1000000),
            )
        ],
        PRIORITY_BENCHMARK_LABEL_COLUMNS,
    )
    # Old results cannot remain green after a label edit.
    warehouse._command("DELETE FROM @priority_benchmark_results")
    return len(payload["labels"])


def pull_labels(warehouse):
    rows = warehouse._query(
        "SELECT payload_json FROM @priority_benchmark_labels WHERE label_set=%s",
        ("current",),
    )
    if not rows:
        raise LabelsUnpublishedError("Priority labels have not been published")
    return validate_payload(json.loads(rows[0][0]))


def measure(warehouse):
    payload = pull_labels(warehouse)
    results = warehouse._query_dicts(benchmark_sql(payload))
    now = datetime.now(UTC)
    warehouse._insert_rows(
        "priority_benchmark_results",
        [
            dict(
                label_set="current",
                results_json=json.dumps(results),
                label_sha256=label_digest(payload),
                collected_at=now,
                sync_version=int(now.timestamp() * 1000000),
            )
        ],
        PRIORITY_BENCHMARK_RESULT_COLUMNS,
    )
    return results


def mart_sql():
    """Fail loudly before publication/measurement, on changed labels or stale results."""
    precision_stratum = f"q.stratum ~ '^{SELF_PRECISION_STRATUM}($|_)'"
    return f"""WITH config AS (
        SELECT payload_json::jsonb AS payload,label_sha256 FROM @priority_benchmark_labels
        WHERE label_set='current'
    ), required AS (
        SELECT a.key AS adapter,s.key AS stratum,s.value::text::bigint AS expected_count
        FROM config c CROSS JOIN LATERAL jsonb_each(c.payload->'requirements') a
        CROSS JOIN LATERAL jsonb_each(a.value) s
    ), label_counts AS (
        SELECT l.adapter,l.stratum,count(*) AS n,
            count(*) FILTER(WHERE l.expected='self') AS self_n
        FROM config c CROSS JOIN LATERAL jsonb_to_recordset(c.payload->'labels')
        AS l(adapter text,stratum text,expected text) GROUP BY l.adapter,l.stratum
    ), scores AS (
        SELECT r.collected_at,r.label_sha256,x.*
        FROM @priority_benchmark_results r CROSS JOIN LATERAL jsonb_to_recordset(r.results_json::jsonb)
        AS x(adapter text,stratum text,expected_count bigint,labeled bigint,resolved bigint,
             correct bigint,missing bigint,failures bigint,self_labeled bigint,self_correct bigint,
             predicted_self bigint,true_self bigint)
        WHERE label_set='current'
    ), grouped AS (
        SELECT q.adapter,sum(q.expected_count)::bigint AS expected_count,
            coalesce(sum(l.n),0)::bigint AS labeled,
            coalesce(sum(CASE WHEN l.n=q.expected_count THEN s.resolved ELSE 0 END),0)::bigint AS resolved,
            coalesce(sum(CASE WHEN l.n=q.expected_count THEN s.correct ELSE 0 END),0)::bigint AS correct,
            coalesce(sum(l.self_n),0)::bigint AS self_labeled,
            coalesce(sum(CASE WHEN l.n=q.expected_count THEN s.self_correct ELSE 0 END),0)::bigint AS self_correct,
            coalesce(sum(s.predicted_self),0)::bigint AS predicted_self,
            coalesce(sum(s.true_self),0)::bigint AS true_self,
            coalesce(sum(q.expected_count) FILTER(WHERE {precision_stratum}),0)::bigint AS precision_sample_count,
            coalesce(sum(s.predicted_self) FILTER(WHERE {precision_stratum}),0)::bigint AS sample_predicted_self,
            coalesce(sum(s.true_self) FILTER(WHERE {precision_stratum}),0)::bigint AS sample_true_self,
            bool_and(CASE WHEN {precision_stratum} THEN coalesce(s.predicted_self>0
                AND s.true_self::numeric/nullif(s.predicted_self,0)>={SELF_PRECISION_THRESHOLD},false)
                ELSE true END) AS precision_samples_ok,
            (sum(q.expected_count)-coalesce(sum(CASE WHEN l.n=q.expected_count THEN s.resolved ELSE 0 END),0))::bigint AS missing,
            bool_and(coalesce(s.label_sha256=c.label_sha256 AND s.collected_at>now()-interval '7 days'
                     AND s.labeled=q.expected_count AND coalesce(l.n,0)=q.expected_count
                     AND s.self_labeled=l.self_n AND s.predicted_self IS NOT NULL
                     AND s.true_self IS NOT NULL,false)) AS complete,
            min(s.collected_at) AS collected_at
        FROM required q CROSS JOIN config c LEFT JOIN scores s USING(adapter,stratum)
        LEFT JOIN label_counts l USING(adapter,stratum)
        GROUP BY q.adapter
    ), judged AS (
        SELECT *,CASE WHEN precision_sample_count>0 THEN sample_predicted_self ELSE predicted_self END AS precision_predicted,
            CASE WHEN precision_sample_count>0 THEN sample_true_self ELSE true_self END AS precision_correct
        FROM grouped
    ) SELECT adapter,labeled,resolved,correct,missing,(expected_count-correct)::bigint AS failures,
        correct::numeric/nullif(expected_count,0) AS accuracy,{PASS_THRESHOLD}::numeric AS pass_threshold,
        CASE WHEN complete AND labeled=expected_count AND expected_count>0
             AND correct::numeric/expected_count>={PASS_THRESHOLD}
             AND (self_labeled=0 OR self_correct::numeric/nullif(self_labeled,0)>={SELF_RECALL_THRESHOLD})
             AND (precision_sample_count=0 AND precision_predicted=0
                  OR precision_correct::numeric/nullif(precision_predicted,0)>={SELF_PRECISION_THRESHOLD})
             AND precision_samples_ok
             THEN 'ok' ELSE 'failing' END AS status,
        collected_at,self_labeled AS gold_self_labeled,self_correct AS gold_self_correct,
        self_correct::numeric/nullif(self_labeled,0) AS gold_self_recall,
        {SELF_RECALL_THRESHOLD}::numeric AS self_recall_threshold,
        precision_predicted AS self_predicted,precision_correct AS self_true,
        precision_correct::numeric/nullif(precision_predicted,0) AS self_precision,
        {SELF_PRECISION_THRESHOLD}::numeric AS self_precision_threshold,
        precision_sample_count
    FROM judged
    UNION ALL SELECT 'unpublished',0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,
        0::numeric,{PASS_THRESHOLD}::numeric,'failing',NULL::timestamptz,
        0::bigint,0::bigint,NULL::numeric,{SELF_RECALL_THRESHOLD}::numeric,
        0::bigint,0::bigint,NULL::numeric,{SELF_PRECISION_THRESHOLD}::numeric,0::bigint
    WHERE NOT EXISTS(SELECT 1 FROM required)"""
