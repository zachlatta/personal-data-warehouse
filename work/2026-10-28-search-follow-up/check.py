"""Read-only bounded operational checks; no query text or personal data is collected."""
import subprocess
import os
from pathlib import Path

ROOT=Path(__file__).parent
def query(name, sql, db='warehouse'):
    container=os.environ[db.upper()+'_CONTAINER']
    result=subprocess.run(['ssh',os.environ['AUDIT_HOST'],f'sudo -n docker exec -i {container} psql -X -v ON_ERROR_STOP=1 -U postgres --csv'],input="SET statement_timeout='30s'; SET max_parallel_workers_per_gather=0; BEGIN READ ONLY;\n"+sql+"\nROLLBACK;",text=True,capture_output=True,check=True)
    (ROOT/(name+'.csv')).write_text(result.stdout)
    print(name, result.stdout)
query('latency',"""WITH t AS (SELECT * FROM ops.search_request_timings WHERE occurred_at >= '2026-10-27 16:10Z' AND occurred_at < '2026-10-28 16:10Z' AND client NOT IN ('search-benchmark','contract-audit'))
SELECT CASE WHEN GROUPING(has_priorities)=1 THEN 'all real traffic' WHEN has_priorities=1 THEN 'priority filtered' ELSE 'unfiltered' END scope, count(*) n, count(*) FILTER(WHERE status <> 'ok') errors, percentile_cont(.5) WITHIN GROUP(ORDER BY duration_ms) p50, percentile_cont(.9) WITHIN GROUP(ORDER BY duration_ms) p90, count(*) FILTER(WHERE duration_ms>2000) over_2s, count(*) FILTER(WHERE degraded_legs LIKE '%exact%') exact_cut FROM t GROUP BY GROUPING SETS ((has_priorities),());""")
query('steps',"""WITH t AS (SELECT * FROM ops.search_request_timings WHERE occurred_at >= '2026-10-27 16:10Z' AND occurred_at < '2026-10-28 16:10Z' AND client NOT IN ('search-benchmark','contract-audit'))
SELECT leg,count(*) n,percentile_cont(.5) WITHIN GROUP(ORDER BY ms) p50,percentile_cont(.9) WITHIN GROUP(ORDER BY ms) p90 FROM t CROSS JOIN LATERAL (VALUES ('embed',embed_ms),('lexical',lexical_ms),('exact',exact_ms),('semantic',semantic_ms),('fuse',fuse_ms)) v(leg,ms) WHERE ms>=0 GROUP BY leg;
""")
query('enrichment_runs',"""SELECT status,create_timestamp,to_timestamp(start_time) started,to_timestamp(end_time) ended FROM runs WHERE pipeline_name='apple_voice_memos_enrichment_job' AND create_timestamp >= '2026-10-27 16:10' ORDER BY create_timestamp DESC LIMIT 10;""",'dagster')

query('lookup_counters',"""SELECT 'legacy identity lookup' kind,count(*) shapes,sum(calls) calls,sum(shared_blks_read) blocks_read FROM pg_stat_statements WHERE replace(query,chr(34),'') ILIKE '%FROM base_slack.messages AS m%' AND query ILIKE '%AS speaker%' AND query ILIKE '%ILIKE%';
SELECT 'indexed identity lookup' kind,calls,round(total_exec_time::numeric) total_ms,shared_blks_read,stats_since FROM pg_stat_statements WHERE queryid=-6639990069809731157;
SELECT dealloc,stats_reset FROM pg_stat_statements_info;""")
query('health',"""SELECT component,status,resident_bytes,total_bytes,resident_fraction,updated_at FROM marts_ops.search_health WHERE component='cache_residency';
SELECT * FROM marts_ops.search_request_latency;""")
