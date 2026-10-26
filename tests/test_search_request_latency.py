"""C6 grades every real request, including failures, against its own host sample."""
import os
from datetime import UTC, datetime, timedelta

import pytest

from personal_data_warehouse.postgres import PostgresWarehouse
from tests.conftest import cleanup_test_warehouse, make_test_schema


@pytest.fixture()
def warehouse():
    wh = PostgresWarehouse(os.environ['POSTGRES_DATABASE_URL'], schema=make_test_schema())
    try:
        yield wh
    finally:
        cleanup_test_warehouse(wh)


def seed(wh, durations, *, age=timedelta(hours=1), client='pdw', **over):
    for i, duration in enumerate(durations):
        row = dict(occurred_at=datetime.now(UTC) - age, request_id=f'{client}-{age}-{i}',
                   client=client, duration_ms=duration, status='ok', slowest_leg='lexical',
                   io_pressure_full_avg10=0, cpu_pressure_some_avg10=0, load_1m=1, cpu_count=28)
        row.update(over)
        wh._command(
            'INSERT INTO @search_request_timings (' + ','.join(row) + ') VALUES (' +
            ','.join(['%s'] * len(row)) + ')', tuple(row.values()))


def read(wh):
    return {r['window']: r for r in wh._query_dicts('SELECT * FROM @marts_search_request_latency')}


def test_real_latency_october_25_shape_catches_slow_tail_and_excludes_probes(warehouse):
    warehouse.ensure_pipeline_health_tables()
    # Half are slow, despite a median below target; all slow work used an idle host.
    seed(warehouse, [900] * 51 + [7000] * 49, status='error')
    seed(warehouse, [60000] * 100, client='search-benchmark')
    seed(warehouse, [60000] * 100, client='contract-audit')
    for row in read(warehouse).values():
        assert row['requests'] == row['errors'] == 100
        assert row['p50_ms'] == 900
        assert row['p90_ms'] == 7000
        assert float(row['share_over_2s']) == .49
        assert row['slow_requests'] == row['idle_slow_requests'] == 49
        assert float(row['idle_slow_share']) == 1
        assert row['saturation_mix'] == {'idle': 49}
        assert row['slowest_leg_mix'] == {'lexical': 49}
        assert row['status'] == 'attention'


def test_latency_windows_always_exist_and_only_count_their_traffic(warehouse):
    warehouse.ensure_pipeline_health_tables()
    assert {k: r['status'] for k, r in read(warehouse).items()} == {
        'rolling_24h': 'no_data', 'rolling_7d': 'no_data'}
    assert all(r['p50_ms'] is None and r['saturation_mix'] == {} for r in read(warehouse).values())
    seed(warehouse, [1000] * 20, age=timedelta(days=2))
    seed(warehouse, [9000] * 30, age=timedelta(days=8))
    seed(warehouse, [9000] * 30, age=timedelta(hours=-1))
    rows = read(warehouse)
    assert rows['rolling_24h']['requests'] == 0
    assert rows['rolling_7d']['requests'] == 20
    assert rows['rolling_7d']['status'] == 'ok'
    assert rows['rolling_7d']['slowest_leg_mix'] == {}
    assert rows['rolling_7d']['idle_slow_share'] is None


@pytest.mark.parametrize('over,kind', [
    ({'io_pressure_full_avg10': 10, 'cpu_pressure_some_avg10': 80}, 'io_bound'),
    ({'cpu_pressure_some_avg10': 50}, 'cpu_bound'),
    ({'load_1m': 28}, 'cpu_bound'),
    ({}, 'idle'),
    *[({col: -1}, 'unmeasured') for col in (
        'io_pressure_full_avg10', 'cpu_pressure_some_avg10', 'load_1m', 'cpu_count')],
])
def test_slow_search_saturation_uses_each_requests_pressure(warehouse, over, kind):
    warehouse.ensure_pipeline_health_tables()
    seed(warehouse, [2100] * 20, **over)
    row = read(warehouse)['rolling_24h']
    assert row['saturation_mix'] == {kind: 20}
    assert row['idle_slow_requests'] == (20 if kind == 'idle' else 0)


@pytest.mark.parametrize('durations,status', [
    ([9000] * 19, 'no_data'),
    ([2000] * 20, 'ok'),
    ([2001] * 20, 'attention'),
    ([1000] * 17 + [2100] * 3, 'attention'),
])
def test_latency_sample_and_target_boundaries(warehouse, durations, status):
    warehouse.ensure_pipeline_health_tables()
    seed(warehouse, durations)
    row = read(warehouse)['rolling_24h']
    assert row['status'] == status
    if durations[0] == 2000:
        assert row['slow_requests'] == 0


@pytest.mark.parametrize('idle,slow,status', [(4, 40, 'ok'), (5, 50, 'attention'), (5, 51, 'ok')])
def test_idle_slow_share_requires_five_and_ten_percent(warehouse, idle, slow, status):
    warehouse.ensure_pipeline_health_tables()
    seed(warehouse, [1000] * 1000)
    seed(warehouse, [2100] * idle, client='web')
    seed(warehouse, [2100] * (slow-idle), client='phone', io_pressure_full_avg10=10)
    row = read(warehouse)['rolling_24h']
    assert row['p90_ms'] == 1000  # isolates the independent idle-host condition
    assert row['status'] == status
    assert float(row['idle_slow_share']) == pytest.approx(idle / slow)
