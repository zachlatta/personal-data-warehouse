from __future__ import annotations

from collections.abc import Callable
import json
import threading
from urllib import error, request

import pytest

from personal_data_warehouse.agent_tool_proxy import (
    DEFAULT_AGENT_TOOL_ALLOWLIST,
    WarehouseAppConfig,
    agent_client_name,
    run_agent_tool_proxy,
    warehouse_app_config_from_env,
)
from tests.warehouse_app_stub import StubWarehouseApp


REAL_TOKEN = "real-app-secret-token-at-least-32-chars"


@pytest.fixture
def fake_app() -> Callable[..., StubWarehouseApp]:
    created: list[StubWarehouseApp] = []

    def factory(**kwargs) -> StubWarehouseApp:
        app = StubWarehouseApp(**kwargs)
        created.append(app)
        return app

    yield factory
    for app in created:
        app.close()


def proxy_for(app: StubWarehouseApp, **kwargs):
    return run_agent_tool_proxy(
        app=WarehouseAppConfig(base_url=app.base_url, token=REAL_TOKEN),
        bind_host="127.0.0.1",
        public_host="127.0.0.1",
        **kwargs,
    )


def call_proxy(env: dict[str, str], path: str, payload: dict | None = None, *, token: str | None = None) -> tuple[int, dict]:
    bearer = token if token is not None else f"{env['PDW_CLIENT_NAME']}:{env['PDW_SECRET_TOKEN']}"
    req = request.Request(
        env["PDW_API_URL"] + path,
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
        headers={"authorization": f"Bearer {bearer}", "content-type": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    try:
        with request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8") or "{}")


def test_agent_tool_proxy_exports_pdw_cli_env_without_the_real_token(fake_app) -> None:
    app = fake_app()

    with proxy_for(app, run_id="run-1") as env:
        assert env["PDW_API_URL"].startswith("http://127.0.0.1:")
        assert env["PDW_SECRET_TOKEN"] != REAL_TOKEN
        assert len(env["PDW_SECRET_TOKEN"]) >= 32
        assert env["PDW_CLIENT_NAME"] == "pdw-agent-run-1"
        # The CLI self-updates from GitHub on every invocation otherwise, which
        # a locked-down agent container must never attempt.
        assert env["PDW_NO_AUTO_UPDATE"] == "1"
        assert REAL_TOKEN not in json.dumps(env)


def test_agent_tool_proxy_forwards_allowlisted_call_with_the_real_token(fake_app) -> None:
    app = fake_app(responses={"sql": {"rows": "n\n1", "total_rows": 1, "format": "csv"}})

    with proxy_for(app) as env:
        status, payload = call_proxy(env, "/api/tools/sql", {"sql": "SELECT 1 AS n", "question": "smoke", "format": "csv"})

    assert status == 200
    assert payload["data"]["rows"] == "n\n1"
    method, path, authorization, body = app.calls[-1]
    assert (method, path) == ("POST", "/api/tools/sql")
    assert authorization == f"Bearer pdw-agent:{REAL_TOKEN}"
    assert json.loads(body)["sql"] == "SELECT 1 AS n"


def test_agent_tool_proxy_reports_the_run_to_the_app(fake_app) -> None:
    """Queries should be attributable to a run in the app's own request log."""

    app = fake_app(responses={"sql": {"rows": "n\n1"}})

    with proxy_for(app, run_id="agent-abc123") as env:
        call_proxy(env, "/api/tools/sql", {"sql": "SELECT 1 AS n"})

    _method, _path, authorization, _body = app.calls[-1]
    assert authorization == f"Bearer pdw-agent-agent-abc123:{REAL_TOKEN}"


def test_agent_client_name_stays_within_the_apps_limit() -> None:
    """An over-long or colon-bearing name is a 401 upstream, not a warning."""

    name = agent_client_name("pdw-agent", "agent-" + "f" * 200)
    assert len(name) == 64
    assert ":" not in agent_client_name("pdw:agent", "run:1")


def test_agent_tool_proxy_identifies_itself_to_cloudflare(fake_app) -> None:
    """Upstream requests must not go out as `Python-urllib`.

    The app sits behind Cloudflare, which bans that signature outright (error
    1010, `browser_signature_banned`) — so without an explicit User-Agent every
    upstream call 403s in production while passing every local test.
    """

    app = fake_app(
        responses={
            "sql": {"rows": "n\n1"},
            "get_object": {
                "storage_file_id": "fid-1",
                "exists": True,
                "download_url": "https://public.example.com/objects/fid-1?exp=9&sig=s",
            },
        }
    )
    app.object_bytes = b"bytes"

    with proxy_for(app) as env:
        call_proxy(env, "/api/tools", None)
        call_proxy(env, "/api/tools/sql", {"sql": "SELECT 1"})
        _status, payload = call_proxy(env, "/api/tools/get_object", {"storage_file_id": "fid-1"})
        with request.urlopen(payload["data"]["download_url"], timeout=10) as response:
            response.read()

    assert app.user_agents, "no upstream requests were recorded"
    for agent in app.user_agents:
        assert agent.startswith("pdw-agent-tool-proxy/"), agent
        assert "urllib" not in agent.lower()


def test_agent_tool_proxy_rejects_a_wrong_bearer(fake_app) -> None:
    app = fake_app(responses={"sql": {"rows": ""}})

    with proxy_for(app) as env:
        status, payload = call_proxy(env, "/api/tools/sql", {"sql": "SELECT 1"}, token="pdw-agent:not-the-token")

    assert status == 401
    assert payload["error"]["code"] == "unauthorized"
    assert app.tool_call_paths() == []


def test_agent_tool_proxy_blocks_tools_outside_the_allowlist(fake_app) -> None:
    app = fake_app(responses={"propose_mutation": {"ok": True}})

    with proxy_for(app) as env:
        status, payload = call_proxy(env, "/api/tools/propose_mutation", {"kind": "gmail_label"})

    assert status == 404
    assert payload["error"]["code"] == "tool_not_found"
    assert "sql" in payload["error"]["message"]
    assert app.tool_call_paths() == [], "a blocked tool must never reach the app"


def test_agent_tool_proxy_keeps_the_connection_usable_after_a_blocked_call(fake_app) -> None:
    """A rejected POST must still consume its body.

    The Go CLI reuses keep-alive connections; an undrained body would become
    the head of the next request and desync everything after it.
    """

    import http.client

    app = fake_app(responses={"sql": {"rows": "n\n1", "total_rows": 1, "format": "csv"}})

    with proxy_for(app) as env:
        host = env["PDW_API_URL"].removeprefix("http://")
        connection = http.client.HTTPConnection(host, timeout=10)
        headers = {
            "authorization": f"Bearer {env['PDW_CLIENT_NAME']}:{env['PDW_SECRET_TOKEN']}",
            "content-type": "application/json",
        }
        try:
            connection.request("POST", "/api/tools/propose_mutation", json.dumps({"padding": "x" * 4096}), headers)
            blocked = connection.getresponse()
            blocked.read()

            # Same connection, immediately after.
            connection.request("POST", "/api/tools/sql", json.dumps({"sql": "SELECT 1 AS n"}), headers)
            allowed = connection.getresponse()
            payload = json.loads(allowed.read().decode("utf-8"))
        finally:
            connection.close()

    assert blocked.status == 404
    assert allowed.status == 200
    assert payload["data"]["rows"] == "n\n1"


def test_agent_tool_proxy_filters_the_tool_list_to_the_allowlist(fake_app) -> None:
    app = fake_app()

    with proxy_for(app) as env:
        status, payload = call_proxy(env, "/api/tools")

    assert status == 200
    assert {entry["name"] for entry in payload["data"]} == set(DEFAULT_AGENT_TOOL_ALLOWLIST)


def test_agent_tool_proxy_caps_csv_rows_so_one_query_cannot_flood_the_agent(fake_app) -> None:
    rows = "\n".join(["n"] + [str(index) for index in range(500)])
    app = fake_app(responses={"sql": {"rows": rows, "total_rows": 500, "format": "csv"}})

    with proxy_for(app, max_rows=3) as env:
        _status, payload = call_proxy(env, "/api/tools/sql", {"sql": "SELECT n FROM generate_series(0, 499) n"})

    data = payload["data"]
    assert data["rows"].splitlines() == ["n", "0", "1", "2"]
    assert data["truncated"] is True
    assert data["total_rows"] == 500
    assert data["proxy_row_limit"] == 3


def test_agent_tool_proxy_caps_json_rows(fake_app) -> None:
    app = fake_app(responses={"sql": {"rows": [{"n": index} for index in range(50)], "total_rows": 50, "format": "json"}})

    with proxy_for(app, max_rows=2) as env:
        _status, payload = call_proxy(env, "/api/tools/sql", {"sql": "SELECT 1", "format": "json"})

    assert payload["data"]["rows"] == [{"n": 0}, {"n": 1}]
    assert payload["data"]["truncated"] is True


def test_agent_tool_proxy_leaves_small_results_untouched(fake_app) -> None:
    app = fake_app(responses={"sql": {"rows": "n\n1", "total_rows": 1, "format": "csv"}})

    with proxy_for(app, max_rows=50) as env:
        _status, payload = call_proxy(env, "/api/tools/sql", {"sql": "SELECT 1 AS n"})

    assert payload["data"]["rows"] == "n\n1"
    assert "truncated" not in payload["data"]
    assert "proxy_row_limit" not in payload["data"]


def test_agent_tool_proxy_rewrites_object_download_urls_to_itself(fake_app) -> None:
    app = fake_app(
        responses={
            "get_object": {
                "storage_file_id": "fid-1",
                "exists": True,
                "filename": "photo.jpg",
                "download_url": "https://public.example.com/objects/fid-1?exp=99&sig=abc",
            }
        }
    )
    app.object_bytes = b"jpeg-bytes"

    with proxy_for(app) as env:
        _status, payload = call_proxy(env, "/api/tools/get_object", {"storage_file_id": "fid-1"})
        download_url = payload["data"]["download_url"]
        assert download_url.startswith(env["PDW_API_URL"] + "/objects/fid-1?")
        # The signed link is its own credential, so the stream needs no bearer.
        with request.urlopen(download_url, timeout=10) as response:
            assert response.read() == b"jpeg-bytes"

    assert app.object_paths() == ["/objects/fid-1?exp=99&sig=abc"]


def test_agent_tool_proxy_streams_objects_larger_than_one_chunk(fake_app) -> None:
    payload = bytes(range(256)) * 40_000  # ~10 MB, several stream chunks
    app = fake_app(
        responses={
            "get_object": {
                "storage_file_id": "big",
                "exists": True,
                "download_url": "https://public.example.com/objects/big?exp=1&sig=z",
            }
        }
    )
    app.object_bytes = payload

    with proxy_for(app) as env:
        _status, response = call_proxy(env, "/api/tools/get_object", {"storage_file_id": "big"})
        with request.urlopen(response["data"]["download_url"], timeout=30) as streamed:
            assert streamed.read() == payload


def test_agent_tool_proxy_serializes_upstream_calls(fake_app) -> None:
    active = 0
    max_active = 0
    guard = threading.Lock()
    original_write = StubWarehouseApp.write_json

    def slow_write(handler, payload, *, status=200):
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        try:
            original_write(handler, payload, status=status)
        finally:
            with guard:
                active -= 1

    app = fake_app(responses={"sql": {"rows": "n\n1"}})
    StubWarehouseApp.write_json = staticmethod(slow_write)
    try:
        with proxy_for(app) as env:
            threads = [
                threading.Thread(target=call_proxy, args=(env, "/api/tools/sql", {"sql": "SELECT 1"}))
                for _ in range(4)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=15)
    finally:
        StubWarehouseApp.write_json = staticmethod(original_write)

    assert max_active == 1


def test_warehouse_app_config_from_env_reads_pdw_then_legacy_names() -> None:
    config = warehouse_app_config_from_env(
        {"PDW_API_URL": "https://warehouse.example.com/", "PDW_SECRET_TOKEN": "secret-token"}
    )
    assert config.base_url == "https://warehouse.example.com"
    assert config.token == "secret-token"

    legacy = warehouse_app_config_from_env({"MCP_BASE_URL": "https://legacy.example.com", "MCP_SECRET_TOKEN": "legacy"})
    assert legacy.base_url == "https://legacy.example.com"
    assert legacy.token == "legacy"


def test_warehouse_app_config_from_env_fails_loud_when_unset() -> None:
    with pytest.raises(RuntimeError) as excinfo:
        warehouse_app_config_from_env({})

    assert "PDW_API_URL" in str(excinfo.value)
    assert "PDW_SECRET_TOKEN" in str(excinfo.value)
