from __future__ import annotations

from personal_data_warehouse_voice_memos.network import (
    NetworkPolicy,
    ingest_base_url_from_env,
    parse_proc_net_route,
    preflight_app_ingest,
)


def test_network_policy_blocks_inflight_wifi_ssid() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en0\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: Wi-Fi\nDevice: en0\n"
        if args[:2] == ["swift", "-e"]:
            return ""
        if args == ["networksetup", "-getairportnetwork", "en0"]:
            return "Current Wi-Fi Network: United Wi-Fi\n"
        return ""

    decision = NetworkPolicy(runner=runner).check()

    assert decision.allowed is False
    assert "blocked Wi-Fi SSID" in decision.reason


def test_parse_proc_net_route_picks_lowest_metric_default() -> None:
    # Real-ish /proc/net/route: a non-default route, then two defaults
    # (Destination 00000000) on different interfaces with different metrics.
    text = (
        "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n"
        "enp1s0\t0000FEA9\t00000000\t0001\t0\t0\t1000\t0000FFFF\t0\t0\t0\n"
        "wlp2s0\t00000000\t0102000A\t0003\t0\t0\t600\t00000000\t0\t0\t0\n"
        "enp1s0\t00000000\t0102000A\t0003\t0\t0\t100\t00000000\t0\t0\t0\n"
    )
    assert parse_proc_net_route(text) == "enp1s0"


def test_parse_proc_net_route_no_default_returns_empty() -> None:
    text = (
        "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n"
        "enp1s0\t0000FEA9\t00000000\t0001\t0\t0\t1000\t0000FFFF\t0\t0\t0\n"
    )
    assert parse_proc_net_route(text) == ""


def test_network_policy_allows_linux_default_route(monkeypatch) -> None:
    # On Linux there is no BSD `route`/`networksetup`; the guard must still pass
    # when the kernel routing table shows a real default route.
    import personal_data_warehouse_voice_memos.network as network

    def runner(args: list[str]) -> str:
        return ""  # macOS commands unavailable

    monkeypatch.setattr(network, "linux_default_route_interface", lambda: "enp1s0")
    decision = NetworkPolicy(runner=runner).check()
    assert decision.allowed is True
    assert "enp1s0" in decision.reason


def test_network_policy_blocks_tethered_hardware_port() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en7\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: iPhone USB\nDevice: en7\n"
        return ""

    decision = NetworkPolicy(runner=runner).check()

    assert decision.allowed is False
    assert "blocked hardware port" in decision.reason


def test_network_policy_allows_normal_wifi() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en0\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: Wi-Fi\nDevice: en0\n"
        if args[:2] == ["swift", "-e"]:
            return ""
        if args == ["networksetup", "-getairportnetwork", "en0"]:
            return "Current Wi-Fi Network: Home Network\n"
        return ""

    decision = NetworkPolicy(runner=runner).check()

    assert decision.allowed is True


def test_network_policy_allows_wifi_when_ssid_is_unavailable_by_default() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en0\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: Wi-Fi\nDevice: en0\n"
        if args[:2] == ["swift", "-e"]:
            return ""
        if args == ["networksetup", "-getairportnetwork", "en0"]:
            return "You are not associated with an AirPort network.\n"
        return ""

    decision = NetworkPolicy(runner=runner).check()

    assert decision.allowed is True
    assert "SSID unavailable" in decision.reason


def test_network_policy_uses_ipconfig_ssid_when_networksetup_is_wrong() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en0\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: Wi-Fi\nDevice: en0\n"
        if args[:2] == ["swift", "-e"]:
            return ""
        if args == ["networksetup", "-getairportnetwork", "en0"]:
            return "You are not associated with an AirPort network.\n"
        if args == ["ipconfig", "getsummary", "en0"]:
            return "InterfaceType : WiFi\nSSID : United Wi-Fi\n"
        return ""

    decision = NetworkPolicy(runner=runner).check()

    assert decision.allowed is False
    assert decision.reason == "blocked Wi-Fi SSID: United Wi-Fi"


def test_network_policy_uses_corewlan_ssid_first() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en0\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: Wi-Fi\nDevice: en0\n"
        if args[:2] == ["swift", "-e"]:
            return "United Wi-Fi\n"
        return ""

    decision = NetworkPolicy(runner=runner).check()

    assert decision.allowed is False
    assert decision.reason == "blocked Wi-Fi SSID: United Wi-Fi"


def test_network_policy_uses_system_profiler_json_as_last_ssid_fallback() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en0\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: Wi-Fi\nDevice: en0\n"
        if args[:2] == ["swift", "-e"]:
            return ""
        if args == ["networksetup", "-getairportnetwork", "en0"]:
            return "You are not associated with an AirPort network.\n"
        if args == ["ipconfig", "getsummary", "en0"]:
            return "SSID : <redacted>\n"
        if args == ["system_profiler", "SPAirPortDataType", "-json"]:
            return (
                '{"SPAirPortDataType":[{"spairport_airport_interfaces":[{"_name":"en0",'
                '"spairport_current_network_information":{"_name":"United Wi-Fi"}}]}]}'
            )
        return ""

    decision = NetworkPolicy(runner=runner).check()

    assert decision.allowed is False
    assert decision.reason == "blocked Wi-Fi SSID: United Wi-Fi"


def test_network_policy_can_require_wifi_ssid() -> None:
    def runner(args: list[str]) -> str:
        if args == ["route", "-n", "get", "default"]:
            return "interface: en0\n"
        if args == ["networksetup", "-listallhardwareports"]:
            return "Hardware Port: Wi-Fi\nDevice: en0\n"
        if args[:2] == ["swift", "-e"]:
            return ""
        if args == ["networksetup", "-getairportnetwork", "en0"]:
            return "You are not associated with an AirPort network.\n"
        return ""

    decision = NetworkPolicy(runner=runner, require_wifi_ssid=True).check()

    assert decision.allowed is False
    assert decision.reason == "Wi-Fi SSID unavailable"


def test_app_ingest_preflight_uses_configured_app_host() -> None:
    calls: list[tuple[tuple[str, int], float]] = []

    class Connection:
        def close(self) -> None:
            pass

    def connector(address: tuple[str, int], timeout: float) -> Connection:
        calls.append((address, timeout))
        return Connection()

    decision = preflight_app_ingest(
        timeout_seconds=2.5,
        base_url="https://pdw.example.test/base",
        connector=connector,
    )

    assert decision.allowed is True
    assert calls == [(("pdw.example.test", 443), 2.5)]


def test_ingest_base_url_uses_main_api_url(monkeypatch) -> None:
    monkeypatch.delenv("PDW_INGEST_BASE_URL", raising=False)
    monkeypatch.delenv("MCP_BASE_URL", raising=False)
    monkeypatch.setenv("PDW_API_URL", "https://warehouse.example.test")

    assert ingest_base_url_from_env() == "https://warehouse.example.test"


def test_ingest_base_url_prefers_explicit_ingest_alias(monkeypatch) -> None:
    monkeypatch.setenv("PDW_INGEST_BASE_URL", "https://ingest.example.test")
    monkeypatch.setenv("PDW_API_URL", "https://warehouse.example.test")

    assert ingest_base_url_from_env() == "https://ingest.example.test"


def test_app_ingest_preflight_rejects_missing_base_url() -> None:
    decision = preflight_app_ingest(timeout_seconds=1, base_url="")

    assert decision.allowed is False
    assert "PDW_API_URL" in decision.reason
