from __future__ import annotations

from personal_data_warehouse_voice_memos.network import NetworkPolicy


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
