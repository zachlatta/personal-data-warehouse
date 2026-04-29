from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
import socket
import subprocess
from typing import Callable

DEFAULT_BLOCKED_HARDWARE_PORT_PATTERNS = (
    r"iphone",
    r"ipad",
    r"android",
    r"bluetooth pan",
    r"tether",
    r"hotspot",
    r"mobile",
    r"cellular",
)
DEFAULT_BLOCKED_SSID_PATTERNS = (
    r"iphone",
    r"ipad",
    r"android",
    r"hotspot",
    r"tether",
    r"inflight",
    r"gogo",
    r"fly-?fi",
    r"united.*wi-?fi",
    r"delta.*wi-?fi",
    r"aa-?inflight",
    r"southwest.*wi-?fi",
    r"jetblue.*fly-?fi",
    r"alaska.*wi-?fi",
)


@dataclass(frozen=True)
class NetworkDecision:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class NetworkContext:
    interface: str
    hardware_port: str
    ssid: str = ""
    ssid_source: str = ""


@dataclass(frozen=True)
class SsidProbe:
    source: str
    ssid: str
    detail: str = ""


class NetworkPolicy:
    def __init__(
        self,
        *,
        blocked_ssid_patterns: tuple[str, ...] = DEFAULT_BLOCKED_SSID_PATTERNS,
        blocked_hardware_port_patterns: tuple[str, ...] = DEFAULT_BLOCKED_HARDWARE_PORT_PATTERNS,
        require_wifi_ssid: bool = False,
        runner: Callable[[list[str]], str] | None = None,
    ) -> None:
        self._blocked_ssid_patterns = blocked_ssid_patterns
        self._blocked_hardware_port_patterns = blocked_hardware_port_patterns
        self._require_wifi_ssid = require_wifi_ssid
        self._runner = runner or run_command

    @classmethod
    def from_env(cls) -> NetworkPolicy:
        return cls(
            blocked_ssid_patterns=patterns_from_env(
                "VOICE_MEMOS_UPLOAD_BLOCKED_SSID_PATTERNS",
                DEFAULT_BLOCKED_SSID_PATTERNS,
            ),
            blocked_hardware_port_patterns=patterns_from_env(
                "VOICE_MEMOS_UPLOAD_BLOCKED_HARDWARE_PORT_PATTERNS",
                DEFAULT_BLOCKED_HARDWARE_PORT_PATTERNS,
            ),
            require_wifi_ssid=parse_bool_env("VOICE_MEMOS_UPLOAD_REQUIRE_WIFI_SSID", False),
        )

    def check(self) -> NetworkDecision:
        context = self.context()
        if context is None:
            return NetworkDecision(False, "no default network route")

        hardware_port = context.hardware_port.lower()
        if matches_any(hardware_port, self._blocked_hardware_port_patterns):
            return NetworkDecision(False, f"blocked hardware port: {context.hardware_port}")

        if context.hardware_port.lower() == "wi-fi":
            if not context.ssid:
                if self._require_wifi_ssid:
                    return NetworkDecision(False, "Wi-Fi SSID unavailable")
                return NetworkDecision(True, "Wi-Fi allowed; SSID unavailable")
            if matches_any(context.ssid.lower(), self._blocked_ssid_patterns):
                return NetworkDecision(False, f"blocked Wi-Fi SSID: {context.ssid}")

        return NetworkDecision(True, f"{context.hardware_port or context.interface} allowed")

    def context(self) -> NetworkContext | None:
        interface = default_route_interface(self._runner)
        if not interface:
            return None
        hardware_ports = hardware_ports_by_device(self._runner)
        hardware_port = hardware_ports.get(interface, interface)
        ssid = ""
        ssid_source = ""
        if hardware_port.lower() == "wi-fi":
            probe = wifi_ssid_probe(interface, self._runner)
            ssid = probe.ssid
            ssid_source = probe.source
        return NetworkContext(interface=interface, hardware_port=hardware_port, ssid=ssid, ssid_source=ssid_source)

    def ssid_probe(self, interface: str) -> SsidProbe:
        return wifi_ssid_probe(interface, self._runner)


def default_route_interface(runner: Callable[[list[str]], str]) -> str:
    output = runner(["route", "-n", "get", "default"])
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("interface:"):
            return stripped.split(":", 1)[1].strip()
    return ""


def hardware_ports_by_device(runner: Callable[[list[str]], str]) -> dict[str, str]:
    output = runner(["networksetup", "-listallhardwareports"])
    ports: dict[str, str] = {}
    current_port = ""
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Hardware Port:"):
            current_port = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("Device:") and current_port:
            ports[stripped.split(":", 1)[1].strip()] = current_port
            current_port = ""
    return ports


def wifi_ssid(interface: str, runner: Callable[[list[str]], str]) -> str:
    return wifi_ssid_probe(interface, runner).ssid


def wifi_ssid_probe(interface: str, runner: Callable[[list[str]], str]) -> SsidProbe:
    probes = (
        SsidProbe("CoreWLAN", ssid_from_corewlan(interface, runner)),
        SsidProbe("networksetup", ssid_from_networksetup_output(runner(["networksetup", "-getairportnetwork", interface]))),
        SsidProbe("ipconfig", ssid_from_ipconfig_output(runner(["ipconfig", "getsummary", interface]))),
        SsidProbe(
            "system_profiler",
            ssid_from_system_profiler_json(runner(["system_profiler", "SPAirPortDataType", "-json"]), interface),
        ),
    )
    for probe in probes:
        if probe.ssid:
            return probe
    return SsidProbe("unavailable", "", "SSID unavailable; recent macOS requires Location Services authorization")


def ssid_from_corewlan(interface: str, runner: Callable[[list[str]], str]) -> str:
    script = (
        "import CoreWLAN; "
        f"let interfaceName = \"{swift_string_literal_value(interface)}\"; "
        "let client = CWWiFiClient.shared(); "
        "let interfaces = client.interfaces() ?? []; "
        "let match = interfaces.first { $0.interfaceName == interfaceName } ?? client.interface(); "
        "if let ssid = match?.ssid(), !ssid.isEmpty { print(ssid) }"
    )
    return normalized_ssid(runner(["swift", "-e", script]))


def ssid_from_networksetup_output(output: str) -> str:
    if ":" not in output:
        return ""
    prefix, value = output.split(":", 1)
    if "network" not in prefix.lower():
        return ""
    return normalized_ssid(value)


def ssid_from_ipconfig_output(output: str) -> str:
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("SSID :"):
            return normalized_ssid(stripped.split(":", 1)[1])
    return ""


def ssid_from_system_profiler_json(output: str, interface: str) -> str:
    try:
        payload = json.loads(output)
    except json.JSONDecodeError:
        return ""
    for item in payload.get("SPAirPortDataType", []):
        if not isinstance(item, dict):
            continue
        interfaces = item.get("spairport_airport_interfaces", [])
        if not isinstance(interfaces, list):
            continue
        for airport_interface in interfaces:
            if not isinstance(airport_interface, dict) or airport_interface.get("_name") != interface:
                continue
            network = airport_interface.get("spairport_current_network_information", {})
            if isinstance(network, dict):
                return normalized_ssid(str(network.get("_name", "")))
    return ""


def normalized_ssid(value: str) -> str:
    ssid = value.strip()
    return "" if ssid in {"", "<redacted>"} else ssid


def swift_string_literal_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def preflight_google_drive(timeout_seconds: float) -> NetworkDecision:
    try:
        socket.create_connection(("www.googleapis.com", 443), timeout=timeout_seconds).close()
    except OSError as exc:
        return NetworkDecision(False, f"Google Drive preflight failed: {exc}")
    return NetworkDecision(True, "Google Drive preflight succeeded")


def patterns_from_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    return tuple(pattern.strip() for pattern in value.split(",") if pattern.strip())


def parse_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def matches_any(value: str, patterns: tuple[str, ...]) -> bool:
    return any(re.search(pattern, value, flags=re.IGNORECASE) for pattern in patterns)


def run_command(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return ""
