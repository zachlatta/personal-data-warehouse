package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The upload guard: a Mac tethered to a phone or on in-flight Wi-Fi must not
// push gigabytes of photos and voice memos. Mirrors
// personal_data_warehouse_voice_memos.network.

var DefaultBlockedHardwarePortPatterns = []string{
	`iphone`, `ipad`, `android`, `bluetooth pan`, `tether`, `hotspot`, `mobile`, `cellular`,
}

var DefaultBlockedSSIDPatterns = []string{
	`iphone`, `ipad`, `android`, `hotspot`, `tether`, `inflight`, `gogo`, `fly-?fi`,
	`united.*wi-?fi`, `delta.*wi-?fi`, `aa-?inflight`, `southwest.*wi-?fi`,
	`jetblue.*fly-?fi`, `alaska.*wi-?fi`,
}

// NetworkDecision is the guard's verdict.
type NetworkDecision struct {
	Allowed bool
	Reason  string
}

// NetworkContext describes the default route.
type NetworkContext struct {
	Interface    string
	HardwarePort string
	SSID         string
	SSIDSource   string
}

// SSIDProbe names which probe answered.
type SSIDProbe struct {
	Source string
	SSID   string
	Detail string
}

// CommandRunner runs a command and returns its stdout ("" on any failure).
type CommandRunner func(args []string) string

// NetworkPolicy decides whether the current network is acceptable for bulk
// uploads.
type NetworkPolicy struct {
	BlockedSSIDPatterns         []string
	BlockedHardwarePortPatterns []string
	RequireWiFiSSID             bool
	Runner                      CommandRunner
}

// NetworkPolicyFromEnv reads <prefix>_BLOCKED_SSID_PATTERNS,
// <prefix>_BLOCKED_HARDWARE_PORT_PATTERNS and <prefix>_REQUIRE_WIFI_SSID,
// falling back to the same names under fallbackPrefix (the voice-memos
// settings every other uploader inherits).
func NetworkPolicyFromEnv(getenv Getenv, prefix, fallbackPrefix string) *NetworkPolicy {
	if fallbackPrefix == "" {
		fallbackPrefix = prefix
	}
	return &NetworkPolicy{
		BlockedSSIDPatterns:         patternsFromEnv(getenv, prefix+"_BLOCKED_SSID_PATTERNS", fallbackPrefix+"_BLOCKED_SSID_PATTERNS", DefaultBlockedSSIDPatterns),
		BlockedHardwarePortPatterns: patternsFromEnv(getenv, prefix+"_BLOCKED_HARDWARE_PORT_PATTERNS", fallbackPrefix+"_BLOCKED_HARDWARE_PORT_PATTERNS", DefaultBlockedHardwarePortPatterns),
		RequireWiFiSSID:             getenv.Bool(prefix+"_REQUIRE_WIFI_SSID", getenv.Bool(fallbackPrefix+"_REQUIRE_WIFI_SSID", false)),
		Runner:                      RunCommand,
	}
}

func patternsFromEnv(getenv Getenv, name, fallback string, defaults []string) []string {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		value = strings.TrimSpace(getenv(fallback))
	}
	if value == "" {
		return defaults
	}
	var out []string
	for _, pattern := range strings.Split(value, ",") {
		if p := strings.TrimSpace(pattern); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (p *NetworkPolicy) runner() CommandRunner {
	if p.Runner != nil {
		return p.Runner
	}
	return RunCommand
}

// Check decides for the current default route.
func (p *NetworkPolicy) Check() NetworkDecision {
	context := p.Context()
	if context == nil {
		return NetworkDecision{false, "no default network route"}
	}
	hardwarePort := strings.ToLower(context.HardwarePort)
	if matchesAny(hardwarePort, p.BlockedHardwarePortPatterns) {
		return NetworkDecision{false, "blocked hardware port: " + context.HardwarePort}
	}
	if hardwarePort == "wi-fi" {
		if context.SSID == "" {
			if p.RequireWiFiSSID {
				return NetworkDecision{false, "Wi-Fi SSID unavailable"}
			}
			return NetworkDecision{true, "Wi-Fi allowed; SSID unavailable"}
		}
		if matchesAny(strings.ToLower(context.SSID), p.BlockedSSIDPatterns) {
			return NetworkDecision{false, "blocked Wi-Fi SSID: " + context.SSID}
		}
	}
	label := context.HardwarePort
	if label == "" {
		label = context.Interface
	}
	return NetworkDecision{true, label + " allowed"}
}

// Context resolves the default route's interface, hardware port and SSID.
func (p *NetworkPolicy) Context() *NetworkContext {
	run := p.runner()
	iface := DefaultRouteInterface(run)
	if iface == "" {
		return nil
	}
	ports := HardwarePortsByDevice(run)
	port, ok := ports[iface]
	if !ok {
		port = iface
	}
	ctx := &NetworkContext{Interface: iface, HardwarePort: port}
	if strings.ToLower(port) == "wi-fi" {
		probe := WiFiSSIDProbe(iface, run)
		ctx.SSID = probe.SSID
		ctx.SSIDSource = probe.Source
	}
	return ctx
}

// SSIDProbe exposes the probe chain for diagnostics.
func (p *NetworkPolicy) SSIDProbe(iface string) SSIDProbe {
	return WiFiSSIDProbe(iface, p.runner())
}

// DefaultRouteInterface asks `route -n get default` (macOS), falling back to
// /proc/net/route on Linux (the openclaw VM).
func DefaultRouteInterface(run CommandRunner) string {
	output := run([]string{"route", "-n", "get", "default"})
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "interface:") {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "interface:"))
		}
	}
	data, err := os.ReadFile("/proc/net/route")
	if err != nil {
		return ""
	}
	return ParseProcNetRoute(string(data))
}

// ParseProcNetRoute returns the lowest-metric default route's interface.
func ParseProcNetRoute(text string) string {
	bestIface := ""
	bestMetric := -1
	lines := strings.Split(text, "\n")
	if len(lines) == 0 {
		return ""
	}
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		if fields[1] != "00000000" {
			continue
		}
		metric, err := strconv.Atoi(fields[6])
		if err != nil {
			metric = 0
		}
		if bestMetric < 0 || metric < bestMetric {
			bestIface, bestMetric = fields[0], metric
		}
	}
	return bestIface
}

// HardwarePortsByDevice parses `networksetup -listallhardwareports`.
func HardwarePortsByDevice(run CommandRunner) map[string]string {
	output := run([]string{"networksetup", "-listallhardwareports"})
	ports := map[string]string{}
	current := ""
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "Hardware Port:"):
			current = strings.TrimSpace(strings.TrimPrefix(trimmed, "Hardware Port:"))
		case strings.HasPrefix(trimmed, "Device:") && current != "":
			ports[strings.TrimSpace(strings.TrimPrefix(trimmed, "Device:"))] = current
			current = ""
		}
	}
	return ports
}

// WiFiSSIDProbe tries CoreWLAN, networksetup, ipconfig, then system_profiler.
func WiFiSSIDProbe(iface string, run CommandRunner) SSIDProbe {
	probes := []SSIDProbe{
		{Source: "CoreWLAN", SSID: ssidFromCoreWLAN(iface, run)},
		{Source: "networksetup", SSID: SSIDFromNetworksetupOutput(run([]string{"networksetup", "-getairportnetwork", iface}))},
		{Source: "ipconfig", SSID: SSIDFromIpconfigOutput(run([]string{"ipconfig", "getsummary", iface}))},
		{Source: "system_profiler", SSID: SSIDFromSystemProfilerJSON(run([]string{"system_profiler", "SPAirPortDataType", "-json"}), iface)},
	}
	for _, probe := range probes {
		if probe.SSID != "" {
			return probe
		}
	}
	return SSIDProbe{Source: "unavailable", Detail: "SSID unavailable; recent macOS requires Location Services authorization"}
}

func ssidFromCoreWLAN(iface string, run CommandRunner) string {
	script := "import CoreWLAN; " +
		fmt.Sprintf("let interfaceName = \"%s\"; ", swiftStringLiteral(iface)) +
		"let client = CWWiFiClient.shared(); " +
		"let interfaces = client.interfaces() ?? []; " +
		"let match = interfaces.first { $0.interfaceName == interfaceName } ?? client.interface(); " +
		"if let ssid = match?.ssid(), !ssid.isEmpty { print(ssid) }"
	return NormalizedSSID(run([]string{"swift", "-e", script}))
}

func swiftStringLiteral(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`)
}

// SSIDFromNetworksetupOutput parses "Current Wi-Fi Network: <ssid>".
func SSIDFromNetworksetupOutput(output string) string {
	prefix, value, ok := strings.Cut(output, ":")
	if !ok || !strings.Contains(strings.ToLower(prefix), "network") {
		return ""
	}
	return NormalizedSSID(value)
}

// SSIDFromIpconfigOutput parses the "SSID : <ssid>" line.
func SSIDFromIpconfigOutput(output string) string {
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "SSID :") {
			return NormalizedSSID(strings.TrimPrefix(trimmed, "SSID :"))
		}
	}
	return ""
}

// SSIDFromSystemProfilerJSON reads the interface's current network name.
func SSIDFromSystemProfilerJSON(output string, iface string) string {
	var payload struct {
		Items []struct {
			Interfaces []struct {
				Name    string `json:"_name"`
				Current struct {
					Name string `json:"_name"`
				} `json:"spairport_current_network_information"`
			} `json:"spairport_airport_interfaces"`
		} `json:"SPAirPortDataType"`
	}
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		return ""
	}
	for _, item := range payload.Items {
		for _, entry := range item.Interfaces {
			if entry.Name == iface {
				return NormalizedSSID(entry.Current.Name)
			}
		}
	}
	return ""
}

// NormalizedSSID trims and drops the "<redacted>" placeholder.
func NormalizedSSID(value string) string {
	ssid := strings.TrimSpace(value)
	if ssid == "" || ssid == "<redacted>" {
		return ""
	}
	return ssid
}

func matchesAny(value string, patterns []string) bool {
	for _, pattern := range patterns {
		re, err := regexp.Compile("(?i)" + pattern)
		if err != nil {
			continue
		}
		if re.MatchString(value) {
			return true
		}
	}
	return false
}

// RunCommand runs a command with a five-second timeout and returns stdout, or
// "" on any failure.
func RunCommand(args []string) string {
	if len(args) == 0 {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return ""
	}
	return out.String()
}

// PreflightAppIngest checks that the app host accepts a TCP connection, so a
// run does not spend its selection work and then fail on the first upload.
func PreflightAppIngest(baseURL string, timeout time.Duration) NetworkDecision {
	raw := strings.TrimSpace(baseURL)
	if raw == "" {
		return NetworkDecision{false, "PDW_API_URL (or MCP_BASE_URL) is not set"}
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return NetworkDecision{false, "invalid ingest base URL: " + raw}
	}
	port := parsed.Port()
	if port == "" {
		if parsed.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(parsed.Hostname(), port), timeout)
	if err != nil {
		return NetworkDecision{false, "app ingest preflight failed: " + err.Error()}
	}
	conn.Close()
	return NetworkDecision{true, "app ingest preflight succeeded"}
}

// BeforeUploadCheck combines the network policy and the app preflight into
// the single guard every runner takes: "" means proceed, otherwise the reason
// to skip this run.
func BeforeUploadCheck(policy *NetworkPolicy, baseURL string, preflightTimeout time.Duration) func() string {
	return func() string {
		if decision := policy.Check(); !decision.Allowed {
			return decision.Reason
		}
		if decision := PreflightAppIngest(baseURL, preflightTimeout); !decision.Allowed {
			return decision.Reason
		}
		return ""
	}
}

// PreflightTimeout reads <prefix>_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS (default 5s).
func PreflightTimeout(getenv Getenv, prefix string) time.Duration {
	raw := strings.TrimSpace(getenv(prefix + "_UPLOAD_PREFLIGHT_TIMEOUT_SECONDS"))
	if raw == "" {
		return 5 * time.Second
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 {
		return 5 * time.Second
	}
	return time.Duration(seconds * float64(time.Second))
}

// NetworkDiagnostics renders the `--network-diagnostics` report.
func NetworkDiagnostics(policy *NetworkPolicy) string {
	var b strings.Builder
	context := policy.Context()
	decision := policy.Check()
	verdict := "blocked"
	if decision.Allowed {
		verdict = "allowed"
	}
	if context == nil {
		fmt.Fprintf(&b, "Network: no default route\nDecision: %s (%s)\n", verdict, decision.Reason)
		return b.String()
	}
	fmt.Fprintf(&b, "Default interface: %s\nHardware port: %s\n", context.Interface, context.HardwarePort)
	if strings.ToLower(context.HardwarePort) == "wi-fi" {
		probe := policy.SSIDProbe(context.Interface)
		ssid := probe.SSID
		if ssid == "" {
			ssid = "<unavailable>"
		}
		fmt.Fprintf(&b, "Wi-Fi SSID: %s\nSSID source: %s\n", ssid, probe.Source)
		if probe.Detail != "" {
			fmt.Fprintf(&b, "SSID detail: %s\n", probe.Detail)
		}
	}
	fmt.Fprintf(&b, "Decision: %s (%s)\n", verdict, decision.Reason)
	return b.String()
}
