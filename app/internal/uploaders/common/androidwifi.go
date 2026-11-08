package common

import (
	"os"
	"os/exec"
	"strings"
)

// AndroidDumpsys is Android's dumpsys, whose `connectivity` report names the
// active default network and its transport.
const AndroidDumpsys = "/system/bin/dumpsys"

// CheckAndroidWiFi reads `dumpsys connectivity` output and returns a non-empty
// reason when the active default network is not Wi-Fi. A VPN (Tailscale) is
// not the default network there; the transport under it is. strict decides
// what an output it cannot read means: false lets the upload go (the screen
// timelapse's opt-in switch), true defers it (an uploader that must never use
// cellular data).
func CheckAndroidWiFi(dumpsysOutput string, strict bool) string {
	var netID string
	for _, line := range strings.Split(dumpsysOutput, "\n") {
		if idx := strings.Index(line, "Active default network:"); idx >= 0 {
			fields := strings.Fields(line[idx+len("Active default network:"):])
			if len(fields) == 0 || fields[0] == "none" || fields[0] == "null" {
				return "device has no active default network (Wi-Fi only)"
			}
			netID = "network{" + fields[0] + "}"
			break
		}
	}
	if netID == "" {
		if strict {
			return "cannot tell the device's default network from dumpsys (Wi-Fi only)"
		}
		return ""
	}
	marker := "NetworkAgentInfo{" + netID
	for _, line := range strings.Split(dumpsysOutput, "\n") {
		if strings.Contains(line, marker) {
			if strings.Contains(line, "ni{WIFI") {
				return ""
			}
			return "device is not on Wi-Fi (Wi-Fi only)"
		}
	}
	return "device default network transport is not Wi-Fi (Wi-Fi only)"
}

// AndroidWiFiGuard runs `dumpsys connectivity` and checks it with
// CheckAndroidWiFi. Off Android (no dumpsys) it allows the upload; on Android
// a dumpsys that fails reads as "cannot tell", which strict defers.
func AndroidWiFiGuard(strict bool) string {
	if info, err := os.Stat(AndroidDumpsys); err != nil || info.Mode()&0o111 == 0 {
		return ""
	}
	out, err := exec.Command(AndroidDumpsys, "connectivity").Output()
	if err != nil {
		return CheckAndroidWiFi("", strict)
	}
	return CheckAndroidWiFi(string(out), strict)
}
