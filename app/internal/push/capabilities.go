package push

import (
	"sort"
	"strings"
)

// CapabilityAndroidNativeAlerts is reported by an Android build whose native
// PdwMessagingService (mobile/plugins/with-messaging-notifications.js) draws
// every data-only push: `android_style` as a chat notification, `android_alert`
// as an ordinary one. Only such a device may be sent an alert as data-only: an
// older build receives the same push and shows nothing at all. It replaced
// android_messaging_style, whose builds draw chats only and so must keep
// getting ordinary alerts; that stored value now normalizes away.
const CapabilityAndroidNativeAlerts = "android_native_alerts"

// AndroidMessagesChannelID is the channel the native renderer posts chat
// alerts on ("Messages"); the app also creates it in mobile/src/lib/push.ts.
const AndroidMessagesChannelID = "messages"

var knownCapabilities = map[string]bool{CapabilityAndroidNativeAlerts: true}

// DrawsAlertsNatively reports whether a device must be sent alerts data-only,
// given its platform and stored capability list. It is the one gate both Expo
// senders use.
func DrawsAlertsNatively(platform, capabilities string) bool {
	return platform == "android" && HasCapability(capabilities, CapabilityAndroidNativeAlerts)
}

// NormalizeCapabilities keeps the capabilities this server knows, once each,
// sorted, so the stored column is a stable comma-separated list.
func NormalizeCapabilities(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if knownCapabilities[value] && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

// ParseCapabilities reads the stored comma-separated column.
func ParseCapabilities(stored string) []string {
	return NormalizeCapabilities(strings.Split(stored, ","))
}

// HasCapability reports whether a stored capability list includes one.
func HasCapability(stored, capability string) bool {
	for _, value := range ParseCapabilities(stored) {
		if value == capability {
			return true
		}
	}
	return false
}
