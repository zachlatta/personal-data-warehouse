package common

import "testing"

func TestCheckAndroidWiFi(t *testing.T) {
	wifiDump := `
Active default network: 104
  NetworkAgentInfo{network{104}  handle{449730285581}  ni{WIFI CONNECTED extra: "Home"}}
`
	if reason := CheckAndroidWiFi(wifiDump, true); reason != "" {
		t.Fatalf("CheckAndroidWiFi(wifi) = %q, want empty", reason)
	}

	lteDump := `
Active default network: 100
  NetworkAgentInfo{network{100}  handle{432550416397}  ni{MOBILE[LTE] CONNECTED}}
  NetworkAgentInfo{network{104}  handle{449730285581}  ni{WIFI DISCONNECTED}}
`
	if reason := CheckAndroidWiFi(lteDump, false); reason == "" {
		t.Fatal("CheckAndroidWiFi(lte) should defer when default network is MOBILE[LTE]")
	}

	noneDump := "Active default network: none\n"
	if reason := CheckAndroidWiFi(noneDump, false); reason == "" {
		t.Fatal("CheckAndroidWiFi(none) should defer when no default network is active")
	}

	if reason := CheckAndroidWiFi("garbage", false); reason != "" {
		t.Fatalf("lenient: an unreadable report must let the upload go, got %q", reason)
	}
	if reason := CheckAndroidWiFi("garbage", true); reason == "" {
		t.Fatal("strict: an unreadable report must defer")
	}
}
