package screentimelapse

import (
	"net"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// On pichu the uploader runs on the phone, where /proc/net/route has no
// default route: the Mac tethering guard deferred every run from 2026-10-27
// ("no default network route") while 815 frames waited. On Android the guard
// asks only whether the app answers.
func TestAndroidGuardIgnoresTheMacNetworkPolicy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	saved := onAndroid
	defer func() { onAndroid = saved }()
	onAndroid = func() bool { return true }
	// A policy that blocks every interface would defer the run if it applied.
	env := common.Getenv(func(name string) string {
		if name == "SCREEN_TIMELAPSE_UPLOAD_BLOCKED_HARDWARE_PORT_PATTERNS" {
			return "."
		}
		return ""
	})
	if reason := networkGuard(env, "http://"+listener.Addr().String())(); reason != "" {
		t.Fatalf("reachable app deferred the upload on Android: %q", reason)
	}
}
