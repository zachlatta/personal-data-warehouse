package webapp

import (
	"encoding/json"
	"net/http"
)

// AssetLinksPath is where Android checks that the phone app may open this
// host's links itself (Android App Links). The app's intent filters
// (mobile/app.json) claim the SPA routes it has screens for; Android only
// honors them once this file names the app's package and signing key.
const AssetLinksPath = "/.well-known/assetlinks.json"

// AndroidPackage is the phone app's Android application id.
const AndroidPackage = "com.zachlatta.pdw"

// AndroidCertFingerprints are the SHA-256 digests of the keys the Android
// app is signed with: the EAS-managed release key for preview and production
// builds (`apksigner verify --print-certs` on the APK). A new signing key
// needs its digest here, or links on that build open in the browser again.
var AndroidCertFingerprints = []string{
	"0D:90:6C:F9:79:EF:FD:4F:CB:C2:42:AB:8E:6F:2F:97:BC:D4:41:F2:4D:36:44:23:ED:91:8C:74:A9:87:B6:A3",
}

type assetLinkTarget struct {
	Namespace              string   `json:"namespace"`
	PackageName            string   `json:"package_name"`
	SHA256CertFingerprints []string `json:"sha256_cert_fingerprints"`
}

type assetLink struct {
	Relation []string        `json:"relation"`
	Target   assetLinkTarget `json:"target"`
}

func serveAssetLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, _ := json.Marshal([]assetLink{{
		Relation: []string{"delegate_permission/common.handle_all_urls"},
		Target: assetLinkTarget{
			Namespace:              "android_app",
			PackageName:            AndroidPackage,
			SHA256CertFingerprints: AndroidCertFingerprints,
		},
	}})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_, _ = w.Write(body)
}
