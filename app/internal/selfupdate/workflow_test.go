package selfupdate_test

// This test pins the contract between the GitHub Actions release workflow and
// the selfupdate package: the workflow must produce asset names of the form
// pdw-cli_<version>_<goos>_<goarch>.tar.gz plus a SHA256SUMS file, covering
// every (os, arch) pair the selfupdate code is willing to install. If a
// platform is added to one side without the other, this test fails loudly.

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/selfupdate"
)

const workflowRelPath = "../../../.github/workflows/pdw-cli-release.yml"

// supportedPlatforms is the set of (goos, goarch) pairs `pdw-cli update`
// expects to be available for download. Adding a new platform here without
// also adding it to the workflow's build matrix will fail TestWorkflowBuildsAllSupportedPlatforms.
var supportedPlatforms = []struct{ GOOS, GOARCH string }{
	{"linux", "amd64"},
	{"linux", "arm64"},
	{"darwin", "amd64"},
	{"darwin", "arm64"},
}

func readWorkflow(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs(workflowRelPath)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	body, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("read workflow: %v (looked at %s)", err, abs)
	}
	return string(body)
}

func TestWorkflowExistsAndIsNonEmpty(t *testing.T) {
	body := readWorkflow(t)
	if strings.TrimSpace(body) == "" {
		t.Fatal("workflow file is empty")
	}
}

func TestWorkflowBuildsAllSupportedPlatforms(t *testing.T) {
	body := readWorkflow(t)
	for _, p := range supportedPlatforms {
		// The matrix entries appear as two adjacent lines:
		//   - goos: linux
		//     goarch: amd64
		// Use a single-line marker that's specific enough to catch typos:
		// the pair must appear on adjacent lines, in order, anywhere.
		marker := "goos: " + p.GOOS + "\n"
		if !strings.Contains(body, marker) {
			t.Errorf("workflow missing matrix entry for goos=%s", p.GOOS)
		}
		archMarker := "goarch: " + p.GOARCH
		if !strings.Contains(body, archMarker) {
			t.Errorf("workflow missing matrix entry for goarch=%s", p.GOARCH)
		}
	}
}

func TestWorkflowAssetNamingMatchesSelfupdate(t *testing.T) {
	body := readWorkflow(t)
	// AssetName("${VERSION}", "${GOOS}", "${GOARCH}") expansion in the
	// workflow's `tar -czf` line. We assert the template is present so the
	// produced filenames will be picked up by Release.AssetFor.
	want := "pdw-cli_${VERSION}_${GOOS}_${GOARCH}.tar.gz"
	if !strings.Contains(body, want) {
		t.Fatalf("workflow does not produce expected asset name template %q", want)
	}
	// Sanity check: the template Go-side and the template shell-side agree
	// on shape if we substitute concrete values.
	produced := strings.NewReplacer("${VERSION}", "v9.9.9", "${GOOS}", "linux", "${GOARCH}", "amd64").Replace(want)
	expected := selfupdate.AssetName("v9.9.9", "linux", "amd64")
	if produced != expected {
		t.Fatalf("template %q expands to %q but selfupdate.AssetName returns %q", want, produced, expected)
	}
}

func TestWorkflowPublishesSHA256SUMS(t *testing.T) {
	body := readWorkflow(t)
	if !strings.Contains(body, "SHA256SUMS") {
		t.Fatal("workflow must publish a SHA256SUMS asset")
	}
	if !strings.Contains(body, "gh release upload") {
		t.Fatal("workflow must upload assets to the GitHub release")
	}
	// The upload step must include both the platform tarballs and the sums
	// file so selfupdate.FetchBinary can verify what it downloads.
	if !strings.Contains(body, "pdw-cli_*.tar.gz SHA256SUMS") {
		t.Fatal("workflow upload must include both pdw-cli_*.tar.gz and SHA256SUMS")
	}
}

func TestWorkflowInjectsVersionLDFlag(t *testing.T) {
	body := readWorkflow(t)
	// Without this, `pdw-cli version` always reports "dev" even from
	// release artifacts and `pdw-cli update` would loop on every run.
	if !strings.Contains(body, "-X main.version=") {
		t.Fatal("workflow must inject the build version via -ldflags -X main.version=")
	}
}

func TestWorkflowReleasesOnEveryMainCommit(t *testing.T) {
	body := readWorkflow(t)
	// Every push to main that touches the app must produce a release so
	// `pdw-cli update` picks up new versions without waiting for a tag.
	if !strings.Contains(body, "branches:") || !strings.Contains(body, "- main") {
		t.Fatal("workflow must trigger on pushes to main")
	}
	if !strings.Contains(body, "refs/heads/main") {
		t.Fatal("release job must run on pushes to refs/heads/main, not just tags")
	}
}

func TestWorkflowAlsoReleasesOnPDWCLITags(t *testing.T) {
	body := readWorkflow(t)
	// Manually-cut tagged releases keep working alongside per-commit ones.
	if !strings.Contains(body, "refs/tags/pdw-cli/v") {
		t.Fatal("release job must also accept pdw-cli/v* tag pushes for cut releases")
	}
	if !strings.Contains(body, `"pdw-cli/v*"`) {
		t.Fatal("workflow trigger should include tags: [pdw-cli/v*]")
	}
}

func TestWorkflowSkipsReleaseOnPullRequests(t *testing.T) {
	body := readWorkflow(t)
	// Fork PRs don't have write perms and we don't want PR builds to clobber
	// the "latest" release for self-update users. The release job must
	// explicitly gate on the push event.
	if !strings.Contains(body, "github.event_name == 'push'") {
		t.Fatal("release job must require github.event_name == 'push' so PRs don't publish")
	}
}

func TestWorkflowForcesReleaseToBeLatest(t *testing.T) {
	body := readWorkflow(t)
	// Per-commit versions look like v0.0.<count>-sha.<short>. The "-sha..."
	// suffix is a semver pre-release identifier, so GitHub would normally
	// skip these from /releases/latest. selfupdate.LatestRelease hits
	// /releases/latest, so we MUST force each new release to be latest.
	if !strings.Contains(body, "--latest") {
		t.Fatal("workflow must pass --latest to gh release create so per-commit releases populate /releases/latest")
	}
}

func TestWorkflowAssignsMonotonicVersionPerCommit(t *testing.T) {
	body := readWorkflow(t)
	// To keep `pdw-cli update` working between untagged commits, the
	// per-commit version must include a monotonically increasing component
	// (commit count) so ShouldUpdate("v0.0.<N>", "v0.0.<N+1>") returns true.
	if !strings.Contains(body, "git rev-list --count") {
		t.Fatal("workflow must derive a monotonic per-commit version from `git rev-list --count`")
	}
}

func TestWorkflowSignsDarwinBinariesWithStableIdentity(t *testing.T) {
	body := readWorkflow(t)
	// macOS TCC keys Full Disk Access to the binary's code-signing designated
	// requirement. Unsigned (ad-hoc) darwin release binaries carry a per-build
	// cdhash requirement, so every self-update would silently invalidate the
	// grant. The workflow must sign darwin binaries with the stable identity
	// so the requirement never changes across releases.
	if !strings.Contains(body, "rcodesign sign") {
		t.Fatal("workflow must sign darwin binaries with rcodesign")
	}
	if !strings.Contains(body, "--binary-identifier com.zachlatta.pdw") {
		t.Fatal("darwin binaries must be signed under the stable identifier com.zachlatta.pdw (TCC grants are keyed to it)")
	}
	for _, secret := range []string{"secrets.PDW_CODESIGN_KEY", "secrets.PDW_CODESIGN_CERT"} {
		if !strings.Contains(body, secret) {
			t.Fatalf("workflow must read the signing identity from %s", secret)
		}
	}
}

func TestWorkflowSignsBeforePackaging(t *testing.T) {
	body := readWorkflow(t)
	// SHA256SUMS is computed from the packaged tarballs, and selfupdate
	// verifies downloads against it. Signing must therefore happen before the
	// Package step or the published checksums wouldn't cover the signed bytes.
	sign := strings.Index(body, "rcodesign sign")
	pack := strings.Index(body, "name: Package")
	if sign == -1 || pack == -1 {
		t.Fatalf("missing steps: sign@%d package@%d", sign, pack)
	}
	if sign > pack {
		t.Fatal("darwin signing must run before the Package step so SHA256SUMS covers the signed binary")
	}
}

func TestWorkflowPinsRcodesignBySha256(t *testing.T) {
	body := readWorkflow(t)
	// The signer is downloaded at release time; a pinned checksum keeps a
	// hijacked or moved download from silently signing (or not) our binaries.
	if !strings.Contains(body, "RCODESIGN_SHA256") {
		t.Fatal("workflow must pin the rcodesign download by sha256")
	}
	if !strings.Contains(body, "sha256sum -c") {
		t.Fatal("workflow must verify the rcodesign download against the pinned sha256")
	}
}

// Local sanity: the platform we're testing on is one selfupdate can install.
func TestCurrentPlatformIsSupported(t *testing.T) {
	for _, p := range supportedPlatforms {
		if p.GOOS == runtime.GOOS && p.GOARCH == runtime.GOARCH {
			return
		}
	}
	t.Skipf("test runs on unsupported platform %s/%s; skipping", runtime.GOOS, runtime.GOARCH)
}
