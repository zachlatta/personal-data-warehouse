package buildinfo

import "os"

var gitSHA = "unknown"

// GitSHA returns the source commit for this build. A runtime env var wins so
// deploy platforms can inject their own commit value without rebuilding.
func GitSHA() string {
	for _, name := range []string{"PDW_GIT_SHA", "SOURCE_COMMIT", "GIT_SHA", "GIT_COMMIT", "COMMIT_SHA", "COOLIFY_GIT_COMMIT"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	if gitSHA != "" {
		return gitSHA
	}
	return "unknown"
}
