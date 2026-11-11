package mutations

// DeriveRequestStatus is a request's status from every one of its
// mutations' statuses. It is the one ladder both languages apply: Python's
// derive_request_status (postgres.py) and this, held equal by
// tests/contracts/request_status_ladder.json. A rejected, withdrawn or
// cancelled step is not live work; a request whose every step is one of those
// takes that status (rejected when they differ). current is returned for a
// request with no mutations or with statuses the ladder does not know.
func DeriveRequestStatus(statuses []string, current string) string {
	if len(statuses) == 0 {
		return current
	}
	var active []string
	for _, status := range statuses {
		switch status {
		case "rejected", "withdrawn", StatusCancelled:
		default:
			active = append(active, status)
		}
	}
	all := func(set []string, want ...string) bool {
		for _, s := range set {
			ok := false
			for _, w := range want {
				if s == w {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		}
		return true
	}
	if len(active) == 0 {
		switch {
		case all(statuses, "withdrawn"):
			return "withdrawn"
		case all(statuses, StatusCancelled):
			return StatusCancelled
		}
		return "rejected"
	}
	for _, status := range []string{"pending_review", "executing", "approved", "failed_retryable", "blocked_missing_credentials", "failed_terminal"} {
		for _, s := range active {
			if s == status {
				return status
			}
		}
	}
	switch {
	case all(active, "observed"):
		return "observed"
	case all(active, "succeeded", "observed"):
		return "succeeded"
	case all(active, StatusSkippedPrecondition):
		return StatusSkippedPrecondition
	case all(active, "succeeded", "observed", StatusSkippedPrecondition):
		return "succeeded"
	}
	return current
}
