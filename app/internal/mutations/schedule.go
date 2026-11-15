package mutations

// Approve now, run later. Any mutation entry may carry not_before (the worker
// does not claim it earlier), not_after (it fails "approval expired" if it has
// not run by then) and a precondition the worker checks right before running
// it (a failed one ends the mutation as skipped_precondition, and Zach is
// told). An approved request whose mutations have not been claimed can be
// cancelled by a reviewer or by the token that proposed it: cancelling only
// ever stops something.

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	StatusCancelled           = "cancelled"
	StatusSkippedPrecondition = "skipped_precondition"

	// PreconditionNotHome skips a step when phone location in the last
	// within_minutes puts the phone at home (or there is no location at all:
	// missing data never sends the house to away). Home is the deployment's
	// PDW_HOME_LATITUDE / PDW_HOME_LONGITUDE (never in Git); with none set the
	// step is skipped.
	PreconditionNotHome = "not_home"

	// maxScheduleAhead bounds not_before: a plan is days ahead, not months.
	maxScheduleAhead = 60 * 24 * time.Hour
)

// scheduleKeys are lifted off every mutation entry before its type parses
// it, so no type has to know them.
var scheduleKeys = []string{"not_before", "not_after", "precondition"}

// MutationSchedule is one mutation's timing and precondition. Zero times mean
// none.
type MutationSchedule struct {
	NotBefore    time.Time
	NotAfter     time.Time
	Precondition map[string]any
}

// IsZero reports whether the schedule sets nothing.
func (s MutationSchedule) IsZero() bool {
	return s.NotBefore.IsZero() && s.NotAfter.IsZero() && len(s.Precondition) == 0
}

// scheduleFromMap reads and removes the schedule keys of a raw entry.
func scheduleFromMap(raw map[string]any) (MutationSchedule, map[string]any, error) {
	rest := make(map[string]any, len(raw))
	for key, value := range raw {
		rest[key] = value
	}
	var schedule MutationSchedule
	for _, key := range []string{"not_before", "not_after"} {
		value, ok := rest[key]
		delete(rest, key)
		if !ok || value == nil || strings.TrimSpace(stringFromAny(value)) == "" {
			continue
		}
		at, err := time.Parse(time.RFC3339, strings.TrimSpace(stringFromAny(value)))
		if err != nil {
			return schedule, nil, fmt.Errorf("%s must be an RFC 3339 time with a zone, like 2026-12-04T07:30:00-05:00", key)
		}
		if key == "not_before" {
			schedule.NotBefore = at.UTC()
		} else {
			schedule.NotAfter = at.UTC()
		}
	}
	if value, ok := rest["precondition"]; ok {
		delete(rest, "precondition")
		if value != nil {
			precondition, ok := value.(map[string]any)
			if !ok {
				return schedule, nil, errors.New("precondition must be an object")
			}
			schedule.Precondition = precondition
		}
	}
	return schedule, rest, nil
}

// preconditionOperations are the types whose executor checks a precondition
// right before it runs: the cloud worker's thermostat path
// (process_upstream_mutation_batch → upstream_mutation_schedule). Device
// workers never check one, so a precondition elsewhere is refused at
// proposal instead of being ignored when the step runs.
var preconditionOperations = map[string]bool{ResideoSetThermostatOperation: true}

// validateSchedule checks one mutation's schedule at proposal time.
func validateSchedule(operation string, schedule MutationSchedule, now time.Time) error {
	if !schedule.NotBefore.IsZero() && schedule.NotBefore.After(now.Add(maxScheduleAhead)) {
		return fmt.Errorf("not_before may be at most %d days ahead", int(maxScheduleAhead.Hours()/24))
	}
	if !schedule.NotAfter.IsZero() {
		if !schedule.NotAfter.After(now) {
			return errors.New("not_after is already past")
		}
		if !schedule.NotBefore.IsZero() && !schedule.NotAfter.After(schedule.NotBefore) {
			return errors.New("not_after must be after not_before")
		}
	}
	if len(schedule.Precondition) > 0 {
		if !preconditionOperations[operation] {
			return fmt.Errorf("takes no precondition: only %s checks one before it runs, so on %s it would be ignored", ResideoSetThermostatOperation, operation)
		}
		kind := stringFromAny(schedule.Precondition["kind"])
		switch kind {
		case PreconditionNotHome:
			for key := range schedule.Precondition {
				if key != "kind" && key != "within_minutes" {
					return fmt.Errorf("precondition %s takes kind and within_minutes, not %s", kind, key)
				}
			}
			minutes, ok := numberValue(schedule.Precondition["within_minutes"])
			if !ok || minutes < 5 || minutes > 24*60 {
				return errors.New("precondition not_home needs within_minutes from 5 to 1440")
			}
		default:
			return fmt.Errorf("precondition kind must be %q, not %q", PreconditionNotHome, kind)
		}
	}
	return nil
}

func scheduleTime(at time.Time) time.Time {
	if at.IsZero() {
		return time.Unix(0, 0).UTC()
	}
	return at.UTC()
}
