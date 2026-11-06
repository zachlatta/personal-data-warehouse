package mutations

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// resideo.set_thermostat changes a setpoint on Zach's home thermostat. The
// executor is the cloud worker (src/personal_data_warehouse/resideo_mutations.py),
// which re-reads the device before writing and reads it back after.
//
// The review shows the thermostat as it is now, so the current state is resolved
// on every read (hydrateThermostatPreviews) rather than kept from the proposal:
// a request that waits overnight must not show yesterday's temperature.

const (
	thermostatHoldTemporary = "temporary"
	thermostatHoldPermanent = "permanent"
	// A typo guard for proposals; the thermostat's own limits are checked against
	// the warehouse here and against the device by the worker.
	minThermostatDegrees = 40.0
	maxThermostatDegrees = 95.0
	// Below this an empty house in winter risks frozen pipes.
	lowHeatWarningDegrees = 55.0
	// Polls are five minutes apart, so older than this means the poller stopped.
	staleThermostatAfter = 30 * time.Minute
)

var thermostatChangeKeys = map[string]bool{
	"type": true, "operation": true, "account": true, "title": true, "reason": true,
	"device_id": true, "heat_setpoint": true, "cool_setpoint": true, "hold": true,
}

// thermostatChange is a validated request. A nil setpoint is left alone.
type thermostatChange struct {
	DeviceID string
	Heat     *float64
	Cool     *float64
	Hold     string
}

func thermostatChangeFromInput(mutation MutationInput) (thermostatChange, error) {
	raw := mutation.Raw
	var unknown []string
	for key := range raw {
		if !thermostatChangeKeys[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return thermostatChange{}, fmt.Errorf("has unknown field(s) %s; %s takes device_id, heat_setpoint, cool_setpoint, hold and reason", strings.Join(unknown, ", "), ResideoSetThermostatOperation)
	}
	if account := normalizeAccount(mutation.Account); account != ResideoAccount {
		return thermostatChange{}, fmt.Errorf("account must be %q or omitted, not %q", ResideoAccount, account)
	}
	change := thermostatChange{DeviceID: strings.TrimSpace(stringFromAny(raw["device_id"]))}
	if change.DeviceID == "" {
		return thermostatChange{}, errors.New("must include device_id (base_resideo.thermostats.device_id)")
	}
	var err error
	if change.Heat, err = thermostatDegrees(raw["heat_setpoint"], "heat_setpoint"); err != nil {
		return thermostatChange{}, err
	}
	if change.Cool, err = thermostatDegrees(raw["cool_setpoint"], "cool_setpoint"); err != nil {
		return thermostatChange{}, err
	}
	if change.Heat == nil && change.Cool == nil {
		return thermostatChange{}, errors.New("must include heat_setpoint or cool_setpoint")
	}
	if change.Heat != nil && change.Cool != nil && *change.Heat >= *change.Cool {
		return thermostatChange{}, errors.New("heat_setpoint must be below cool_setpoint")
	}
	change.Hold = strings.TrimSpace(stringFromAny(raw["hold"]))
	if change.Hold == "" {
		change.Hold = thermostatHoldTemporary
	}
	if change.Hold != thermostatHoldTemporary && change.Hold != thermostatHoldPermanent {
		return thermostatChange{}, fmt.Errorf("hold must be %s or %s, not %q", thermostatHoldTemporary, thermostatHoldPermanent, change.Hold)
	}
	if strings.TrimSpace(mutation.Reason) == "" {
		return thermostatChange{}, errors.New("must include reason: why the temperature is changing")
	}
	return change, nil
}

// thermostatDegrees reads an optional setpoint in whole or half degrees.
func thermostatDegrees(value any, field string) (*float64, error) {
	invalid := fmt.Errorf("%s must be a number of degrees from %g to %g, in whole or half degrees", field, minThermostatDegrees, maxThermostatDegrees)
	var degrees float64
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case float64:
		degrees = typed
	case int:
		degrees = float64(typed)
	case int64:
		degrees = float64(typed)
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return nil, invalid
		}
		degrees = parsed
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return nil, invalid
		}
		degrees = parsed
	default:
		return nil, invalid
	}
	if degrees < minThermostatDegrees || degrees > maxThermostatDegrees || degrees*2 != math.Trunc(degrees*2) {
		return nil, invalid
	}
	return &degrees, nil
}

func thermostatChangePayload(change thermostatChange) map[string]any {
	payload := map[string]any{"device_id": change.DeviceID, "hold": change.Hold}
	if change.Heat != nil {
		payload["heat_setpoint"] = *change.Heat
	}
	if change.Cool != nil {
		payload["cool_setpoint"] = *change.Cool
	}
	return payload
}

func thermostatChangeFromPayload(payload map[string]any) (thermostatChange, bool) {
	change := thermostatChange{DeviceID: stringFromAny(payload["device_id"]), Hold: stringFromAny(payload["hold"])}
	heat, heatErr := thermostatDegrees(payload["heat_setpoint"], "heat_setpoint")
	cool, coolErr := thermostatDegrees(payload["cool_setpoint"], "cool_setpoint")
	if change.DeviceID == "" || heatErr != nil || coolErr != nil {
		return thermostatChange{}, false
	}
	change.Heat, change.Cool = heat, cool
	return change, true
}

func thermostatChangeTitle(change thermostatChange) string {
	var parts []string
	if change.Heat != nil {
		parts = append(parts, fmt.Sprintf("heat to %g°", *change.Heat))
	}
	if change.Cool != nil {
		parts = append(parts, fmt.Sprintf("cool to %g°", *change.Cool))
	}
	return fmt.Sprintf("Set the thermostat: %s (%s hold)", strings.Join(parts, ", "), change.Hold)
}

// thermostatRow is what the warehouse holds for a thermostat now.
type thermostatRow struct {
	Name                     string
	IsOnline                 int64
	Indoor, Heat, Cool       float64
	MinHeat, MaxHeat         float64
	MinCool, MaxCool         float64
	Mode, Running, HoldState string
	Units                    string
	ScheduleEnabled          int64
	LastPolledAt             time.Time
}

func (s *PostgresStore) thermostatRow(ctx context.Context, deviceID string) (thermostatRow, bool, error) {
	var row thermostatRow
	err := queryRowContext(ctx, s.db, `
		SELECT name, is_online, indoor_temperature, heat_setpoint, cool_setpoint,
		       min_heat_setpoint, max_heat_setpoint, min_cool_setpoint, max_cool_setpoint,
		       system_mode, operation_mode, setpoint_status, temperature_units, schedule_enabled, last_polled_at
		FROM @resideo_thermostats
		WHERE account = $1 AND device_id = $2
	`, ResideoAccount, deviceID).Scan(&row.Name, &row.IsOnline, &row.Indoor, &row.Heat, &row.Cool,
		&row.MinHeat, &row.MaxHeat, &row.MinCool, &row.MaxCool,
		&row.Mode, &row.Running, &row.HoldState, &row.Units, &row.ScheduleEnabled, &row.LastPolledAt)
	if errors.Is(err, sql.ErrNoRows) {
		return row, false, nil
	}
	return row, err == nil, err
}

// enrichThermostatChanges refuses a thermostat the warehouse does not have and a
// value outside its limits, so neither reaches a reviewer.
func (s *PostgresStore) enrichThermostatChanges(ctx context.Context, mutations []storedMutation) ([]storedMutation, error) {
	for index := range mutations {
		if mutations[index].Operation != ResideoSetThermostatOperation {
			continue
		}
		change, ok := thermostatChangeFromPayload(mutations[index].Payload)
		if !ok {
			return nil, invalidProposalInput(fmt.Errorf("mutation %d has an unreadable %s payload", index, ResideoSetThermostatOperation))
		}
		if s == nil || s.db == nil {
			return nil, errors.New("cannot check the thermostat: the warehouse is not connected")
		}
		row, found, err := s.thermostatRow(ctx, change.DeviceID)
		if err != nil {
			return nil, fmt.Errorf("read the thermostat: %w", err)
		}
		if !found {
			return nil, invalidProposalInput(fmt.Errorf("mutation %d names device_id %q, which base_resideo.thermostats does not have", index, change.DeviceID))
		}
		if err := thermostatChangeWithinLimits(change, row); err != nil {
			return nil, invalidProposalInput(fmt.Errorf("mutation %d %w", index, err))
		}
		mutations[index].Preview = map[string]any{"thermostat": thermostatPreview(change, &row, time.Now())}
	}
	return mutations, nil
}

func thermostatChangeWithinLimits(change thermostatChange, row thermostatRow) error {
	if change.Heat != nil && (*change.Heat < row.MinHeat || *change.Heat > row.MaxHeat) {
		return fmt.Errorf("heat_setpoint %g is outside this thermostat's %g to %g", *change.Heat, row.MinHeat, row.MaxHeat)
	}
	if change.Cool != nil && (*change.Cool < row.MinCool || *change.Cool > row.MaxCool) {
		return fmt.Errorf("cool_setpoint %g is outside this thermostat's %g to %g", *change.Cool, row.MinCool, row.MaxCool)
	}
	return nil
}

// thermostatPreview is the review. A nil row means the warehouse could not be
// read, which must show as unknown rather than as anything reassuring.
func thermostatPreview(change thermostatChange, row *thermostatRow, now time.Time) map[string]any {
	preview := map[string]any{"device_id": change.DeviceID, "hold": change.Hold}
	if change.Hold == thermostatHoldPermanent {
		preview["effect"] = "Holds this setpoint until something changes it again, overriding any schedule."
	} else {
		preview["effect"] = "Holds this setpoint until the thermostat's next scheduled period, like pressing its arrows."
	}
	if change.Heat != nil {
		preview["heat_setpoint"] = *change.Heat
	}
	if change.Cool != nil {
		preview["cool_setpoint"] = *change.Cool
	}
	warnings := []string{}
	if change.Heat != nil && *change.Heat < lowHeatWarningDegrees {
		warnings = append(warnings, fmt.Sprintf("A heat setpoint of %g° is low enough to risk frozen pipes in winter.", *change.Heat))
	}
	if row == nil {
		preview["current"] = "unknown"
		preview["warnings"] = append(warnings, "The thermostat's current state could not be read.")
		return preview
	}
	if row.ScheduleEnabled == 0 {
		preview["effect"] = "Holds this setpoint until something changes it again: the thermostat has no schedule on."
	}
	preview["current"] = map[string]any{
		"name":               row.Name,
		"indoor_temperature": row.Indoor,
		"heat_setpoint":      row.Heat,
		"cool_setpoint":      row.Cool,
		"system_mode":        row.Mode,
		"running":            row.Running,
		"hold":               row.HoldState,
		"units":              row.Units,
		"as_of":              row.LastPolledAt.UTC().Format(time.RFC3339),
	}
	if row.IsOnline == 0 {
		warnings = append(warnings, "The thermostat was offline at the last poll; the worker retries for up to an hour after approval.")
	}
	if now.Sub(row.LastPolledAt) > staleThermostatAfter {
		warnings = append(warnings, "This state is old: the thermostat poller has not run lately.")
	}
	switch row.Mode {
	case "Off":
		warnings = append(warnings, "The system is off, so this setpoint does nothing until it is turned on.")
	case "Heat":
		if change.Heat == nil {
			warnings = append(warnings, "The system is in heat mode, so a cool setpoint does nothing.")
		}
	case "Cool":
		if change.Cool == nil {
			warnings = append(warnings, "The system is in cool mode, so a heat setpoint does nothing.")
		}
	}
	preview["warnings"] = warnings
	return preview
}

// hydrateThermostatPreviews rewrites each review with the thermostat as it is now.
func (s *PostgresStore) hydrateThermostatPreviews(ctx context.Context, mutations []Mutation) {
	for index := range mutations {
		if mutations[index].Operation != ResideoSetThermostatOperation {
			continue
		}
		change, ok := thermostatChangeFromPayload(mutations[index].Payload)
		if !ok {
			continue
		}
		var current *thermostatRow
		if s != nil && s.db != nil {
			if row, found, err := s.thermostatRow(ctx, change.DeviceID); err == nil && found {
				current = &row
			}
		}
		preview := make(map[string]any, len(mutations[index].Preview)+1)
		for key, value := range mutations[index].Preview {
			preview[key] = value
		}
		preview["thermostat"] = thermostatPreview(change, current, time.Now())
		mutations[index].Preview = preview
	}
}
