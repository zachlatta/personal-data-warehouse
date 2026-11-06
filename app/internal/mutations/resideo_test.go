package mutations

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func thermostatInput(raw map[string]any) MutationInput {
	raw["type"] = ResideoSetThermostatOperation
	if _, ok := raw["device_id"]; !ok {
		raw["device_id"] = "AABBCCDDEEFF"
	}
	if _, ok := raw["reason"]; !ok {
		raw["reason"] = "warming up"
	}
	return MutationInput{
		Type: ResideoSetThermostatOperation, Account: ResideoAccount,
		Reason: stringFromAny(raw["reason"]), Raw: raw,
	}
}

func TestThermostatChangeValidation(t *testing.T) {
	refused := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{"no setpoint", map[string]any{}, "heat_setpoint or cool_setpoint"},
		{"no device", map[string]any{"device_id": "", "heat_setpoint": 68.0}, "device_id"},
		{"no reason", map[string]any{"reason": "", "heat_setpoint": 68.0}, "reason"},
		{"too hot", map[string]any{"heat_setpoint": 680.0}, "heat_setpoint must be"},
		{"too cold", map[string]any{"cool_setpoint": 10.0}, "cool_setpoint must be"},
		{"a third of a degree", map[string]any{"heat_setpoint": 68.3}, "half degrees"},
		{"not a number", map[string]any{"heat_setpoint": "warm"}, "heat_setpoint must be"},
		{"crossed", map[string]any{"heat_setpoint": 75.0, "cool_setpoint": 70.0}, "below cool_setpoint"},
		{"bad hold", map[string]any{"heat_setpoint": 68.0, "hold": "forever"}, "hold must be"},
		{"misspelled", map[string]any{"heat_setpoints": 68.0}, "unknown field"},
		{"mode is not this mutation", map[string]any{"heat_setpoint": 68.0, "mode": "Off"}, "unknown field"},
	}
	for _, test := range refused {
		t.Run(test.name, func(t *testing.T) {
			_, err := thermostatChangeFromInput(thermostatInput(test.raw))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want it to mention %q", err, test.want)
			}
		})
	}
}

func TestThermostatChangeDefaultsToATemporaryHold(t *testing.T) {
	change, err := thermostatChangeFromInput(thermostatInput(map[string]any{"heat_setpoint": 68.5}))
	if err != nil {
		t.Fatal(err)
	}
	if change.Hold != thermostatHoldTemporary || *change.Heat != 68.5 || change.Cool != nil {
		t.Fatalf("change = %+v", change)
	}
	if got := thermostatChangeTitle(change); got != "Set the thermostat: heat to 68.5° (temporary hold)" {
		t.Fatalf("title = %q", got)
	}
	payload := thermostatChangePayload(change)
	if _, hasCool := payload["cool_setpoint"]; hasCool {
		t.Fatalf("a setpoint nobody named must stay out of the payload: %v", payload)
	}
	again, ok := thermostatChangeFromPayload(payload)
	if !ok || *again.Heat != 68.5 || again.Hold != thermostatHoldTemporary {
		t.Fatalf("round trip = %+v, %v", again, ok)
	}
}

func TestThermostatChangeWithinTheDevicesLimits(t *testing.T) {
	row := thermostatRow{MinHeat: 50, MaxHeat: 90, MinCool: 50, MaxCool: 90}
	low := 45.0
	if err := thermostatChangeWithinLimits(thermostatChange{Heat: &low}, row); err == nil || !strings.Contains(err.Error(), "50 to 90") {
		t.Fatalf("err = %v", err)
	}
	fine := 68.0
	if err := thermostatChangeWithinLimits(thermostatChange{Heat: &fine, Cool: &fine}, row); err != nil {
		t.Fatal(err)
	}
}

func TestThermostatPreviewWarnsWhatTheReviewerCannotSeeOtherwise(t *testing.T) {
	now := time.Date(2026, 11, 6, 20, 0, 0, 0, time.UTC)
	low, normal := 50.0, 68.0
	warnings := func(change thermostatChange, row *thermostatRow) string {
		list, _ := thermostatPreview(change, row, now)["warnings"].([]string)
		return strings.Join(list, " | ")
	}
	fresh := thermostatRow{IsOnline: 1, Mode: "Heat", LastPolledAt: now.Add(-time.Minute)}

	if got := warnings(thermostatChange{Heat: &normal}, &fresh); got != "" {
		t.Fatalf("an ordinary change warned: %q", got)
	}
	cases := []struct {
		name   string
		change thermostatChange
		mutate func(*thermostatRow)
		want   string
	}{
		{"frozen pipes", thermostatChange{Heat: &low}, func(*thermostatRow) {}, "frozen pipes"},
		{"system off", thermostatChange{Heat: &normal}, func(r *thermostatRow) { r.Mode = "Off" }, "system is off"},
		{"wrong mode", thermostatChange{Heat: &normal}, func(r *thermostatRow) { r.Mode = "Cool" }, "cool mode"},
		{"cool in heat mode", thermostatChange{Cool: &normal}, func(*thermostatRow) {}, "heat mode"},
		{"offline", thermostatChange{Heat: &normal}, func(r *thermostatRow) { r.IsOnline = 0 }, "offline"},
		{"stale", thermostatChange{Heat: &normal}, func(r *thermostatRow) { r.LastPolledAt = now.Add(-time.Hour) }, "old"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			row := fresh
			test.mutate(&row)
			if got := warnings(test.change, &row); !strings.Contains(got, test.want) {
				t.Fatalf("warnings = %q, want %q", got, test.want)
			}
		})
	}
}

func TestThermostatPreviewSaysAHoldLastsWhenNoScheduleIsOn(t *testing.T) {
	heat := 68.0
	now := time.Now()
	row := thermostatRow{IsOnline: 1, Mode: "Heat", LastPolledAt: now}
	effect, _ := thermostatPreview(thermostatChange{Heat: &heat, Hold: thermostatHoldTemporary}, &row, now)["effect"].(string)
	if !strings.Contains(effect, "no schedule") {
		t.Fatalf("effect = %q", effect)
	}
	row.ScheduleEnabled = 1
	effect, _ = thermostatPreview(thermostatChange{Heat: &heat, Hold: thermostatHoldTemporary}, &row, now)["effect"].(string)
	if !strings.Contains(effect, "next scheduled period") {
		t.Fatalf("effect = %q", effect)
	}
}

func TestThermostatPreviewShowsAnUnreadableStateAsUnknown(t *testing.T) {
	heat := 68.0
	preview := thermostatPreview(thermostatChange{Heat: &heat}, nil, time.Now())
	if preview["current"] != "unknown" {
		t.Fatalf("current = %v", preview["current"])
	}
	list, _ := preview["warnings"].([]string)
	if len(list) != 1 || !strings.Contains(list[0], "could not be read") {
		t.Fatalf("warnings = %v", list)
	}
}

func TestThermostatProposalReviewApproval(t *testing.T) {
	url := os.Getenv("PDW_RESIDEO_TEST_URL")
	if url == "" {
		t.Skip("run through tests/test_resideo.py")
	}
	store, err := NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := NewService(store, Config{})
	server := newAPIServer(t, store)
	ctx := context.Background()

	for _, test := range []struct {
		entry map[string]any
		want  string
	}{
		{map[string]any{"device_id": "NOSUCHDEVICE", "heat_setpoint": 68}, "base_resideo.thermostats"},
		{map[string]any{"device_id": "AABBCCDDEEFF", "heat_setpoint": 45}, "50 to 90"},
	} {
		test.entry["type"] = ResideoSetThermostatOperation
		test.entry["reason"] = "isolated test"
		_, err := service.ProposeMutation(ctx, ProposeMutationInput{Title: "Set", Reason: "isolated test", Mutations: []map[string]any{test.entry}})
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("proposal %#v: err = %v, want %q", test.entry, err, test.want)
		}
	}

	proposal, err := service.ProposeMutation(ctx, ProposeMutationInput{
		Title: "Warm the house", Reason: "isolated test",
		Mutations: []map[string]any{{
			"type": ResideoSetThermostatOperation, "device_id": "AABBCCDDEEFF",
			"heat_setpoint": 68, "reason": "warming up",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execContext(ctx, store.db, "UPDATE @resideo_thermostats SET indoor_temperature = 64"); err != nil {
		t.Fatal(err)
	}
	request, err := store.GetRequest(ctx, proposal.RequestID, AllMutations)
	if err != nil {
		t.Fatal(err)
	}
	preview := mapFromAny(request.Mutations[0].Preview["thermostat"])
	current := mapFromAny(preview["current"])
	// 64, not the 70 at proposal time: the review reads the thermostat as it is now.
	if current["indoor_temperature"] != 64.0 || current["heat_setpoint"] != 66.0 || current["system_mode"] != "Heat" {
		t.Fatalf("preview current = %#v", current)
	}
	if request.Mutations[0].Account != ResideoAccount || preview["heat_setpoint"] != 68.0 || preview["hold"] != thermostatHoldTemporary {
		t.Fatalf("mutation = %#v", request.Mutations[0])
	}

	response, err := http.Post(server.URL+"/api/mutations/requests/"+request.ID+"/approve", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if body := decodeBody(t, response); response.StatusCode != http.StatusOK {
		t.Fatalf("approve: status %d: %v", response.StatusCode, body)
	}
}
