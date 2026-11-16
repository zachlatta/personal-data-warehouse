package server

import (
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/notifications"
	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
)

// A policy outcome is an outbox row; the timeline renderer draws it as the
// alert the phone acts on: a quiet Undo alert when a change ran.
func TestAPolicyOutcomeRowRendersAsAQuietUndoAlert(t *testing.T) {
	row := mutations.PolicyOutcomeAlert(mutations.PolicyOutcome{
		Kind:     mutations.PolicyOutcomeApplied,
		Request:  mutations.Request{ID: "req-1", Title: "Set the thermostat: heat to 55° (permanent hold)", Reason: "You left home at 8:12 AM."},
		PolicyID: "heating-winter",
	})
	alert := renderTimelineNotification(row, "https://pdw.example", timelineLinkEnv{})
	if alert.Category != push.CategoryMutationApplied || alert.Kind != "mutation_applied" || alert.RequestID != "req-1" || !alert.Quiet {
		t.Fatalf("alert %+v", alert)
	}
	if alert.Route != "/mutations/req-1" || !strings.Contains(alert.Body, "Undo") || alert.Title == "" {
		t.Fatalf("alert %+v", alert)
	}
	found := false
	for _, category := range push.Categories() {
		if category.ID == push.CategoryMutationApplied {
			for _, action := range category.Actions {
				found = found || action.ID == push.ActionUndo
			}
		}
	}
	if !found {
		t.Fatal("the mutation_applied category must offer Undo")
	}
}

func TestOtherPolicyOutcomeRowsRenderAsOrdinaryAlerts(t *testing.T) {
	request := mutations.Request{ID: "req-2", Title: "Heat", Error: "auto-approval was switched off before this ran"}
	sentBack := renderTimelineNotification(mutations.PolicyOutcomeAlert(mutations.PolicyOutcome{Kind: mutations.PolicyOutcomeSentBack, Request: request}), "", timelineLinkEnv{})
	if sentBack.Category != push.CategoryMutationReview || sentBack.Quiet || !strings.Contains(sentBack.Body, "switched off") {
		t.Fatalf("sent back: %+v", sentBack)
	}
	off := renderTimelineNotification(mutations.PolicyOutcomeAlert(mutations.PolicyOutcome{Kind: mutations.PolicyOutcomePolicyDisabled, Request: request, PolicyID: "heating-winter", Detail: "turned off after 3 failed runs in a row"}), "", timelineLinkEnv{})
	if !strings.Contains(off.Title, "heating-winter") || !strings.Contains(off.Body, "3 failed runs") || off.Category != push.CategoryLink {
		t.Fatalf("policy off: %+v", off)
	}
}

// The outbox's exemption from the timeline switch keys on the adapter the
// outcome loop writes; the two names are one name.
func TestTheOutboxExemptsTheAdapterTheOutcomeLoopWrites(t *testing.T) {
	if notifications.MutationAdapter != mutations.PolicyOutcomeAdapter {
		t.Fatalf("%q != %q", notifications.MutationAdapter, mutations.PolicyOutcomeAdapter)
	}
}
