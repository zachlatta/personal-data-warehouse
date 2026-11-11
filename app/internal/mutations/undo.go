package mutations

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Undo puts the thermostat back. A thermostat change that ran records the
// device as it was (result.previous, written by the worker before it wrote);
// undoing proposes the inverse as a new request and approves it at once on the
// reviewer's behalf, so it is a human approval with the reviewer as actor. Only
// a reviewer principal reaches the route (api.go), and an undo is a change the
// policy did not make, so it pauses the policy like any hand change.

// undoableStatuses are the request statuses whose changes have landed.
var undoableStatuses = map[string]bool{"succeeded": true, "observed": true}

// undoMutations returns the inverse of each thermostat change in request, or
// an error naming why the request cannot be undone.
func undoMutations(request Request) ([]map[string]any, error) {
	if !undoableStatuses[request.Status] {
		return nil, fmt.Errorf("only a request that ran can be undone; this one is %s", request.Status)
	}
	var inverse []map[string]any
	for _, mutation := range request.Mutations {
		if mutation.Operation != ResideoSetThermostatOperation {
			return nil, fmt.Errorf("only thermostat changes can be undone; this request also has %s", mutation.Operation)
		}
		if mutation.Status != "succeeded" && mutation.Status != "observed" {
			continue
		}
		previous, ok := mutation.Result["previous"].(map[string]any)
		if !ok {
			// already_set: nothing changed, so there is nothing to put back.
			continue
		}
		change, ok := thermostatChangeFromPayload(mutation.Payload)
		if !ok {
			return nil, errors.New("a thermostat change in this request is unreadable")
		}
		entry := map[string]any{
			"type":      ResideoSetThermostatOperation,
			"device_id": change.DeviceID,
			"hold":      holdFromSetpointStatus(stringFromAny(previous["setpoint_status"])),
			"reason":    "Undo of " + request.ID,
		}
		if change.Heat != nil {
			value, ok := numberValue(previous["heat_setpoint"])
			if !ok {
				return nil, errors.New("the previous heat setpoint was not recorded")
			}
			entry["heat_setpoint"] = value
		}
		if change.Cool != nil {
			value, ok := numberValue(previous["cool_setpoint"])
			if !ok {
				return nil, errors.New("the previous cool setpoint was not recorded")
			}
			entry["cool_setpoint"] = value
		}
		inverse = append(inverse, entry)
	}
	if len(inverse) == 0 {
		return nil, errors.New("nothing in this request changed the thermostat, so there is nothing to undo")
	}
	return inverse, nil
}

// canUndo reports whether the review should offer Undo.
func canUndo(request Request) bool {
	if len(request.Mutations) == 0 || len(request.Mutations) < request.MutationCount {
		return false
	}
	_, err := undoMutations(request)
	return err == nil
}

func holdFromSetpointStatus(status string) string {
	if strings.EqualFold(status, "PermanentHold") {
		return thermostatHoldPermanent
	}
	return thermostatHoldTemporary
}

// UndoRequest proposes the inverse of a request that ran and approves it as
// actor. It returns the new request.
func (s *Service) UndoRequest(ctx context.Context, requestID, actor string) (Request, error) {
	original, err := s.store.GetRequest(ctx, requestID, AllMutations)
	if err != nil {
		return Request{}, err
	}
	inverse, err := undoMutations(original)
	if err != nil {
		return Request{}, invalidProposalInput(err)
	}
	mutations := make([]MutationInput, 0, len(inverse))
	for index, raw := range inverse {
		mutation, err := mutationInputFromMap(raw, index)
		if err != nil {
			return Request{}, invalidProposalInput(err)
		}
		mutations = append(mutations, mutation)
	}
	proposal, err := s.createRequestNotifying(ctx, CreateRequestInput{
		Title:              "Undo: " + original.Title,
		Reason:             "Zach undid " + original.ID + " from the review.",
		Context:            map[string]any{"undo_of": original.ID},
		Mutations:          mutations,
		RequestedBy:        actor,
		RequestedByTokenID: requestTokenID(ctx),
	}, false)
	if err != nil {
		return Request{}, err
	}
	if proposal.Status != StatusPendingReview {
		return s.store.GetRequest(ctx, proposal.RequestID, AllMutations)
	}
	if _, err := s.store.ApproveRequest(ctx, proposal.RequestID, actor); err != nil {
		return Request{}, err
	}
	return s.store.GetRequest(ctx, proposal.RequestID, AllMutations)
}
