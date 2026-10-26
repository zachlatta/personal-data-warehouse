package mutations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// screen_timelapse.purge_moment deletes one captured moment of a device's
// screen from the warehouse: its screenshots (one display or all of them),
// the moment's accessibility tree, the agent readings of those frames, the
// screen_capture timeline event, and each stored object in Drive. It exists
// for a frame that caught something that must not be kept (a card number on
// screen). The executor is the cloud worker
// (src/personal_data_warehouse/screen_timelapse_purge.py): rows first, in one
// transaction, then the objects, retried until Drive reports each one gone.
//
// The proposal is refused when the moment matches nothing, and its preview
// carries storage ids and counts only: the request is itself a timeline event,
// so nothing the frame showed may be copied into it.

var screenTimelapsePurgeKeys = map[string]bool{
	"type": true, "operation": true, "account": true, "title": true, "reason": true,
	"device": true, "captured_at": true, "display_index": true,
}

// screenTimelapsePurgeTarget is the moment a purge names. DisplayIndex 0
// means every display of the moment.
type screenTimelapsePurgeTarget struct {
	Device       string
	CapturedAt   time.Time
	DisplayIndex int
}

func isScreenTimelapsePurgeInput(mutation MutationInput) bool {
	return strings.TrimSpace(mutation.Type) == ScreenTimelapsePurgeMomentOperation
}

func screenTimelapsePurgeTargetFromInput(mutation MutationInput) (screenTimelapsePurgeTarget, error) {
	raw := mutation.Raw
	var unknown []string
	for key := range raw {
		if !screenTimelapsePurgeKeys[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return screenTimelapsePurgeTarget{}, fmt.Errorf("has unknown field(s) %s; %s takes device, captured_at, display_index and reason", strings.Join(unknown, ", "), ScreenTimelapsePurgeMomentOperation)
	}
	device := strings.TrimSpace(stringFromAny(raw["device"]))
	if device == "" {
		return screenTimelapsePurgeTarget{}, errors.New("must include device (base_screen_timelapse.screenshots.device)")
	}
	capturedText := strings.TrimSpace(stringFromAny(raw["captured_at"]))
	if capturedText == "" {
		return screenTimelapsePurgeTarget{}, errors.New("must include captured_at, the moment's exact base_screen_timelapse.screenshots.captured_at as an RFC 3339 timestamp")
	}
	capturedAt, err := time.Parse(time.RFC3339Nano, capturedText)
	if err != nil {
		return screenTimelapsePurgeTarget{}, fmt.Errorf("captured_at %q is not an RFC 3339 timestamp such as 2026-10-25T14:03:00Z", capturedText)
	}
	display, err := screenTimelapseDisplayIndex(raw["display_index"])
	if err != nil {
		return screenTimelapsePurgeTarget{}, err
	}
	if strings.TrimSpace(mutation.Reason) == "" {
		return screenTimelapsePurgeTarget{}, errors.New("must include reason: why this moment is deleted (say what kind of thing it showed, never the thing itself)")
	}
	return screenTimelapsePurgeTarget{Device: device, CapturedAt: capturedAt.UTC(), DisplayIndex: display}, nil
}

func screenTimelapseDisplayIndex(value any) (int, error) {
	invalid := errors.New("display_index must be a whole number of at least 1 (omit it to purge every display of the moment)")
	switch typed := value.(type) {
	case nil:
		return 0, nil
	case float64:
		if typed < 1 || typed != math.Trunc(typed) || typed > math.MaxInt32 {
			return 0, invalid
		}
		return int(typed), nil
	case int:
		if typed < 1 {
			return 0, invalid
		}
		return typed, nil
	case int64:
		if typed < 1 || typed > math.MaxInt32 {
			return 0, invalid
		}
		return int(typed), nil
	case json.Number:
		parsed, err := strconv.Atoi(typed.String())
		if err != nil || parsed < 1 {
			return 0, invalid
		}
		return parsed, nil
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil || parsed < 1 {
			return 0, invalid
		}
		return parsed, nil
	default:
		return 0, invalid
	}
}

func validateScreenTimelapsePurge(mutation MutationInput) error {
	_, err := screenTimelapsePurgeTargetFromInput(mutation)
	return err
}

func screenTimelapsePurgePayload(target screenTimelapsePurgeTarget, reason string) map[string]any {
	payload := map[string]any{
		"device":      target.Device,
		"captured_at": target.CapturedAt.Format(time.RFC3339Nano),
		"reason":      reason,
	}
	if target.DisplayIndex > 0 {
		payload["display_index"] = target.DisplayIndex
	}
	return payload
}

func screenTimelapsePurgeTitle(target screenTimelapsePurgeTarget) string {
	displays := "every display"
	if target.DisplayIndex > 0 {
		displays = fmt.Sprintf("display %d", target.DisplayIndex)
	}
	return fmt.Sprintf("Delete the %s screen capture at %s (%s)", target.Device, target.CapturedAt.Format(time.RFC3339Nano), displays)
}

func screenTimelapsePurgeTargetFromPayload(payload map[string]any) (screenTimelapsePurgeTarget, bool) {
	device := stringFromAny(payload["device"])
	capturedAt, err := time.Parse(time.RFC3339Nano, stringFromAny(payload["captured_at"]))
	if device == "" || err != nil {
		return screenTimelapsePurgeTarget{}, false
	}
	display, err := screenTimelapseDisplayIndex(payload["display_index"])
	if err != nil {
		return screenTimelapsePurgeTarget{}, false
	}
	return screenTimelapsePurgeTarget{Device: device, CapturedAt: capturedAt.UTC(), DisplayIndex: display}, true
}

// screenTimelapseCaptureEventID is the screen_capture timeline adapter's
// event_id for a moment (timeline.py _SCREEN_CAPTURE): device|second, UTC.
func screenTimelapseCaptureEventID(target screenTimelapsePurgeTarget) string {
	return target.Device + "|" + target.CapturedAt.UTC().Format("2006-01-02T15:04:05Z")
}

// screenTimelapseMoment is what the warehouse holds for a purge's moment:
// storage ids and sizes, never text.
type screenTimelapseMoment struct {
	Screenshots       []map[string]any
	Tree              map[string]any
	Descriptions      int64
	SameContentFrames int64
}

func (s *PostgresStore) enrichScreenTimelapsePurges(ctx context.Context, mutations []storedMutation) ([]storedMutation, error) {
	for index := range mutations {
		if mutations[index].Operation != ScreenTimelapsePurgeMomentOperation {
			continue
		}
		target, ok := screenTimelapsePurgeTargetFromPayload(mutations[index].Payload)
		if !ok {
			return nil, invalidProposalInput(fmt.Errorf("mutation %d has an unreadable %s payload", index, ScreenTimelapsePurgeMomentOperation))
		}
		if s == nil || s.db == nil {
			return nil, errors.New("cannot check the screen capture to delete: the warehouse is not connected")
		}
		moment, err := s.screenTimelapseMoment(ctx, target)
		if err != nil {
			return nil, fmt.Errorf("read the screen capture to delete: %w", err)
		}
		if err := screenTimelapsePurgeMatches(target, moment); err != nil {
			return nil, invalidProposalInput(fmt.Errorf("mutation %d %w", index, err))
		}
		mutations[index].Preview = map[string]any{"screen_timelapse_purge": screenTimelapsePurgePreview(target, moment)}
	}
	return mutations, nil
}

// screenTimelapsePurgeMatches refuses a purge of nothing: a moment with no
// screenshot and no tree, or a display the moment does not have.
func screenTimelapsePurgeMatches(target screenTimelapsePurgeTarget, moment screenTimelapseMoment) error {
	when := target.CapturedAt.Format(time.RFC3339Nano)
	if len(moment.Screenshots) == 0 && target.DisplayIndex > 0 {
		return fmt.Errorf("matches no screenshot: base_screen_timelapse.screenshots has no display %d for device %q at %s", target.DisplayIndex, target.Device, when)
	}
	if len(moment.Screenshots) == 0 && moment.Tree == nil {
		return fmt.Errorf("matches nothing: base_screen_timelapse.screenshots and .accessibility_trees have no row for device %q at exactly %s (copy captured_at from the row)", target.Device, when)
	}
	return nil
}

func screenTimelapsePurgePreview(target screenTimelapsePurgeTarget, moment screenTimelapseMoment) map[string]any {
	removesEvent := false
	for _, shot := range moment.Screenshots {
		if int64FromAny(shot["display_index"]) == 1 {
			removesEvent = true
		}
	}
	preview := map[string]any{
		"device":              target.Device,
		"captured_at":         target.CapturedAt.Format(time.RFC3339Nano),
		"displays":            "all",
		"screenshots":         moment.Screenshots,
		"descriptions":        moment.Descriptions,
		"removes_timeline":    removesEvent,
		"same_content_frames": moment.SameContentFrames,
		"effect": "Deletes these rows from base_screen_timelapse.screenshots, the moment's base_screen_timelapse.accessibility_trees row and its derived_screen_timelapse.descriptions readings, " +
			"then each stored file in Drive. Files on the device itself and pgBackRest backups on slowking are not touched.",
	}
	if target.DisplayIndex > 0 {
		preview["displays"] = target.DisplayIndex
	}
	if moment.Tree != nil {
		preview["accessibility_tree"] = moment.Tree
	}
	if removesEvent {
		eventID := screenTimelapseCaptureEventID(target)
		preview["timeline_event_id"] = eventID
		preview["timeline_path"] = "/timeline/screen_capture/" + url.PathEscape(eventID)
	}
	return preview
}

func (s *PostgresStore) screenTimelapseMoment(ctx context.Context, target screenTimelapsePurgeTarget) (screenTimelapseMoment, error) {
	var shots, tree string
	moment := screenTimelapseMoment{}
	err := queryRowContext(ctx, s.db, `
		WITH shots AS (
			SELECT display_index, content_sha256, size_bytes, storage_backend, storage_file_id
			FROM @screen_timelapse_screenshots
			WHERE device = $1 AND captured_at = $2 AND ($3 = 0 OR display_index = $3)
		)
		SELECT
			COALESCE((SELECT jsonb_agg(jsonb_build_object(
				'display_index', display_index, 'content_sha256', content_sha256, 'size_bytes', size_bytes,
				'storage_backend', storage_backend, 'storage_file_id', storage_file_id) ORDER BY display_index)
				FROM shots), '[]'::jsonb)::text,
			COALESCE((SELECT jsonb_build_object(
				'content_sha256', content_sha256, 'size_bytes', size_bytes,
				'storage_backend', storage_backend, 'storage_file_id', storage_file_id)
				FROM @screen_timelapse_accessibility_trees
				WHERE device = $1 AND captured_at = $2), 'null'::jsonb)::text,
			(SELECT count(*) FROM @screen_timelapse_descriptions
				WHERE device = $1 AND captured_at = $2 AND ($3 = 0 OR display_index = $3)),
			-- Identical bytes elsewhere show the same thing: the agent can
			-- propose those moments too.
			(SELECT count(*) FROM @screen_timelapse_screenshots other
				WHERE other.content_sha256 IN (SELECT content_sha256 FROM shots WHERE content_sha256 <> '')
				  AND NOT (other.device = $1 AND other.captured_at = $2 AND ($3 = 0 OR other.display_index = $3)))
	`, target.Device, target.CapturedAt, target.DisplayIndex).Scan(&shots, &tree, &moment.Descriptions, &moment.SameContentFrames)
	if err != nil {
		return moment, err
	}
	if err := json.Unmarshal([]byte(shots), &moment.Screenshots); err != nil {
		return moment, err
	}
	if tree != "null" {
		if err := json.Unmarshal([]byte(tree), &moment.Tree); err != nil {
			return moment, err
		}
	}
	return moment, nil
}
