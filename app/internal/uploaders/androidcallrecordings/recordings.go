// Package androidcallrecordings uploads finished BCR audio/JSON pairs without deleting them.
package androidcallrecordings

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const DefaultRecorderDir = "/storage/emulated/0/Recordings/Calls"
const CompletedState = 3

type Recording struct {
	UUID, Account, Title, Path, StoredPath, Extension, ContentType string
	CreatedAt, ModifiedAt, MetadataModifiedAt                      time.Time
	DurationMS, SizeBytes, State                                   int64
	FileExists                                                     bool
	BCR                                                            json.RawMessage
	Fields                                                         map[string]any
}

// ReadRecordings reads sidecars only; absence after the reaper runs is never a tombstone.
func ReadRecordings(dir, scratch string) ([]Recording, error) {
	return readRecordings(dir, time.Now().UTC(), DefaultSettleAge)
}

func readRecordings(dir string, now time.Time, settleAge time.Duration) ([]Recording, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result []Recording
	for _, e := range entries {
		ext := strings.ToLower(filepath.Ext(e.Name()))
		mime := map[string]string{".oga": "audio/ogg", ".ogg": "audio/ogg", ".opus": "audio/ogg", ".m4a": "audio/mp4", ".flac": "audio/flac", ".wav": "audio/wav", ".amr": "audio/amr"}[ext]
		if e.IsDir() || mime == "" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		stem := strings.TrimSuffix(e.Name(), filepath.Ext(e.Name()))
		metaPath := filepath.Join(dir, stem+".json")
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat audio failed")
		}
		mi, err := os.Stat(metaPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stat sidecar failed")
		}
		// Do not parse a sidecar BCR is still writing. Keep the pair visible
		// to the runner so it schedules its settle retry even without another write.
		if now.Sub(info.ModTime()) < settleAge || now.Sub(mi.ModTime()) < settleAge {
			result = append(result, Recording{UUID: stem, Path: path, State: CompletedState,
				FileExists: true, SizeBytes: info.Size(), ModifiedAt: info.ModTime(), MetadataModifiedAt: mi.ModTime()})
			continue
		}
		raw, err := os.ReadFile(metaPath)
		if err != nil {
			return nil, fmt.Errorf("read sidecar failed")
		}
		var b struct {
			Timestamp   int64            `json:"timestamp_unix_ms"`
			Direction   *string          `json:"direction"`
			SIM         *int             `json:"sim_slot"`
			CallLogName *string          `json:"call_log_name"`
			Parties     []map[string]any `json:"calls"`
			Output      struct {
				Recording map[string]any `json:"recording"`
				Format    any            `json:"format"`
			} `json:"output"`
		}
		if json.Unmarshal(raw, &b) != nil || b.Timestamp <= 0 {
			return nil, fmt.Errorf("invalid BCR metadata timestamp")
		}
		fields := map[string]any{"direction": b.Direction, "sim_slot": b.SIM, "call_log_name": b.CallLogName, "format": b.Output.Format}
		numbers, contacts, callers := []string{}, []string{}, []string{}
		for _, party := range b.Parties {
			for _, key := range []string{"phone_number_e164", "phone_number", "phone_number_formatted"} {
				if v, ok := party[key].(string); ok && v != "" {
					numbers = append(numbers, v)
					break
				}
			}
			if v, ok := party["contact_name"].(string); ok && v != "" {
				contacts = append(contacts, v)
			}
			if v, ok := party["caller_name"].(string); ok && v != "" {
				callers = append(callers, v)
			}
		}
		fields["phone_numbers"], fields["contact_names"], fields["caller_names"] = numbers, contacts, callers
		duration := float64(0)
		for _, key := range []string{"duration_secs_encoded", "duration_secs_total"} {
			if v, ok := b.Output.Recording[key].(float64); ok {
				duration = v
				break
			}
		}
		wall := duration
		if v, ok := b.Output.Recording["duration_secs_wall"].(float64); ok {
			wall = v
		}
		fields["ended_at"] = time.UnixMilli(b.Timestamp).UTC().Add(time.Duration(wall * 1e9)).Format(time.RFC3339Nano)
		title := "Call"
		if len(contacts) > 0 {
			title = "Call with " + strings.Join(contacts, ", ")
		} else if len(numbers) > 0 {
			title = "Call with " + strings.Join(numbers, ", ")
		}
		result = append(result, Recording{UUID: stem, Title: title, Path: path, StoredPath: path, Extension: ext, ContentType: mime, CreatedAt: time.UnixMilli(b.Timestamp).UTC(), ModifiedAt: info.ModTime().UTC(), MetadataModifiedAt: mi.ModTime().UTC(), DurationMS: int64(duration * 1000), SizeBytes: info.Size(), State: CompletedState, FileExists: true, BCR: json.RawMessage(raw), Fields: fields})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result, nil
}
