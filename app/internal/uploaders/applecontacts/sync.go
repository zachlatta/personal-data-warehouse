package applecontacts

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// StateEntry is one recorded card.
type StateEntry struct {
	SourceID    string
	ContactID   string
	Fingerprint string
	DisplayName string
	IsDeleted   bool
	Complete    bool
}

// State is the per-card upload state.
type State struct {
	db *common.StateDB
}

// DefaultStateFile is the state path.
func DefaultStateFile() string {
	return common.DefaultStateFile("apple-contacts-upload-state.sqlite")
}

// OpenState opens the state for (account, store path).
func OpenState(path, account, storePath string) (*State, error) {
	db, err := common.OpenStateDB(path, []string{
		`CREATE TABLE IF NOT EXISTS upload_state (
			source_id TEXT NOT NULL,
			contact_id TEXT NOT NULL,
			fingerprint TEXT NOT NULL DEFAULT '',
			display_name TEXT NOT NULL DEFAULT '',
			is_deleted INTEGER NOT NULL DEFAULT 0,
			complete INTEGER NOT NULL DEFAULT 0,
			last_success_at TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (source_id, contact_id)
		)`,
	}, map[string]string{"schema_version": "1", "account": account, "store_path": storePath}, []string{"upload_state"})
	if err != nil {
		return nil, err
	}
	return &State{db: db}, nil
}

// Close releases the database.
func (s *State) Close() error { return s.db.Close() }

// Entries lists every recorded card.
func (s *State) Entries() ([]StateEntry, error) {
	rows, err := common.Query(s.db.DB, "SELECT source_id, contact_id, fingerprint, display_name, is_deleted, complete FROM upload_state ORDER BY source_id, contact_id")
	if err != nil {
		return nil, err
	}
	var out []StateEntry
	for _, row := range rows {
		out = append(out, StateEntry{
			SourceID: row.String("source_id"), ContactID: row.String("contact_id"), Fingerprint: row.String("fingerprint"),
			DisplayName: row.String("display_name"), IsDeleted: row.Bool("is_deleted"), Complete: row.Bool("complete"),
		})
	}
	return out, nil
}

// IsComplete reports whether the card was uploaded with this fingerprint.
func (s *State) IsComplete(sourceID, contactID, fingerprint string) (bool, error) {
	rows, err := common.Query(s.db.DB, "SELECT fingerprint, complete FROM upload_state WHERE source_id = ? AND contact_id = ?", sourceID, contactID)
	if err != nil {
		return false, err
	}
	return len(rows) > 0 && rows[0].Bool("complete") && rows[0].String("fingerprint") == fingerprint, nil
}

// MarkSuccess records an uploaded card.
func (s *State) MarkSuccess(sourceID, contactID, fingerprint, displayName string, isDeleted bool, now time.Time) error {
	deleted := 0
	if isDeleted {
		deleted = 1
	}
	_, err := s.db.DB.Exec(`INSERT INTO upload_state (source_id, contact_id, fingerprint, display_name, is_deleted, complete, last_success_at)
		VALUES (?, ?, ?, ?, ?, 1, ?)
		ON CONFLICT(source_id, contact_id) DO UPDATE SET
			fingerprint = excluded.fingerprint, display_name = excluded.display_name,
			is_deleted = excluded.is_deleted, complete = 1, last_success_at = excluded.last_success_at`,
		sourceID, contactID, fingerprint, displayName, deleted, common.ISOFormat(now))
	return err
}

// Summary is the run's counts.
type Summary struct {
	ContactsSeen     int
	ContactsSelected int
	ContactsSkipped  int
	ContactsDeleted  int
	ContactsDeferred int
	BatchesUploaded  int
}

// Uploader is what the runner needs from the ingest client.
type Uploader interface {
	UploadAppleContactsBatch(gzipBytes []byte, exportedAt string) (ingestclient.StoredObject, error)
}

// Runner uploads changed cards and tombstones.
type Runner struct {
	Account           string
	StorePath         string
	Client            Uploader
	Logger            common.Logger
	State             *State
	Now               func() time.Time
	Mode              string
	Limit             int
	BeforeUploadCheck func() string
}

type selectedContact struct {
	payload     map[string]any
	fingerprint string
}

// Sync runs one pass.
func (r *Runner) Sync() (Summary, error) {
	if r.Client == nil {
		return Summary{}, errors.New("ingest client is required")
	}
	if r.Mode == "" {
		r.Mode = "incremental"
	}
	if r.Mode != "incremental" && r.Mode != "full" {
		return Summary{}, errors.New("mode must be 'full' or 'incremental'")
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
	contacts, err := r.scan()
	if err != nil {
		return Summary{}, err
	}
	currentKeys := map[[2]string]bool{}
	var selected []selectedContact
	skipped := 0
	for _, contact := range contacts {
		currentKeys[[2]string{contact.SourceID, contact.ContactID}] = true
		payload := ContactPayload(contact)
		fingerprint := common.JSONSHA256(payload)
		if r.Mode == "incremental" && r.State != nil {
			complete, err := r.State.IsComplete(contact.SourceID, contact.ContactID, fingerprint)
			if err != nil {
				return Summary{}, err
			}
			if complete {
				skipped++
				continue
			}
		}
		selected = append(selected, selectedContact{payload, fingerprint})
	}
	var tombstones []selectedContact
	if r.State != nil {
		entries, err := r.State.Entries()
		if err != nil {
			return Summary{}, err
		}
		for _, entry := range entries {
			if currentKeys[[2]string{entry.SourceID, entry.ContactID}] || entry.IsDeleted {
				continue
			}
			payload := TombstonePayload(entry.SourceID, entry.ContactID, entry.DisplayName, r.Now())
			tombstones = append(tombstones, selectedContact{payload, common.JSONSHA256(payload)})
		}
	}
	selected = append(selected, tombstones...)

	deferred := 0
	if r.Limit > 0 && len(selected) > r.Limit {
		deferred = len(selected) - r.Limit
		selected = selected[:r.Limit]
	}
	summary := Summary{ContactsSeen: len(contacts), ContactsSelected: len(selected), ContactsSkipped: skipped, ContactsDeleted: len(tombstones), ContactsDeferred: deferred}
	if len(selected) > 0 && r.BeforeUploadCheck != nil {
		if reason := r.BeforeUploadCheck(); reason != "" {
			r.Logger.Warningf("Skipping Apple Contacts upload: %s", reason)
			summary.ContactsDeferred = len(selected) + deferred
			return summary, nil
		}
	}
	exportedAt := r.Now()
	if len(selected) > 0 {
		records := make([]map[string]any, 0, len(selected))
		for _, item := range selected {
			records = append(records, Envelope(r.Account, exportedAt, item.payload))
		}
		body, err := common.GzipJSONL(records)
		if err != nil {
			return summary, err
		}
		stored, err := r.Client.UploadAppleContactsBatch(body, common.ISOFormat(exportedAt))
		if err != nil {
			return summary, err
		}
		summary.BatchesUploaded = 1
		r.Logger.Infof("Uploaded Apple Contacts batch %s with %d records", stored.StorageKey, len(records))
		if r.State != nil {
			for _, item := range selected {
				payload := item.payload
				isDeleted, _ := payload["is_deleted"].(bool)
				if err := r.State.MarkSuccess(common.PyStr(payload["source_id"]), common.PyStr(payload["contact_id"]), item.fingerprint, common.PyStr(payload["display_name"]), isDeleted, exportedAt); err != nil {
					return summary, err
				}
			}
		}
	}
	return summary, nil
}

func (r *Runner) scan() ([]Contact, error) {
	stores, err := DiscoverStores(r.StorePath)
	if err != nil {
		return nil, err
	}
	tempDir, err := os.MkdirTemp("", "pdw-apple-contacts-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempDir)
	var contacts []Contact
	for index, store := range stores {
		r.Logger.Infof("Snapshotting Apple Contacts store at %s", store.Path)
		snapshot := filepath.Join(tempDir, fmt.Sprintf("%d-%s.abcddb", index, store.SourceID))
		if err := Snapshot(store.Path, snapshot); err != nil {
			return nil, err
		}
		scanned, err := Scan(snapshot, store.SourceID)
		if err != nil {
			return nil, err
		}
		contacts = append(contacts, scanned...)
	}
	sort.SliceStable(contacts, func(i, j int) bool {
		if contacts[i].SourceID != contacts[j].SourceID {
			return contacts[i].SourceID < contacts[j].SourceID
		}
		return contacts[i].ContactID < contacts[j].ContactID
	})
	return contacts, nil
}

func listValue(items []map[string]any) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

// ContactPayload is the record shipped for one card; its canonical JSON is
// the fingerprint.
func ContactPayload(contact Contact) map[string]any {
	dates := contact.Dates
	if dates == nil {
		dates = map[string]any{}
	}
	raw := contact.Raw
	if raw == nil {
		raw = map[string]any{}
	}
	return map[string]any{
		"source_id":         contact.SourceID,
		"contact_id":        contact.ContactID,
		"source_uid":        contact.SourceUID,
		"display_name":      contact.DisplayName,
		"given_name":        contact.GivenName,
		"middle_name":       contact.MiddleName,
		"family_name":       contact.FamilyName,
		"nickname":          contact.Nickname,
		"organization":      contact.Organization,
		"department":        contact.Department,
		"job_title":         contact.JobTitle,
		"primary_email":     contact.PrimaryEmail,
		"primary_phone":     contact.PrimaryPhone,
		"emails":            listValue(contact.Emails),
		"phones":            listValue(contact.Phones),
		"addresses":         listValue(contact.Addresses),
		"organizations":     listValue(contact.Organizations),
		"urls":              listValue(contact.URLs),
		"nicknames":         listValue(contact.Nicknames),
		"groups":            listValue(contact.Groups),
		"dates":             dates,
		"photos":            listValue(contact.Photos),
		"notes":             contact.Note,
		"is_deleted":        false,
		"created_at":        common.ISOFormat(contact.CreatedAt),
		"source_updated_at": common.ISOFormat(contact.ModifiedAt),
		"raw":               raw,
	}
}

// TombstonePayload marks a card that left the address book.
func TombstonePayload(sourceID, contactID, displayName string, deletedAt time.Time) map[string]any {
	return map[string]any{
		"source_id":         sourceID,
		"contact_id":        contactID,
		"source_uid":        contactID,
		"display_name":      displayName,
		"given_name":        "",
		"middle_name":       "",
		"family_name":       "",
		"nickname":          "",
		"organization":      "",
		"department":        "",
		"job_title":         "",
		"primary_email":     "",
		"primary_phone":     "",
		"emails":            []any{},
		"phones":            []any{},
		"addresses":         []any{},
		"organizations":     []any{},
		"urls":              []any{},
		"nicknames":         []any{},
		"groups":            []any{},
		"dates":             map[string]any{},
		"photos":            []any{},
		"notes":             "",
		"is_deleted":        true,
		"created_at":        "1970-01-01T00:00:00+00:00",
		"source_updated_at": common.ISOFormat(deletedAt),
		"raw":               map[string]any{"deleted": true},
	}
}

// Envelope wraps one record for the batch.
func Envelope(account string, exportedAt time.Time, record map[string]any) map[string]any {
	return map[string]any{
		"schema_version": int64(1),
		"source":         "apple_contacts",
		"account":        account,
		"exported_at":    common.ISOFormat(exportedAt),
		"record_type":    "contact",
		"record":         record,
	}
}
