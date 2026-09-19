package applecontacts

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/testfixtures"
)

// The golden file was produced by the Python scanner + contact_payload the Go
// port replaced (testdata/README.md). A byte-different payload means a
// different fingerprint, which means every card re-uploads on the first Go
// run, so this is the contract that matters most.
func TestScanMatchesPythonGoldens(t *testing.T) {
	goldens := testfixtures.LoadGoldens(t, filepath.Join("testdata", "golden.jsonl"))
	for _, store := range []string{"synthetic", "coredata"} {
		contacts, err := Scan(filepath.Join("testdata", store+".abcddb"), store+"-src")
		if err != nil {
			t.Fatalf("scan %s: %v", store, err)
		}
		want := goldens[store]
		if len(contacts) != len(want) {
			t.Fatalf("%s: scanned %d contacts, golden has %d", store, len(contacts), len(want))
		}
		for i, contact := range contacts {
			payload := ContactPayload(contact)
			encoded, err := common.CanonicalJSON(payload)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != want[i].Payload {
				t.Errorf("%s contact %d payload differs:\n got %s\nwant %s", store, i, encoded, want[i].Payload)
			}
			if got := common.JSONSHA256(payload); got != want[i].Fingerprint {
				t.Errorf("%s contact %d fingerprint %s != %s", store, i, got, want[i].Fingerprint)
			}
		}
	}
}

func TestDiscoverStoresIncludesLocalAndAccountSources(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, StoreFilename), nil, 0o644)
	os.MkdirAll(filepath.Join(root, "Sources", "account-source"), 0o755)
	os.WriteFile(filepath.Join(root, "Sources", "account-source", StoreFilename), nil, 0o644)
	stores, err := DiscoverStores(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(stores) != 2 || stores[0].SourceID != "local" || stores[1].SourceID != "account-source" {
		t.Fatalf("stores = %+v", stores)
	}
	if _, err := DiscoverStores(filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing root must error")
	}
}

type fakeClient struct{ batches [][]map[string]any }

func (f *fakeClient) UploadAppleContactsBatch(gz []byte, exportedAt string) (ingestclient.StoredObject, error) {
	f.batches = append(f.batches, testfixtures.DecodeGzipJSONL(gz))
	return ingestclient.StoredObject{StorageKey: "apple-contacts/inbox/" + exportedAt}, nil
}

func TestIncrementalUploadEmitsChangesThenTombstone(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "Sources", "src"), 0o755)
	store := filepath.Join(root, "Sources", "src", StoreFilename)
	testfixtures.CopyFile(t, filepath.Join("testdata", "synthetic.abcddb"), store)
	statePath := filepath.Join(t.TempDir(), "state.sqlite")
	state, err := OpenState(statePath, "zach@example.com", root)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	client := &fakeClient{}
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	runner := &Runner{Account: "zach@example.com", StorePath: root, Client: client, Logger: &common.RecordingLogger{}, State: state, Now: func() time.Time { return now }}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.ContactsSeen != 2 || summary.ContactsSelected != 2 || summary.BatchesUploaded != 1 {
		t.Fatalf("first summary = %+v", summary)
	}
	first := client.batches[0][0]
	if first["source"] != "apple_contacts" || first["record_type"] != "contact" || first["exported_at"] != "2026-07-22T12:00:00+00:00" {
		t.Fatalf("envelope = %v", first)
	}
	summary, _ = runner.Sync()
	if summary.ContactsSkipped != 2 || summary.BatchesUploaded != 0 {
		t.Fatalf("second summary = %+v", summary)
	}
	// Empty the store: every known card becomes a tombstone.
	testfixtures.CreateEmptySyntheticContacts(t, store)
	summary, err = runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.ContactsDeleted != 2 || summary.BatchesUploaded != 1 {
		t.Fatalf("tombstone summary = %+v", summary)
	}
	record := client.batches[1][0]["record"].(map[string]any)
	if record["is_deleted"] != true || record["created_at"] != "1970-01-01T00:00:00+00:00" {
		t.Fatalf("tombstone = %v", record)
	}
	summary, _ = runner.Sync()
	if summary.ContactsDeleted != 0 || summary.BatchesUploaded != 0 {
		t.Fatalf("a tombstone is emitted once: %+v", summary)
	}
}

func TestLimitDefersAndNetworkGuardSkips(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "Sources", "src"), 0o755)
	testfixtures.CopyFile(t, filepath.Join("testdata", "synthetic.abcddb"), filepath.Join(root, "Sources", "src", StoreFilename))
	client := &fakeClient{}
	runner := &Runner{Account: "z", StorePath: root, Client: client, Logger: &common.RecordingLogger{}, Limit: 1}
	summary, err := runner.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if summary.ContactsSelected != 1 || summary.ContactsDeferred != 1 {
		t.Fatalf("limit summary = %+v", summary)
	}
	blocked := &Runner{Account: "z", StorePath: root, Client: client, Logger: &common.RecordingLogger{}, BeforeUploadCheck: func() string { return "blocked" }}
	summary, _ = blocked.Sync()
	if summary.BatchesUploaded != 0 || summary.ContactsDeferred != 2 {
		t.Fatalf("blocked summary = %+v", summary)
	}
}

func TestStateWipesWhenStorePathChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.sqlite")
	state, _ := OpenState(path, "z", "/a")
	state.MarkSuccess("s", "c", "fp", "Name", false, time.Now())
	state.Close()
	state, _ = OpenState(path, "z", "/b")
	defer state.Close()
	entries, _ := state.Entries()
	if len(entries) != 0 {
		t.Fatalf("entries survived a store change: %+v", entries)
	}
}
