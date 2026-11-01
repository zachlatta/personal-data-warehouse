package printer

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/mutationworkers/queue"
)

type noLock struct{}

func (noLock) Acquire(context.Context) (func(), bool, error) { return func() {}, true, nil }

// TestIntegrationAnApprovedPrintIsClaimedOnlyByThePrinterWorker proposes and
// approves a print through the app's own store, then runs one pass of the
// printer worker against the same throwaway Postgres (PDW_TEST_POSTGRES_URL).
func TestIntegrationAnApprovedPrintIsClaimedOnlyByThePrinterWorker(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("PDW_TEST_POSTGRES_URL"))
	if url == "" {
		t.Skip("PDW_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	app, err := mutations.NewPostgresStore(url, 30*time.Second)
	if err != nil {
		t.Fatalf("open app store: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	worker, err := queue.OpenPostgres(ctx, url, 30*time.Second)
	if err != nil {
		t.Fatalf("open worker store: %v", err)
	}
	t.Cleanup(func() { _ = worker.Close() })
	if err := worker.EnsureTables(ctx); err != nil {
		t.Fatalf("ensure tables: %v", err)
	}

	encoded := base64.StdEncoding.EncodeToString(testPDF)
	raw := map[string]any{
		"type": mutations.PrinterPrintDocumentOperation, "filename": "a.pdf", "content_type": "application/pdf",
		"data_base64": encoded, "page_count": float64(2), "copies": float64(1),
	}
	request, err := app.CreateRequest(ctx, mutations.CreateRequestInput{
		Title: fmt.Sprintf("%s %d", t.Name(), time.Now().UnixNano()), Reason: "integration test", RequestedBy: "test",
		Mutations: []mutations.MutationInput{{Type: mutations.PrinterPrintDocumentOperation, Account: mutations.PrinterAccount, Raw: raw}},
	})
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	// What a reviewer reads carries the preview, not the bytes.
	if _, ok := request.Mutations[0].Payload["data_base64"]; ok {
		t.Fatalf("review read carries the PDF: %#v", request.Mutations[0].Payload)
	}
	if preview, _ := request.Mutations[0].Preview["print_document"].(map[string]any); preview["sha256"] == nil {
		t.Fatalf("preview = %#v", request.Mutations[0].Preview)
	}
	if _, err := app.ApproveRequest(ctx, request.ID, "test-reviewer"); err != nil {
		t.Fatalf("approve: %v", err)
	}

	// Another local-only worker never sees it.
	if claimed, err := worker.ClaimApproved(ctx, 25, "test:other", []string{"apple_notes"}); err != nil {
		t.Fatalf("claim apple_notes: %v", err)
	} else {
		for _, mutation := range claimed {
			if mutation.ID == request.Mutations[0].ID {
				t.Fatal("apple_notes claimed a print")
			}
		}
	}

	var seen []queue.Mutation
	commands := &fakeCommands{pages: "2", print: printPass, statuses: []string{statusCompleted}}
	executor := testExecutor(commands)
	summary, err := queue.Process(ctx, queue.Options{
		Store: worker, Lock: noLock{}, Provider: Spec.Provider, Idempotent: Spec.Idempotent, ClaimedBy: "test:printer",
		Executor: queue.ExecutorFunc(func(mutation queue.Mutation) queue.Result {
			seen = append(seen, mutation)
			return executor.Execute(mutation)
		}),
	})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	var mine *queue.Mutation
	for i := range seen {
		if seen[i].ID == request.Mutations[0].ID {
			mine = &seen[i]
		}
		if seen[i].Provider != Provider {
			t.Fatalf("printer worker claimed %s", seen[i].Provider)
		}
	}
	if mine == nil || summary.Succeeded < 1 {
		t.Fatalf("summary = %#v seen = %d", summary, len(seen))
	}
	// The worker gets the stored bytes the review read left out.
	if mine.Payload["data_base64"] != encoded {
		t.Fatal("the worker did not get the PDF")
	}
	read, err := app.GetRequest(ctx, request.ID, mutations.AllMutations)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	got := read.Mutations[0]
	if got.Status != "succeeded" || got.Result["job_id"] != "41" || got.Result["verified"] != true {
		t.Fatalf("mutation after the pass = %s %#v", got.Status, got.Result)
	}
}
