package server

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/zachlatta/personal-data-warehouse/app/internal/mutations"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
)

// updateSchemaGoldens regenerates the testdata/schemas/*.input_schema.json
// files from the live MCP server. Run `go test ./internal/server/ -update`
// after intentional schema changes; otherwise this test acts as a
// regression guard against accidental drift in tool input schemas.
var updateSchemaGoldens = flag.Bool("update", false, "regenerate input-schema golden files")

// snapshotTools is the list of tool names we want byte-identical MCP
// InputSchemas for across refactors. _debug_cache_status is excluded
// because it is gated behind a config flag and covered separately.
var snapshotTools = []string{
	"query",
	"get_rows",
	"get_field",
	"grep_rows",
	"schema_overview",
	"propose_mutation",
	"propose_mutation_help",
}

func TestMCPToolInputSchemasMatchGolden(t *testing.T) {
	runner := fakeRunner{results: map[string]query.RawResult{}}
	mutationSvc := mutations.NewService(fakeMutationStore{request: mutations.Request{ID: "snapshot"}}, mutations.Config{BaseURL: "https://snapshot.example.test"})
	srv := NewMCPServerWithMutations(runner, query.Options{MaxRows: 5, MaxFieldChars: 100}, mutationSvc)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx, serverTransport) }()

	client := mcp.NewClient(&mcp.Implementation{Name: "snapshot-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect failed: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	tools, err := session.ListTools(ctx, &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	byName := map[string]*mcp.Tool{}
	for _, tl := range tools.Tools {
		byName[tl.Name] = tl
	}

	for _, name := range snapshotTools {
		tl, ok := byName[name]
		if !ok {
			t.Fatalf("tool %q not exposed; got %v", name, byName)
		}
		got, err := json.MarshalIndent(tl.InputSchema, "", "  ")
		if err != nil {
			t.Fatalf("marshal %s input schema: %v", name, err)
		}
		path := filepath.Join("testdata", "schemas", name+".input_schema.json")
		if *updateSchemaGoldens {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			if err := os.WriteFile(path, append(got, '\n'), 0o644); err != nil {
				t.Fatalf("write golden: %v", err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read golden %s: %v (run with -update to create it)", path, err)
		}
		gotWithNL := append(got, '\n')
		if string(gotWithNL) != string(want) {
			t.Fatalf("input schema for %s drifted.\n--- want ---\n%s\n--- got ---\n%s", name, want, gotWithNL)
		}
	}
}
