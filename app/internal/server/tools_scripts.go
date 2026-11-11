package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
	"github.com/zachlatta/personal-data-warehouse/app/internal/scripts"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

type scriptToolWithoutResultLog struct{ tool.Tool }

func (scriptToolWithoutResultLog) LogOutput() bool { return false }

func scriptTool(runner query.Runner, opts query.Options) tool.Tool {
	// A separate service instance shares all implementation paths, while its
	// runner places stricter wire/row limits on every script read.
	opts.MaxRows = 1000
	svc := query.NewService(query.ScriptRunner{Runner: runner}, opts)
	guarded, ok := runner.(interface {
		QueryRole() string
		CheckQuerySafety(context.Context) error
	})
	role := ok && guarded.QueryRole() != ""
	var check func(context.Context) error
	if ok {
		check = guarded.CheckQuerySafety
	}
	var record scripts.Recorder
	if writer, ok := opts.ScriptRecorder.(scripts.Recorder); ok {
		record = writer
	}
	run := scripts.New(func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		switch method {
		case "search":
			var in searchInput
			if err := scripts.DecodeInput(raw, &in); err != nil {
				return nil, err
			}
			if in.MaxResults > 100 {
				return nil, fmt.Errorf("search max_results must be <=100")
			}
			v := svc.Search(ctx, query.SearchRequest{Query: in.Query, MaxResults: in.MaxResults, Sources: in.Sources, Since: in.Since, Mode: in.Mode, Priorities: in.Priorities})
			if v.Error != "" {
				return nil, scripts.SafeError(v.Error, v.Hint)
			}
			return v, nil
		case "context":
			var in contextInput
			if err := scripts.DecodeInput(raw, &in); err != nil {
				return nil, err
			}
			before, after := 5, 5
			if in.Before != nil {
				before = *in.Before
			}
			if in.After != nil {
				after = *in.After
			}
			v := svc.Context(ctx, query.ContextRequest{Ref: in.Ref, Before: before, After: after, Full: in.Full})
			if v.Error != "" {
				return nil, scripts.SafeError(v.Error, "")
			}
			return v, nil
		case "columns":
			var in struct {
				Table string `json:"table"`
			}
			if err := scripts.DecodeInput(raw, &in); err != nil {
				return nil, err
			}
			v, e := svc.StructuredColumns(ctx, in.Table)
			return map[string]any{"columns": v.Rows}, e
		case "schema":
			var in struct {
				Schema string `json:"schema"`
			}
			if err := scripts.DecodeInput(raw, &in); err != nil {
				return nil, err
			}
			v, e := svc.StructuredSchema(ctx, in.Schema)
			return map[string]any{"rows": v.Rows}, e
		case "sql":
			var in struct {
				SQL      string `json:"sql"`
				Question string `json:"question"`
			}
			if err := scripts.DecodeInput(raw, &in); err != nil {
				return nil, err
			}
			if in.Question == "" {
				in.Question = "script SQL"
			}
			v := svc.ExecuteBatchFull(ctx, []query.Statement{{SQL: in.SQL, Question: in.Question}}, "json").Results[0]
			if v.Error != "" {
				return nil, scripts.SafeError(v.Error, v.Hint)
			}
			return v, nil
		}
		return nil, fmt.Errorf("method is unavailable")
	}, role, record, check)
	base := &tool.Typed[scripts.Input, scripts.Response]{NameStr: "run", TitleStr: "Run read-only JavaScript", DescriptionStr: "Compose warehouse reads on the PDW server in one fresh sandbox. Async JavaScript function body supports await and return. Globals: frozen pdw.search({query,priorities?,sources?,since?,max_results?}), pdw.context({ref,before?,after?,full?}), pdw.columns({table}), pdw.schema({schema?}), pdw.sql({sql,question?}) and immutable args. Search first, use priorities, preserve refs. No imports, files, network, timers, connections or writes. 12 inner calls, 32 KiB code/args each, 128 KiB output, 30s default/60s max. Read readme topic scripts for schemas, limits and examples.", Handle: func(ctx context.Context, in scripts.Input) (scripts.Response, error) { return run.Run(ctx, in), nil }, IsError: func(r scripts.Response) bool { return r.Error != nil }}
	return scriptToolWithoutResultLog{base}
}

// The managed benchmark proxy forwards only restrictions, never credentials.
// Applying them inside the broker prevents a script from projecting excluded
// rows into an unrecognizable answer before the Python proxy sees it.
func withScriptExclusions(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if raw := r.Header.Get("X-PDW-Source-Exclusions"); raw != "" {
			var e scripts.Exclusions
			if len(raw) > 65536 || json.Unmarshal([]byte(raw), &e) != nil {
				http.Error(w, "invalid source exclusions", 400)
				return
			}
			r = r.WithContext(scripts.WithExclusions(r.Context(), e))
		}
		next.ServeHTTP(w, r)
	})
}
