package scripts

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/query"
)

type Service struct {
	invoke      Invoke
	role        bool
	checkSafety func(context.Context) error
	record      Recorder
	mu          sync.Mutex
	clients     map[string]int
	runs        chan struct{}
	inner       chan struct{}
	executable  string
}

func New(invoke Invoke, role bool, record Recorder, checks ...func(context.Context) error) *Service {
	exe, _ := os.Executable()
	s := &Service{invoke: invoke, role: role, record: record, clients: map[string]int{}, runs: make(chan struct{}, 4), inner: make(chan struct{}, 6), executable: exe}
	if len(checks) > 0 {
		s.checkSafety = checks[0]
	}
	return s
}

// Run never supplies credentials or objects to the worker: only serialized JSON.
func (s *Service) Run(ctx context.Context, in Input) (r Response) {
	start := time.Now()
	var id [16]byte
	_, _ = rand.Read(id[:])
	r = Response{RunID: hex.EncodeToString(id[:]), Value: json.RawMessage("null"), Operations: []Operation{}, Sources: []Source{}}
	defer func() {
		r.DurationMS = time.Since(start).Milliseconds()
		b, _ := json.Marshal(r)
		if len(b) > MaxOutput {
			r.Value = json.RawMessage("null")
			r.Sources = []Source{}
			for i := range r.Operations {
				if r.Operations[i].Error != nil {
					r.Operations[i].Error = fault(r.Operations[i].Error.Code, "Operation failed; narrow the script to inspect the error")
				}
			}
			r.Error = fault("output_limit", "Result and citations exceed 128 KiB; return less data")
		}
		b, _ = json.Marshal(r)
		if s.record != nil {
			p, _ := auth.PrincipalFromContext(ctx)
			surface := "api"
			if p.Scope == auth.ScopeOAuthQuery {
				surface = "mcp"
			}
			rec := Record{Response: r, Client: auth.ClientNameFromContext(ctx), TokenID: p.TokenID, Surface: surface, Outcome: "ok", BytesReturned: len(b)}
			if r.Error != nil {
				rec.Outcome = r.Error.Code
			}
			rec.Value = nil
			rec.Sources = nil
			// An unavailable telemetry store is visible to the client, not a silent
			// success that leaves the week-long pilot impossible to evaluate.
			writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			if err := s.record(writeCtx, rec); err != nil {
				slog.Error("script telemetry write failed", "run_id", r.RunID, "error", err)
				r.Error = fault("telemetry_unavailable", "Run telemetry could not be saved; do not automatically retry")
			}
		}
	}()
	if !s.role {
		r.Error = fault("query_role_required", "Scripts require a configured query role and a verified unprivileged database login")
		return
	}
	if s.checkSafety != nil {
		checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := s.checkSafety(checkCtx)
		cancel()
		if err != nil {
			r.Error = fault("unsafe_query_login", err.Error())
			return
		}
	}
	if len(in.Code) == 0 || len(in.Code) > 32768 || len(in.Args) > 32768 {
		r.Error = fault("input_limit", "Code and args must each be at most 32 KiB")
		return
	}
	if in.TimeoutMS < 0 || in.TimeoutMS > 60000 {
		r.Error = fault("invalid_input", "timeout_ms must be between 1 and 60000")
		return
	}
	if len(in.Args) == 0 {
		in.Args = json.RawMessage("null")
	}
	if !json.Valid(in.Args) {
		r.Error = fault("invalid_input", "args must be valid JSON")
		return
	}
	in.Args, _ = json.Marshal(safeValue(in.Args))
	if in.TimeoutMS == 0 {
		in.TimeoutMS = 30000
	}
	callerCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, time.Duration(in.TimeoutMS)*time.Millisecond)
	defer cancel()
	client := auth.ClientNameFromContext(ctx)
	p, _ := auth.PrincipalFromContext(ctx)
	client += "/" + p.TokenID
	// Client names divide shared-root capacity too; the global four-run cap
	// remains authoritative even when a client supplies a different name.
	client = string(p.Scope) + "/" + client
	s.mu.Lock()
	if s.clients[client] >= 2 {
		s.mu.Unlock()
		r.Error = fault("busy", "At most two concurrent runs per caller")
		return
	}
	s.clients[client]++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.clients[client]--
		if s.clients[client] == 0 {
			delete(s.clients, client)
		}
		s.mu.Unlock()
	}()
	select {
	case s.runs <- struct{}{}:
		defer func() { <-s.runs }()
	default:
		r.Error = fault("busy", "Four script workers are already active; retry later")
		return
	}
	cmd := exec.CommandContext(ctx, s.executable, "--pdw-script-worker")
	cmd.Env = []string{"GOMEMLIMIT=96MiB", "GOGC=50", "GOMAXPROCS=1"}
	cmd.WaitDelay = time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		r.Error = fault("worker_error", err.Error())
		return
	}
	defer stdin.Close()
	stdout, workerOut, err := os.Pipe()
	if err != nil {
		r.Error = fault("worker_error", err.Error())
		return
	}
	defer stdout.Close()
	defer workerOut.Close()
	cmd.Stdout = workerOut
	// stderr is discarded: scripts cannot turn errors into an unbounded log sink.
	if err = cmd.Start(); err != nil {
		r.Error = fault("worker_error", err.Error())
		return
	}
	_ = workerOut.Close()
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited); cancel() }()
	stopped := make(chan struct{})
	defer close(stopped)
	violations := make(chan string, 1)
	go watchWorker(cmd.Process, stopped, func(code string) { violations <- code; cancel() })
	defer func() { _ = stdin.Close(); _ = cmd.Process.Kill(); <-exited }()
	workerFailure := func() *Error {
		select {
		case code := <-violations:
			return fault(code, "Worker resource limit reached")
		default:
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || callerCtx.Err() != nil {
			return fault("timeout", "Script deadline or caller cancellation reached")
		}
		return fault("worker_error", "Worker exited before completion")
	}
	encoder := json.NewEncoder(stdin)
	if err = encoder.Encode(in); err != nil {
		r.Error = fault("worker_error", err.Error())
		return
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), MaxOperationBytes+MaxOutput)
	totalBytes, totalRows := 0, 0
	localInner := make(chan struct{}, 4)
	seen := map[int]bool{}
	sourceSeen := map[Source]bool{}
	for scanner.Scan() {
		var packet packet
		if err = json.Unmarshal(scanner.Bytes(), &packet); err != nil {
			r.Error = fault("worker_protocol", "Invalid worker response")
			return
		}
		if packet.Done {
			r.Value = packet.Value
			r.Error = packet.Error
			if r.Error != nil && !knownErrorCode(r.Error.Code) {
				r.Error.Code = "script_error"
			}
			return
		}
		if len(packet.Calls) > 12-len(r.Operations) {
			r.Error = fault("operation_limit", "At most twelve operations per run")
			return
		}
		replies := make([]Reply, len(packet.Calls))
		ops := make([]Operation, len(packet.Calls))
		values := make([]map[string]any, len(packet.Calls))
		// Validate the entire batch before starting any dispatch.
		for _, call := range packet.Calls {
			if seen[call.ID] || call.ID <= 0 || call.ID > 12 || !allowed(call.Method) || len(call.Input) > 32768 {
				r.Error = fault("worker_protocol", "Invalid or duplicate broker request")
				return
			}
			seen[call.ID] = true
		}
		var wg sync.WaitGroup
		for i, call := range packet.Calls {
			wg.Add(1)
			go func(i int, call Call) {
				defer wg.Done()
				started := time.Now()
				ops[i] = Operation{Method: call.Method}
				replies[i].ID = call.ID
				select {
				case localInner <- struct{}{}:
					defer func() { <-localInner }()
				case <-ctx.Done():
					replies[i].Error = fault("timeout", "Run cancelled")
					return
				}
				select {
				case s.inner <- struct{}{}:
					defer func() { <-s.inner }()
				case <-ctx.Done():
					replies[i].Error = fault("timeout", "Run cancelled")
					return
				}
				operationCtx, stop := context.WithTimeout(ctx, 15*time.Second)
				defer stop()
				var requested struct {
					Priorities []string `json:"priorities"`
					SQL        string   `json:"sql"`
				}
				_ = json.Unmarshal(call.Input, &requested)
				ops[i].Priorities = requested.Priorities
				ops[i].Priorities = knownPriorities(ops[i].Priorities)
				var v any
				var e error
				if call.Method == "sql" {
					e = query.ValidateReadOnlySQL(requested.SQL)
				}
				if e == nil {
					e = auth.CheckAuthorization(operationCtx)
				}
				if e == nil {
					v, e = s.invoke(operationCtx, call.Method, call.Input)
				}
				if e != nil {
					var typed *Error
					if errors.As(e, &typed) {
						replies[i].Error = typed
					} else {
						replies[i].Error = fault("operation_error", e.Error())
					}
				} else {
					b, encodeErr := json.Marshal(v)
					if encodeErr != nil {
						replies[i].Error = fault("encoding_error", "Operation cannot be encoded")
					} else if len(b) > MaxOperationBytes {
						replies[i].Error = fault("operation_limit", "Operation exceeds 1 MiB; narrow the read")
					} else {
						var data map[string]any
						d := json.NewDecoder(bytes.NewReader(b))
						d.UseNumber()
						_ = d.Decode(&data)
						values[i] = data
					}
				}
				ops[i].DurationMS = time.Since(started).Milliseconds()
			}(i, call)
		}
		wg.Wait()
		for i, call := range packet.Calls {
			data := values[i]
			if data != nil {
				dropped := filterExcluded(ctx, call.Method, call.Input, data)
				r.DroppedRows += dropped
				if rows, ok := data["rows"].([]any); ok {
					totalRows += len(rows)
					for _, row := range rows {
						m, _ := row.(map[string]any)
						ref, _ := m["ref"].(string)
						priority, _ := m["priority"].(string)
						if ref == "" {
							adapter, _ := m["adapter"].(string)
							event, _ := m["event_id"].(string)
							if adapter != "" && event != "" {
								ref = adapter + ":" + event
							}
						}
						if ref != "" {
							source := Source{ref, priority}
							if !sourceSeen[source] {
								sourceSeen[source] = true
								r.Sources = append(r.Sources, source)
							}
						}
					}
				}
				if m, ok := data["returned_priority_counts"].(map[string]any); ok {
					ops[i].ReturnedPriorityCounts = map[string]int{}
					for k, v := range m {
						n, _ := v.(json.Number).Int64()
						ops[i].ReturnedPriorityCounts[k] = int(n)
					}
				}
				if data["truncated"] == true || data["truncations"] != nil {
					r.Truncated = true
				}
				replies[i].Value, _ = json.Marshal(safeValue(data))
			}
			ops[i].Error = replies[i].Error
			r.Operations = append(r.Operations, ops[i])
		}
		encodedReplies, encodeErr := json.Marshal(replies)
		if encodeErr != nil {
			r.Error = fault("encoding_error", "Broker response could not be encoded")
			return
		}
		totalBytes += len(encodedReplies)
		if totalBytes > 4*MaxOperationBytes || totalRows > 5000 {
			r.Error = fault("operation_limit", "Aggregate row or byte budget exhausted")
			return
		}
		if err = encoder.Encode(replies); err != nil {
			r.Error = workerFailure()
			return
		}
	}
	r.Error = workerFailure()
	return
}
func allowed(method string) bool {
	switch method {
	case "search", "context", "columns", "schema", "sql":
		return true
	}
	return false
}

func knownErrorCode(code string) bool {
	switch code {
	case "script_error", "stack_limit", "operation_error", "operation_limit", "input_limit", "output_limit", "encoding_error", "timeout", "busy", "worker_error", "worker_protocol", "query_role_required", "invalid_input", "telemetry_unavailable", "cpu_limit", "memory_limit":
		return true
	}
	return false
}
func knownPriorities(values []string) []string {
	out := []string{}
	for _, v := range values {
		switch v {
		case "self", "direct", "cc", "noise", "background", "unclassified":
			out = append(out, v)
		}
	}
	return out
}

// DecodeInput disallows guessed arguments instead of silently dropping them.
func DecodeInput(raw json.RawMessage, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("one JSON value required")
	}
	return nil
}

// SafeError preserves existing recovery hints while giving scripts typed errors.
func SafeError(message, hint string) error {
	return &Error{Code: "operation_error", Message: strings.TrimSpace(message), Hint: hint}
}
