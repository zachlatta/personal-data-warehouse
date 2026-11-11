// Package scripts composes warehouse reads in a fresh, constrained subprocess.
package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

const MaxOutput = 128 * 1024
const MaxOperationBytes = 1024 * 1024

type Input struct {
	Code      string          `json:"code" jsonschema:"JavaScript async function body; await and return supported; pdw read-only facade and args only"`
	Args      json.RawMessage `json:"args,omitempty" jsonschema:"JSON input data"`
	TimeoutMS int             `json:"timeout_ms,omitempty" jsonschema:"wall clock budget in milliseconds, default 30000, maximum 60000"`
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

func (e *Error) Error() string { return e.Message }

type Operation struct {
	Method     string   `json:"method"`
	DurationMS int64    `json:"duration_ms"`
	Error      *Error   `json:"error,omitempty"`
	Priorities []string `json:"priorities,omitempty"`
	// Result rows never enter the telemetry record. This small scope summary lets
	// agent_usage account for effective priority filters without retaining data.
	ReturnedPriorityCounts map[string]int `json:"returned_priority_counts,omitempty"`
}
type Source struct {
	Ref      string `json:"ref"`
	Priority string `json:"priority"`
}
type Response struct {
	// Put the authoritative id before user-controlled value data in transcript
	// JSON, so collectors never mistake a nested value.run_id for this run.
	RunID       string          `json:"run_id"`
	Value       json.RawMessage `json:"value"`
	Operations  []Operation     `json:"operations"`
	DurationMS  int64           `json:"duration_ms"`
	Truncated   bool            `json:"truncated"`
	Sources     []Source        `json:"sources"`
	Error       *Error          `json:"error,omitempty"`
	DroppedRows int             `json:"dropped_rows,omitempty"`
}
type Call struct {
	ID     int             `json:"id"`
	Method string          `json:"method"`
	Input  json.RawMessage `json:"input"`
}
type Reply struct {
	ID    int             `json:"id"`
	Value json.RawMessage `json:"value,omitempty"`
	Error *Error          `json:"error,omitempty"`
}
type packet struct {
	Calls []Call          `json:"calls"`
	Done  bool            `json:"done"`
	Value json.RawMessage `json:"value"`
	Error *Error          `json:"error"`
}
type Invoke func(context.Context, string, json.RawMessage) (any, error)
type Record struct {
	Response
	Client        string `json:"client"`
	TokenID       string `json:"token_id"`
	Surface       string `json:"surface"`
	Outcome       string `json:"outcome"`
	BytesReturned int    `json:"bytes_returned"`
}
type Recorder func(context.Context, Record) error

func fault(code, message string) *Error { return &Error{Code: code, Message: message} }

// safeValue keeps exact PostgreSQL bigint/numeric values across JSON.parse.
func safeValue(x any) any {
	if x == nil {
		return nil
	}
	switch v := x.(type) {
	case json.RawMessage:
		var y any
		d := json.NewDecoder(bytes.NewReader(v))
		d.UseNumber()
		if d.Decode(&y) == nil {
			return safeValue(y)
		}
		return nil
	case json.Number:
		if n, e := strconv.ParseInt(string(v), 10, 64); e == nil {
			return safeValue(n)
		}
		return string(v)
	}
	v := reflect.ValueOf(x)
	switch v.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32:
		n := v.Int()
		if n > 9007199254740991 || n < -9007199254740991 {
			return strconv.FormatInt(n, 10)
		}
		return n
	case reflect.Uint, reflect.Uint64:
		n := v.Uint()
		if n > 9007199254740991 {
			return strconv.FormatUint(n, 10)
		}
		return n
	case reflect.Slice, reflect.Array:
		a := make([]any, v.Len())
		for i := range a {
			a[i] = safeValue(v.Index(i).Interface())
		}
		return a
	case reflect.Map:
		a := map[string]any{}
		for _, k := range v.MapKeys() {
			a[fmt.Sprint(k.Interface())] = safeValue(v.MapIndex(k).Interface())
		}
		return a
	}
	return x
}
