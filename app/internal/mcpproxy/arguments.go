package mcpproxy

import (
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// unknownArguments checks call arguments against a tool's cached input
// schema. Only a schema that closes its object (additionalProperties: false)
// is judged: then a key it does not list can never succeed, and the upstream's
// answer to one was an opaque http 500 (26 sessions sent intelligence__query
// {"sql": ...} in the week to 2026-11-23). It returns "" when the arguments
// pass or the schema says nothing about extra keys.
func unknownArguments(toolName string, schema any, arguments map[string]json.RawMessage) string {
	raw, err := json.Marshal(schema)
	if err != nil {
		return ""
	}
	var shape struct {
		AdditionalProperties json.RawMessage            `json:"additionalProperties"`
		Properties           map[string]json.RawMessage `json:"properties"`
		Required             []string                   `json:"required"`
	}
	if json.Unmarshal(raw, &shape) != nil || strings.TrimSpace(string(shape.AdditionalProperties)) != "false" {
		return ""
	}
	var unknown []string
	for key := range arguments {
		if _, ok := shape.Properties[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return ""
	}
	sort.Strings(unknown)
	quoted := make([]string, len(unknown))
	for i, key := range unknown {
		quoted[i] = strconv.Quote(key)
	}
	message := "unknown field " + quoted[0]
	if len(quoted) > 1 {
		message = "unknown fields " + strings.Join(quoted, ", ")
	}
	return message + "; " + toolName + " takes: " + describeArguments(shape.Properties, shape.Required)
}

// describeArguments lists required properties first, in schema order, then
// the optional ones alphabetically.
func describeArguments(properties map[string]json.RawMessage, required []string) string {
	var parts []string
	for _, name := range required {
		if _, ok := properties[name]; ok {
			parts = append(parts, name+" (required)")
		}
	}
	var optional []string
	for name := range properties {
		if !slices.Contains(required, name) {
			optional = append(optional, name)
		}
	}
	sort.Strings(optional)
	parts = append(parts, optional...)
	if len(parts) == 0 {
		return "no arguments"
	}
	return strings.Join(parts, ", ")
}
