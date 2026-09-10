// Package guide is the agent-facing manual for the personal data warehouse,
// rendered by `pdw readme` on the CLI and by the `readme` tool on MCP.
//
// It exists because the manual used to live in a fleet skill outside this
// repository, where it drifted from the code it described: command names,
// relation names and priority tiers were all transcribed by hand, and every
// warehouse reorg made a paragraph of it wrong. Keeping the manual beside the
// dispatcher means the usage tests, the catalog and the priority contract all
// pin it, and an agent reads the version that matches the binary answering it.
//
// The main guide is deliberately compact -- it is what every session reads --
// and the domain detail lives in topics an agent reads when it enters that
// domain. Both are markdown templates; the only templating is choosing the
// command spelling for the surface that asked.
package guide

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"text/template"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// Surface is the transport an agent is reading the guide from. The guide is
// one document; only the spelling of each call differs (`pdw search ...`
// against the `search` tool), so the surface selects that spelling.
type Surface int

const (
	// SurfaceCLI renders `pdw` command lines.
	SurfaceCLI Surface = iota
	// SurfaceMCP renders MCP tool calls with JSON arguments.
	SurfaceMCP
)

func (s Surface) String() string {
	if s == SurfaceMCP {
		return "mcp"
	}
	return "cli"
}

// Topic is one deep-dive section an agent reads when its question enters that
// domain. Name is what the caller passes; Summary is the one-line index entry.
type Topic struct {
	Name    string
	Summary string
	// CLIOnly topics describe machinery that only exists on the CLI (local
	// uploaders, credential publishers) and are not listed to MCP callers.
	CLIOnly bool
}

// topics is the index, in the order the main guide lists them. Every entry
// must have a template at topics/<name>.md; TestEveryTopicHasATemplate pins it.
var topics = []Topic{
	{Name: "search", Summary: "query shape, modes, scoping, conversation context, and what a miss means"},
	{Name: "sql", Summary: "columns-first SQL, the layers, type traps, the epoch sentinel, and the timeout budget"},
	{Name: "sources", Summary: "where each domain lives: the starting relation for mail, chat, calendar, files, notes, photos, voice, health, finance"},
	{Name: "agent-sessions", Summary: "every prior agent session across providers and machines: finding one, reading it, pairing tool calls"},
	{Name: "finance", Summary: "the ledger, net worth, commitments, securities and tax lots, and the guards that withhold a number"},
	{Name: "health", Summary: "WHOOP's two sources, the unit traps, cycles, sleep, strain and the private-API documents"},
	{Name: "slack", Summary: "Zach's Slack identity, what is and is not synced, coverage history, huddles"},
	{Name: "mutations", Summary: "proposing a reviewed write to Gmail, Calendar, Contacts, Slack or Apple Notes"},
	{Name: "ops", Summary: "is the data current: pipeline, mart, adapter, search and backup health"},
	{Name: "ingest", Summary: "local uploaders and credential publishers (`pdw ingest`, `pdw slack|chatgpt|whoop publish-session`)", CLIOnly: true},
}

//go:embed readme.md topics/*.md
var files embed.FS

// Topics returns the index visible on a surface, in listing order.
func Topics(surface Surface) []Topic {
	out := make([]Topic, 0, len(topics))
	for _, t := range topics {
		if t.CLIOnly && surface != SurfaceCLI {
			continue
		}
		out = append(out, t)
	}
	return out
}

// TopicNames returns the topic names visible on a surface, for error messages
// and completion.
func TopicNames(surface Surface) []string {
	names := make([]string, 0, len(topics))
	for _, t := range Topics(surface) {
		names = append(names, t.Name)
	}
	return names
}

// UnknownTopicError names the topic that does not exist and the ones that do,
// so the caller's next call is right.
type UnknownTopicError struct {
	Topic   string
	Surface Surface
}

func (e *UnknownTopicError) Error() string {
	return fmt.Sprintf("no readme topic %q; topics are: %s", e.Topic, strings.Join(TopicNames(e.Surface), ", "))
}

// Render returns the main guide when topic is empty, or one topic's section.
func Render(surface Surface, topic string) (string, error) {
	topic = strings.ToLower(strings.TrimSpace(topic))
	// Accept the spellings agents reach for: `agent_sessions`, `AgentSessions`.
	topic = strings.ReplaceAll(topic, "_", "-")
	topic = strings.ReplaceAll(topic, " ", "-")
	if topic == "" || topic == "readme" || topic == "index" {
		return render(surface, "readme.md")
	}
	for _, t := range Topics(surface) {
		if t.Name == topic {
			return render(surface, "topics/"+t.Name+".md")
		}
	}
	return "", &UnknownTopicError{Topic: topic, Surface: surface}
}

// templateData is what the markdown templates see. Everything that has a
// generated source (the priority contract, the topic index) comes from that
// source rather than being spelled again in prose.
type templateData struct {
	CLI        bool
	MCP        bool
	Surface    string
	Attention  string
	Tiers      []warehouse.TimelinePriorityTier
	Sentinel   warehouse.TimelinePriorityTier
	Selections []warehouse.TimelinePrioritySelection
	Topics     []Topic
}

func newTemplateData(surface Surface) templateData {
	return templateData{
		CLI:        surface == SurfaceCLI,
		MCP:        surface == SurfaceMCP,
		Surface:    surface.String(),
		Attention:  strings.Join(warehouse.TimelineAttentionPriorities(), ","),
		Tiers:      warehouse.TimelinePriorities.Tiers,
		Sentinel:   warehouse.TimelinePriorities.Sentinel,
		Selections: warehouse.TimelinePriorities.SelectionGuide,
		Topics:     Topics(surface),
	}
}

var funcs = template.FuncMap{
	"join": strings.Join,
	// jsonList renders a priority list as a JSON array literal for MCP examples.
	"jsonList": func(values []string) string {
		quoted := make([]string, 0, len(values))
		for _, v := range values {
			quoted = append(quoted, `"`+v+`"`)
		}
		return "[" + strings.Join(quoted, ",") + "]"
	},
	// split turns "self,direct,cc" back into its tiers.
	"split": func(s string) []string { return strings.Split(s, ",") },
}

func render(surface Surface, path string) (string, error) {
	raw, err := fs.ReadFile(files, path)
	if err != nil {
		return "", fmt.Errorf("guide: %w", err)
	}
	tmpl, err := template.New(path).Funcs(funcs).Parse(string(raw))
	if err != nil {
		return "", fmt.Errorf("guide: parse %s: %w", path, err)
	}
	var out bytes.Buffer
	if err := tmpl.Execute(&out, newTemplateData(surface)); err != nil {
		return "", fmt.Errorf("guide: render %s: %w", path, err)
	}
	return collapseBlankLines(out.String()), nil
}

// collapseBlankLines squeezes the runs of empty lines that conditional blocks
// leave behind, so a rendered guide reads as one document on either surface.
func collapseBlankLines(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	blank := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			blank++
			if blank > 1 {
				continue
			}
			out = append(out, "")
			continue
		}
		blank = 0
		out = append(out, strings.TrimRight(line, " \t"))
	}
	return strings.TrimRight(strings.Join(out, "\n"), "\n") + "\n"
}

// TemplatePaths lists every embedded template, for tests that sweep the prose.
func TemplatePaths() []string {
	var paths []string
	_ = fs.WalkDir(files, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		paths = append(paths, path)
		return nil
	})
	sort.Strings(paths)
	return paths
}

// ReadTemplate returns one embedded template's raw text, for tests.
func ReadTemplate(path string) (string, error) {
	raw, err := fs.ReadFile(files, path)
	return string(raw), err
}
