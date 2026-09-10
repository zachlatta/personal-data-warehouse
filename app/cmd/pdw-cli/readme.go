package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/zachlatta/personal-data-warehouse/app/internal/guide"
)

// readmeUsage is the command's own help.
var readmeUsage = `pdw readme - the agent guide to the warehouse.

USAGE
  pdw readme [topic]

With no topic, prints the main guide: the search-first workflow, the command
map, the priority tiers, the SQL rules, where each domain lives, and what a
negative result means. Bare "pdw" prints the same thing. With a topic, prints
that deep-dive section.

TOPICS
` + readmeTopicLines() + `
The guide is rendered from the binary, so it needs no warehouse URL or token
and always matches the commands this build accepts. Over MCP the same guide
is the "readme" tool.
`

func readmeTopicLines() string {
	out := ""
	for _, topic := range guide.Topics(guide.SurfaceCLI) {
		out += fmt.Sprintf("  %-16s%s\n", topic.Name, topic.Summary)
	}
	return out
}

// runReadme prints the guide. It is dispatched before configuration is
// resolved on purpose: a machine with no token yet still needs to learn that
// `pdw login` is the next step, and a guide behind a login is a guide nobody
// reads at the moment it matters.
func runReadme(args []string, stdout, stderr io.Writer) int {
	if hasHelpArg(args) {
		fmt.Fprint(stdout, readmeUsage)
		return 0
	}
	if len(args) > 1 {
		fmt.Fprintln(stderr, "pdw readme: pass at most one topic (usage: pdw readme [topic])")
		return 2
	}
	topic := ""
	if len(args) == 1 {
		topic = args[0]
	}
	text, err := guide.Render(guide.SurfaceCLI, topic)
	if err != nil {
		var unknown *guide.UnknownTopicError
		if errors.As(err, &unknown) {
			fmt.Fprintln(stderr, "pdw readme:", err)
			return 2
		}
		fmt.Fprintln(stderr, "pdw readme:", err)
		return 1
	}
	fmt.Fprint(stdout, text)
	return 0
}
