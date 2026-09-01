package main

import (
	"bytes"
	"flag"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/zachlatta/personal-data-warehouse/app/internal/warehouse"
)

// The CLI's usage text is the only discovery surface an agent reads before it
// picks a command, and it had drifted from the code twice over: `--priority`
// worked but appeared nowhere (used 6 times in a month of real sessions, while
// 321 ILIKE-on-base_* queries were written instead of scoped searches), and
// `pdw chatgpt` / `pdw whoop` shipped undocumented. These tests make the usage
// text a checked mirror of the dispatch table and of the search FlagSet, so the
// same drift cannot recur silently.

// dispatchedCommandRe finds the `cmd == "name"` guards run() dispatches on
// before the switch. Only string literals are matched, so the hidden
// autoUpdateCommand constant is correctly invisible here.
var dispatchedCommandRe = regexp.MustCompile(`\bcmd == "([a-z_][a-z0-9_-]*)"`)

// switchCaseRe finds the `case "name":` labels of the `switch cmd` block.
var switchCaseRe = regexp.MustCompile(`case "([a-z_][a-z0-9_-]*)":`)

// usageCommandsSection is the COMMANDS block, where each documented command
// begins a line at a two-space indent.
func usageCommandsSection(t *testing.T) string {
	t.Helper()
	_, after, ok := strings.Cut(usage, "\nCOMMANDS\n")
	if !ok {
		t.Fatal("usage has no COMMANDS section")
	}
	section, _, ok := strings.Cut(after, "\nAUTO-UPDATE\n")
	if !ok {
		t.Fatal("usage's COMMANDS section has no terminator")
	}
	return section
}

// dispatchedCommands reads run.go and returns every command name run()
// actually dispatches. Reading the source rather than a hand-kept list is the
// point: a list would drift exactly the way the usage text did.
func dispatchedCommands(t *testing.T) []string {
	t.Helper()
	source, err := os.ReadFile("run.go")
	if err != nil {
		t.Fatalf("read run.go: %v", err)
	}
	text := string(source)
	seen := map[string]bool{}
	var names []string
	add := func(name string) {
		// -h/--help are flags spelled as commands, and the hidden
		// auto-update worker is not a user-facing command.
		if name == "help" || strings.HasPrefix(name, "-") || seen[name] {
			return
		}
		seen[name] = true
		names = append(names, name)
	}
	for _, match := range dispatchedCommandRe.FindAllStringSubmatch(text, -1) {
		add(match[1])
	}
	_, afterSwitch, ok := strings.Cut(text, "\tswitch cmd {\n")
	if !ok {
		t.Fatal("run.go no longer dispatches through `switch cmd {`")
	}
	switchBlock, _, ok := strings.Cut(afterSwitch, "\n\t}\n")
	if !ok {
		t.Fatal("could not find the end of the `switch cmd` block")
	}
	for _, match := range switchCaseRe.FindAllStringSubmatch(switchBlock, -1) {
		add(match[1])
	}
	return names
}

func TestEveryDispatchedCommandIsDocumentedInUsage(t *testing.T) {
	section := usageCommandsSection(t)
	commands := dispatchedCommands(t)
	if len(commands) < 10 {
		t.Fatalf("found only %d dispatched commands (%v); the scraper is broken", len(commands), commands)
	}
	for _, name := range commands {
		documented := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(name) + `\b`)
		if !documented.MatchString(section) {
			t.Fatalf("`pdw %s` is dispatched but absent from the usage COMMANDS section", name)
		}
	}
}

func TestUsageDocumentsTheSearchPriorityFlagWithAnExample(t *testing.T) {
	// --priority is the difference between "what needs my attention" and 48M
	// rows. It worked for a month while appearing nowhere in help.
	if !strings.Contains(usage, "--priority TIERS") {
		t.Fatalf("usage does not document the search --priority flag:\n%s", usage)
	}
	for _, tier := range []string{"self", "direct", "cc", "noise", "background"} {
		if !strings.Contains(usage, tier) {
			t.Fatalf("usage does not name the %q priority tier", tier)
		}
	}
	if !strings.Contains(usage, "pdw search --priority self,direct 'budget approval'") {
		t.Fatalf("usage has no worked --priority example:\n%s", usage)
	}
}

func TestSearchHelpRendersTheGeneratedPriorityContract(t *testing.T) {
	for _, tier := range warehouse.TimelinePriorities.Tiers {
		if !strings.Contains(searchUsage, tier.Name) || !strings.Contains(searchUsage, tier.Meaning) {
			t.Fatalf("search help omitted generated tier %q: %s", tier.Name, searchUsage)
		}
	}
	for _, selection := range warehouse.TimelinePriorities.SelectionGuide {
		if !strings.Contains(searchUsage, selection.Intent) || !strings.Contains(searchUsage, selection.Guidance) {
			t.Fatalf("search help omitted generated selection %q: %s", selection.Intent, searchUsage)
		}
	}
}

// searchFlagAliases are the plural spellings accepted so an agent's first guess
// works. They only have to appear in the subcommand's own help; repeating them
// in the global usage would bury the canonical flag.
var searchFlagAliases = map[string]bool{"sources": true, "priorities": true}

func TestEverySearchFlagIsDocumented(t *testing.T) {
	var opts searchOptions
	fs := newSearchFlagSet(&opts)
	count := 0
	fs.VisitAll(func(f *flag.Flag) {
		count++
		if !strings.Contains(searchUsage, "--"+f.Name) && !strings.Contains(searchUsage, "-"+f.Name) {
			t.Fatalf("search flag -%s is absent from `pdw search --help`:\n%s", f.Name, searchUsage)
		}
		if searchFlagAliases[f.Name] {
			return
		}
		if !strings.Contains(usage, "--"+f.Name) && !strings.Contains(usage, "-"+f.Name+" ") {
			t.Fatalf("search flag -%s is absent from the global usage text", f.Name)
		}
	})
	if count == 0 {
		t.Fatal("the search FlagSet registered no flags; the test is inspecting the wrong thing")
	}
}

func TestSearchHelpPrintsTheSearchFlagSetNotTheGlobalUsage(t *testing.T) {
	// `pdw search --help` short-circuited to the global usage, so the flags
	// this command actually accepts were undiscoverable from the command.
	for _, args := range [][]string{{"search", "--help"}, {"search", "-h"}} {
		var outBuf, errBuf bytes.Buffer
		code := run(args, strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" })
		if code != 0 {
			t.Fatalf("%v exit = %d (stderr=%s)", args, code, errBuf.String())
		}
		out := outBuf.String()
		if !strings.Contains(out, "pdw search") || !strings.Contains(out, "--priority") {
			t.Fatalf("%v did not print the search command's own help:\n%s", args, out)
		}
		if strings.Contains(out, "AUTO-UPDATE") {
			t.Fatalf("%v printed the global usage instead of the search FlagSet:\n%s", args, out)
		}
	}
}

func TestVersionFlagRedirectsToTheVersionCommand(t *testing.T) {
	// `pdw --version` was answered with a bare flag error plus the whole
	// ~100-line usage blob, ten times in thirty days.
	for _, args := range [][]string{{"--version"}, {"-version"}, {"-v"}, {"--base-url", "http://x", "--version"}} {
		arg := strings.Join(args, " ")
		var outBuf, errBuf bytes.Buffer
		code := run(args, strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" })
		if code == 0 {
			t.Fatalf("%s should not succeed", arg)
		}
		errOut := errBuf.String()
		if !strings.Contains(errOut, "pdw version") {
			t.Fatalf("%s should name `pdw version`, got: %s", arg, errOut)
		}
		if strings.Contains(errOut, "AUTO-UPDATE") {
			t.Fatalf("%s dumped the full usage instead of a one-line redirect:\n%s", arg, errOut)
		}
	}
}

func TestUnknownCommandRedirectsToTheRealOne(t *testing.T) {
	for _, c := range []struct{ command, want string }{
		{"query", "pdw sql"},
		{"schema_overview", "pdw schema"},
		{"describe_table", "pdw columns"},
	} {
		var outBuf, errBuf bytes.Buffer
		code := run([]string{c.command, "x"}, strings.NewReader(""), &outBuf, &errBuf, func(string) string { return "" })
		if code == 0 {
			t.Fatalf("`pdw %s` should not succeed", c.command)
		}
		errOut := errBuf.String()
		if !strings.Contains(errOut, c.want) {
			t.Fatalf("`pdw %s` should redirect to %q, got: %s", c.command, c.want, errOut)
		}
		if strings.Contains(errOut, "AUTO-UPDATE") {
			t.Fatalf("`pdw %s` dumped the full usage instead of a one-line redirect:\n%s", c.command, errOut)
		}
	}
}

func TestCallRedirectsCatalogToolsToTheirCommands(t *testing.T) {
	// C8, single path: `pdw schema` / `pdw columns` render these tools' CSV as
	// text. Going through `call` returns raw JSON of the same thing, so it is a
	// second path to the same answer with a worse result.
	for _, c := range []struct{ tool, want string }{
		{"schema_overview", "pdw schema"},
		{"describe_table", "pdw columns"},
	} {
		srv := newStubServer(t, func(http.ResponseWriter, *http.Request) {
			t.Fatalf("server should not be hit for call %s", c.tool)
		})
		_, errOut, code := runCLI(t, srv.URL, "", "call", c.tool)
		if code == 0 {
			t.Fatalf("call %s should be redirected, not run", c.tool)
		}
		if !strings.Contains(errOut, c.want) {
			t.Fatalf("call %s should redirect to %q, got: %s", c.tool, c.want, errOut)
		}
	}
}

func TestUsageExamplesDoNotTeachTheRedundantCallPath(t *testing.T) {
	// The EXAMPLES block taught `pdw call schema_overview` — the exact path the
	// call fence now blocks.
	for _, forbidden := range []string{"pdw call schema_overview", "pdw call describe_table", "pdw call sql", "pdw call query"} {
		if strings.Contains(usage, forbidden) {
			t.Fatalf("usage still teaches the blocked path %q", forbidden)
		}
	}
}
