package mutations

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The email editor's allow-list exists three times: here (authoritative for
// stored editor_html), in the web composer (mutation_view.js, for loaded and
// pasted HTML) and inside the phone's WebView editor (email-editor.ts). A tag
// or attribute added to one and not the others is kept on one surface and
// stripped on the next save from another, so the three sets must stay equal.
func TestEmailEditorAllowListsMatchOnServerWebAndPhone(t *testing.T) {
	keys := func(set map[string]bool) []string {
		out := make([]string, 0, len(set))
		for key := range set {
			out = append(out, key)
		}
		sort.Strings(out)
		return out
	}
	read := func(path string) string {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	extract := func(source, name string, block, item *regexp.Regexp) []string {
		match := block.FindStringSubmatch(source)
		if match == nil {
			t.Fatalf("%s not found", name)
		}
		var out []string
		for _, m := range item.FindAllStringSubmatch(match[1], -1) {
			out = append(out, m[1])
		}
		sort.Strings(out)
		return out
	}
	web := read("../webapp/static/mutation_view.js")
	phone := read("../../../mobile/src/lib/email-editor.ts")
	quoted := regexp.MustCompile(`"([a-z0-9]+)"`)
	phoneKey := regexp.MustCompile(`([a-z0-9]+):1`)
	for _, c := range []struct {
		goName, webName, phoneName string
		server                     map[string]bool
	}{
		{"emailSanitizeDropSubtree", "DROP_SUBTREE_ELEMENTS", "DROP", emailSanitizeDropSubtree},
		{"emailSanitizeAllowedElements", "ALLOWED_EDITOR_ELEMENTS", "ALLOW_TAG", emailSanitizeAllowedElements},
		{"emailSanitizeAllowedAttrs", "ALLOWED_EDITOR_ATTRS", "ALLOW_ATTR", emailSanitizeAllowedAttrs},
	} {
		want := strings.Join(keys(c.server), ",")
		webSet := extract(web, c.webName, regexp.MustCompile(`(?s)const `+c.webName+` = new Set\(\[(.*?)\]\)`), quoted)
		phoneSet := extract(phone, c.phoneName, regexp.MustCompile(`var `+c.phoneName+` = \{(.*?)\};`), phoneKey)
		if got := strings.Join(webSet, ","); got != want {
			t.Errorf("web %s != server %s:\n web    %s\n server %s", c.webName, c.goName, got, want)
		}
		if got := strings.Join(phoneSet, ","); got != want {
			t.Errorf("phone %s != server %s:\n phone  %s\n server %s", c.phoneName, c.goName, got, want)
		}
	}
}
