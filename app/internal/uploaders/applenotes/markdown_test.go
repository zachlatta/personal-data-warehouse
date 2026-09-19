package applenotes

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// testdata/markdownify_cases.jsonl: [html, markdownify(html), .strip()] as
// produced by the markdownify version the Python uploader used.
func TestHTMLToMarkdownMatchesMarkdownify(t *testing.T) {
	file, err := os.Open(filepath.Join("testdata", "markdownify_cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var c []string
		if err := json.Unmarshal(scanner.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		if got := HTMLToMarkdown(c[0]); got != c[2] {
			t.Errorf("%q:\n got %q\nwant %q", c[0], got, c[2])
		}
		count++
	}
	if count < 15 {
		t.Fatalf("only %d cases", count)
	}
}

func TestPyHTMLEscape(t *testing.T) {
	if got := PyHTMLEscape(`a<b>&"c'd`); got != "a&lt;b&gt;&amp;&quot;c&#x27;d" {
		t.Fatalf("PyHTMLEscape = %q", got)
	}
	if got := PlainTextFromHTML("<p>a  b</p>\n<i>c</i>"); got != "a b c" {
		t.Fatalf("PlainTextFromHTML = %q", got)
	}
}
