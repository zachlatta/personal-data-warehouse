package applenotes

import (
	"regexp"
	"strings"

	xhtml "golang.org/x/net/html"
)

// HTMLToMarkdown mirrors the subset of markdownify (v1.2, default options:
// underlined headings, asterisk emphasis, stripped document) the Python
// uploader applied to a note's HTML. Every real note reaches it as
// <html><body><pre>escaped text</pre></body></html>, which converts to a
// fenced block whose content is the note text; the other tags are covered
// so a note whose decoded body happened to look like HTML converts the way
// it did before.
func HTMLToMarkdown(source string) string {
	// html.parser keeps carriage returns; Go's HTML5 parser normalizes them
	// away, so hide them through the parse.
	const crPlaceholder = "\uE000"
	source = strings.ReplaceAll(source, "\r", crPlaceholder)
	doc, err := xhtml.Parse(strings.NewReader(source))
	if err != nil {
		return strings.TrimSpace(strings.ReplaceAll(source, "\n", " "))
	}
	c := &converter{}
	text := c.processNode(doc, map[string]bool{})
	text = strings.ReplaceAll(text, crPlaceholder, "\r")
	return strings.Trim(text, "\n")
}

type converter struct{}

var (
	reWhitespace        = regexp.MustCompile(`[\t ]+`)
	reAllWhitespace     = regexp.MustCompile(`[\t \r\n]+`)
	reNewlineWhitespace = regexp.MustCompile(`[\t \r\n]*[\r\n][\t \r\n]*`)
	rePreLstrip         = regexp.MustCompile(`^[ \n]*\n`)
	rePreRstrip         = regexp.MustCompile(`[ \n]*$`)
	reHeading           = regexp.MustCompile(`^h(\d+)$`)
)

func isBlockInside(name string) bool {
	if reHeading.MatchString(name) {
		return true
	}
	switch name {
	case "p", "blockquote", "article", "div", "section", "ol", "ul", "li", "dl", "dt", "dd", "table", "thead", "tbody", "tfoot", "tr", "td", "th":
		return true
	}
	return false
}

func nodeName(n *xhtml.Node) string {
	if n == nil {
		return ""
	}
	switch n.Type {
	case xhtml.ElementNode:
		return n.Data
	case xhtml.DocumentNode:
		return "[document]"
	}
	return ""
}

func removeWhitespaceInside(n *xhtml.Node) bool {
	return n != nil && n.Type == xhtml.ElementNode && isBlockInside(n.Data)
}

func removeWhitespaceOutside(n *xhtml.Node) bool {
	return removeWhitespaceInside(n) || (n != nil && n.Type == xhtml.ElementNode && n.Data == "pre")
}

func isWhitespaceText(n *xhtml.Node) bool {
	return n != nil && n.Type == xhtml.TextNode && strings.TrimSpace(n.Data) == ""
}

func (c *converter) canIgnore(n *xhtml.Node, removeInside bool) bool {
	switch n.Type {
	case xhtml.ElementNode:
		return false
	case xhtml.CommentNode, xhtml.DoctypeNode:
		return true
	case xhtml.TextNode:
		if strings.TrimSpace(n.Data) != "" {
			return false
		}
		if removeInside && (n.PrevSibling == nil || n.NextSibling == nil) {
			return true
		}
		return removeWhitespaceOutside(n.PrevSibling) || removeWhitespaceOutside(n.NextSibling)
	}
	return true
}

func copyTags(tags map[string]bool) map[string]bool {
	out := make(map[string]bool, len(tags)+2)
	for k, v := range tags {
		out[k] = v
	}
	return out
}

func hasPreParent(n *xhtml.Node) bool {
	for p := n; p != nil; p = p.Parent {
		if p.Type == xhtml.ElementNode && p.Data == "pre" {
			return true
		}
	}
	return false
}

func (c *converter) processNode(n *xhtml.Node, parentTags map[string]bool) string {
	name := nodeName(n)
	removeInside := removeWhitespaceInside(n)
	childTags := copyTags(parentTags)
	childTags[name] = true
	if reHeading.MatchString(name) || name == "td" || name == "th" {
		childTags["_inline"] = true
	}
	if name == "pre" || name == "code" || name == "kbd" || name == "samp" {
		childTags["_noformat"] = true
	}
	var childStrings []string
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if c.canIgnore(child, removeInside) {
			continue
		}
		var s string
		if child.Type == xhtml.TextNode {
			s = c.processText(child, childTags)
		} else {
			s = c.processNode(child, childTags)
		}
		if s != "" {
			childStrings = append(childStrings, s)
		}
	}
	var text string
	if name == "pre" || hasPreParent(n) {
		text = strings.Join(childStrings, "")
	} else {
		updated := []string{""}
		for _, child := range childStrings {
			leading, content, trailing := extractNewlines(child)
			if updated[len(updated)-1] != "" && leading != "" {
				prev := updated[len(updated)-1]
				updated = updated[:len(updated)-1]
				n := len(prev)
				if len(leading) > n {
					n = len(leading)
				}
				if n > 2 {
					n = 2
				}
				leading = strings.Repeat("\n", n)
			}
			updated = append(updated, leading, content, trailing)
		}
		text = strings.Join(updated, "")
	}
	return c.convertTag(n, name, text, parentTags)
}

func extractNewlines(s string) (string, string, string) {
	leading := 0
	for leading < len(s) && s[leading] == '\n' {
		leading++
	}
	if leading == len(s) {
		return s, "", ""
	}
	trailing := len(s)
	for trailing > leading && s[trailing-1] == '\n' {
		trailing--
	}
	return s[:leading], s[leading:trailing], s[trailing:]
}

func (c *converter) processText(n *xhtml.Node, parentTags map[string]bool) string {
	text := n.Data
	if !parentTags["pre"] {
		text = reNewlineWhitespace.ReplaceAllString(text, "\n")
		text = reWhitespace.ReplaceAllString(text, " ")
	}
	if !parentTags["_noformat"] && text != "" {
		text = strings.ReplaceAll(text, "*", `\*`)
		text = strings.ReplaceAll(text, "_", `\_`)
	}
	if removeWhitespaceOutside(n.PrevSibling) || (removeWhitespaceInside(n.Parent) && n.PrevSibling == nil) {
		text = strings.TrimLeft(text, " \t\r\n")
	}
	if removeWhitespaceOutside(n.NextSibling) || (removeWhitespaceInside(n.Parent) && n.NextSibling == nil) {
		text = strings.TrimRight(text, " \t\r\n\v\f")
	}
	return text
}

func chomp(text string) (string, string, string) {
	prefix, suffix := "", ""
	if text != "" && text[0] == ' ' {
		prefix = " "
	}
	if text != "" && text[len(text)-1] == ' ' {
		suffix = " "
	}
	return prefix, suffix, strings.TrimSpace(text)
}

func inlineWrap(markup, text string, parentTags map[string]bool) string {
	if parentTags["_noformat"] {
		return text
	}
	prefix, suffix, inner := chomp(text)
	if inner == "" {
		return ""
	}
	return prefix + markup + inner + markup + suffix
}

func attr(n *xhtml.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func (c *converter) convertTag(n *xhtml.Node, name, text string, parentTags map[string]bool) string {
	switch name {
	case "[document]":
		return text
	case "pre":
		if text == "" {
			return ""
		}
		text = rePreLstrip.ReplaceAllString(text, "")
		text = rePreRstrip.ReplaceAllString(text, "")
		return "\n\n```\n" + text + "\n```\n\n"
	case "p":
		if parentTags["_inline"] {
			return " " + strings.Trim(text, " \t\r\n") + " "
		}
		text = strings.Trim(text, " \t\r\n")
		if text == "" {
			return ""
		}
		return "\n\n" + text + "\n\n"
	case "div", "article", "section", "dl":
		if parentTags["_inline"] {
			return " " + strings.TrimSpace(text) + " "
		}
		text = strings.TrimSpace(text)
		if text == "" {
			return ""
		}
		return "\n\n" + text + "\n\n"
	case "br":
		if parentTags["_inline"] {
			return " "
		}
		return "  \n"
	case "b", "strong":
		return inlineWrap("**", text, parentTags)
	case "i", "em":
		return inlineWrap("*", text, parentTags)
	case "del", "s":
		return inlineWrap("~~", text, parentTags)
	case "hr":
		return "\n\n---\n\n"
	case "a":
		if parentTags["_noformat"] {
			return text
		}
		prefix, suffix, inner := chomp(text)
		if inner == "" {
			return ""
		}
		href := attr(n, "href")
		title := attr(n, "title")
		if strings.ReplaceAll(inner, `\_`, "_") == href && title == "" {
			return "<" + href + ">"
		}
		if href == "" {
			return inner
		}
		titlePart := ""
		if title != "" {
			titlePart = ` "` + strings.ReplaceAll(title, `"`, `\"`) + `"`
		}
		return prefix + "[" + inner + "](" + href + titlePart + ")" + suffix
	case "ul", "ol":
		if parentTags["li"] {
			return "\n" + strings.TrimRight(text, " \t\r\n")
		}
		before := false
		for sib := n.NextSibling; sib != nil; sib = sib.NextSibling {
			if sib.Type == xhtml.ElementNode {
				before = sib.Data != "ul" && sib.Data != "ol"
				break
			}
			if sib.Type == xhtml.TextNode && strings.TrimSpace(sib.Data) != "" {
				before = true
				break
			}
		}
		if before {
			return "\n\n" + text + "\n"
		}
		return "\n\n" + text
	case "li":
		return convertLi(n, text)
	case "table":
		return "\n\n" + strings.TrimSpace(text) + "\n\n"
	case "td", "th":
		return " " + strings.ReplaceAll(strings.TrimSpace(text), "\n", " ") + " |"
	case "tr":
		return convertTr(n, text)
	case "blockquote":
		text = strings.Trim(text, " \t\r\n")
		if parentTags["_inline"] {
			return " " + text + " "
		}
		if text == "" {
			return "\n"
		}
		var lines []string
		for _, line := range strings.Split(text, "\n") {
			if line == "" {
				lines = append(lines, ">")
			} else {
				lines = append(lines, "> "+line)
			}
		}
		return "\n" + strings.Join(lines, "\n") + "\n\n"
	case "code", "kbd", "samp":
		if parentTags["_noformat"] {
			return text
		}
		prefix, suffix, inner := chomp(text)
		if inner == "" {
			return ""
		}
		return prefix + "`" + inner + "`" + suffix
	case "script", "style":
		return ""
	case "img":
		alt := attr(n, "alt")
		if parentTags["_inline"] {
			return alt
		}
		return "![" + alt + "](" + attr(n, "src") + ")"
	}
	if m := reHeading.FindStringSubmatch(name); m != nil {
		if parentTags["_inline"] {
			return text
		}
		level := int(m[1][0] - '0')
		if len(m[1]) > 1 {
			level = 6
		}
		if level < 1 {
			level = 1
		}
		text = strings.TrimSpace(text)
		if level <= 2 {
			text = strings.TrimRight(text, " \t\r\n")
			if text == "" {
				return ""
			}
			pad := "="
			if level == 2 {
				pad = "-"
			}
			return "\n\n" + text + "\n" + strings.Repeat(pad, len([]rune(text))) + "\n\n"
		}
		text = reAllWhitespace.ReplaceAllString(text, " ")
		return "\n\n" + strings.Repeat("#", level) + " " + text + "\n\n"
	}
	return text
}

func convertLi(n *xhtml.Node, text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "\n"
	}
	bullet := ""
	if n.Parent != nil && n.Parent.Type == xhtml.ElementNode && n.Parent.Data == "ol" {
		start := 1
		if s := attr(n.Parent, "start"); s != "" {
			start = 0
			for _, ch := range s {
				if ch < '0' || ch > '9' {
					start = 1
					break
				}
				start = start*10 + int(ch-'0')
			}
		}
		index := 0
		for sib := n.PrevSibling; sib != nil; sib = sib.PrevSibling {
			if sib.Type == xhtml.ElementNode && sib.Data == "li" {
				index++
			}
		}
		bullet = strings.Repeat("", 0) + itoa(start+index) + "."
	} else {
		depth := -1
		for p := n; p != nil; p = p.Parent {
			if p.Type == xhtml.ElementNode && p.Data == "ul" {
				depth++
			}
		}
		bullets := "*+-"
		if depth < 0 {
			depth = 0
		}
		bullet = string(bullets[depth%len(bullets)])
	}
	bullet += " "
	indent := strings.Repeat(" ", len(bullet))
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = indent + line
		}
	}
	text = strings.Join(lines, "\n")
	return bullet + text[len(bullet):] + "\n"
}

func convertTr(n *xhtml.Node, text string) string {
	var cells []*xhtml.Node
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == xhtml.ElementNode && (child.Data == "td" || child.Data == "th") {
			cells = append(cells, child)
		}
	}
	isFirstRow := true
	for sib := n.PrevSibling; sib != nil; sib = sib.PrevSibling {
		if sib.Type == xhtml.ElementNode {
			isFirstRow = false
			break
		}
	}
	allTh := len(cells) > 0
	for _, cell := range cells {
		if cell.Data != "th" {
			allTh = false
		}
	}
	parentName := nodeName(n.Parent)
	isHeadRow := allTh || parentName == "thead"
	isHeadRowMissing := isFirstRow && parentName != "tbody"
	if isFirstRow && parentName == "tbody" {
		hasThead := false
		if n.Parent.Parent != nil {
			for sib := n.Parent.Parent.FirstChild; sib != nil; sib = sib.NextSibling {
				if sib.Type == xhtml.ElementNode && sib.Data == "thead" {
					hasThead = true
				}
			}
		}
		isHeadRowMissing = !hasThead
	}
	sep := "| " + strings.Join(repeatSlice("---", len(cells)), " | ") + " |\n"
	if isHeadRow && isFirstRow {
		return "|" + text + "\n" + sep
	}
	if isHeadRowMissing {
		overline := "| " + strings.Join(repeatSlice("", len(cells)), " | ") + " |\n" + sep
		return overline + "|" + text + "\n"
	}
	return "|" + text + "\n"
}

func repeatSlice(value string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = value
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}

// PyHTMLEscape mirrors Python's html.escape(value, quote=True).
func PyHTMLEscape(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	value = strings.ReplaceAll(value, ">", "&gt;")
	value = strings.ReplaceAll(value, `"`, "&quot;")
	value = strings.ReplaceAll(value, "'", "&#x27;")
	return value
}

// PlainTextFromHTML mirrors _plain_text_from_html: tags stripped to spaces,
// whitespace collapsed.
func PlainTextFromHTML(value string) string {
	stripped := reTag.ReplaceAllString(value, " ")
	return strings.TrimSpace(reAnyWhitespace.ReplaceAllString(stripped, " "))
}

var (
	reTag           = regexp.MustCompile(`<[^>]+>`)
	reAnyWhitespace = regexp.MustCompile(`\s+`)
)
