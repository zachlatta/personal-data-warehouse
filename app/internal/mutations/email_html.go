package mutations

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// ErrOldClientEditUnsupported is returned when an older plain-text client
// submits an edit against a rich HTML email that cannot be provably mapped onto
// the stored HTML without losing text or formatting.
var ErrOldClientEditUnsupported = errors.New(
	"this email has formatting that an older app version cannot safely edit in place; update the PDW app or edit this email on the web",
)

var (
	// Elements dropped together with their subtrees during sanitization.
	emailSanitizeDropSubtree = map[string]bool{
		"script":    true,
		"style":     true,
		"svg":       true,
		"math":      true,
		"iframe":    true,
		"frame":     true,
		"frameset":  true,
		"object":    true,
		"embed":     true,
		"applet":    true,
		"noscript":  true,
		"template":  true,
		"form":      true,
		"input":     true,
		"button":    true,
		"select":    true,
		"textarea":  true,
		"base":      true,
		"meta":      true,
		"link":      true,
		"xmp":       true,
		"noembed":   true,
		"plaintext": true,
		"audio":     true,
		"video":     true,
		"source":    true,
		"canvas":    true,
		"marquee":   true,
	}

	// Explicit allow-list of safe email formatting elements. Any element not in
	// either map is unwrapped (keeping its child text/elements).
	emailSanitizeAllowedElements = map[string]bool{
		"p": true, "div": true, "br": true, "span": true,
		"strong": true, "b": true, "em": true, "i": true, "u": true,
		"s": true, "strike": true, "del": true, "ins": true, "sub": true, "sup": true,
		"a": true, "ul": true, "ol": true, "li": true,
		"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true,
		"blockquote": true, "pre": true, "code": true, "hr": true,
		"table": true, "thead": true, "tbody": true, "tfoot": true,
		"tr": true, "th": true, "td": true, "caption": true, "colgroup": true, "col": true,
		"img": true, "font": true, "center": true, "small": true, "big": true,
		"abbr": true, "cite": true, "dl": true, "dt": true, "dd": true,
		"section": true, "article": true, "header": true, "footer": true,
	}

	// Explicit allow-list of safe attributes. Event handlers (on*), srcset,
	// formaction, etc. are never allowed.
	emailSanitizeAllowedAttrs = map[string]bool{
		"href": true, "src": true, "alt": true, "title": true,
		"target": true, "rel": true, "style": true, "class": true,
		"dir": true, "align": true, "valign": true,
		"width": true, "height": true, "colspan": true, "rowspan": true,
		"cellpadding": true, "cellspacing": true, "border": true,
		"color": true, "face": true, "size": true, "start": true, "type": true,
		"role": true, "bgcolor": true, "background": true, "nowrap": true,
	}

	emailListPrefixPattern = regexp.MustCompile(`^\s*(?:\d+\.\s+|[-*•]\s+)`)

	emailInlineFormatTags = map[string]bool{
		"strong": true, "b": true, "em": true, "i": true, "a": true, "u": true, "s": true, "code": true,
	}
)

func parseHTMLFragment(value string) (*html.Node, error) {
	ctx := &html.Node{
		Type:     html.ElementNode,
		DataAtom: atom.Div,
		Data:     "div",
	}
	nodes, err := html.ParseFragment(strings.NewReader(value), ctx)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		ctx.AppendChild(node)
	}
	return ctx, nil
}

func renderHTMLChildren(parent *html.Node) string {
	var buf bytes.Buffer
	for child := parent.FirstChild; child != nil; child = child.NextSibling {
		if err := html.Render(&buf, child); err != nil {
			return ""
		}
	}
	return buf.String()
}

// sanitizeEmailEditorHTML parses an HTML fragment with golang.org/x/net/html
// and enforces a strict element and attribute allow-list, removing executable
// markup while preserving all email formatting and never altering visible text.
func sanitizeEmailEditorHTML(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	root, err := parseHTMLFragment(value)
	if err != nil {
		return ""
	}
	sanitizeHTMLNodeChildren(root)
	return strings.TrimSpace(renderHTMLChildren(root))
}

func sanitizeHTMLNodeChildren(parent *html.Node) {
	for child := parent.FirstChild; child != nil; {
		next := child.NextSibling
		switch child.Type {
		case html.CommentNode:
			parent.RemoveChild(child)
		case html.ElementNode:
			tag := strings.ToLower(child.Data)
			if emailSanitizeDropSubtree[tag] {
				parent.RemoveChild(child)
				child = next
				continue
			}
			if !emailSanitizeAllowedElements[tag] {
				// Unwrap unknown non-dangerous elements so their text/children survive.
				sanitizeHTMLNodeChildren(child)
				for grand := child.FirstChild; grand != nil; {
					gNext := grand.NextSibling
					child.RemoveChild(grand)
					parent.InsertBefore(grand, child)
					grand = gNext
				}
				parent.RemoveChild(child)
				child = next
				continue
			}
			filtered := child.Attr[:0]
			for _, attr := range child.Attr {
				key := strings.ToLower(strings.TrimSpace(attr.Key))
				if !emailSanitizeAllowedAttrs[key] {
					continue
				}
				if key == "href" || key == "src" || key == "background" {
					if !isSafeEmailURL(attr.Val, key != "href") {
						attr.Val = "#"
					}
				} else if key == "style" {
					if !isSafeEmailStyle(attr.Val) {
						continue
					}
				}
				filtered = append(filtered, attr)
			}
			child.Attr = filtered
			sanitizeHTMLNodeChildren(child)
		}
		child = next
	}
}

func isSafeEmailURL(raw string, isSrc bool) bool {
	cleaned := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return -1
		}
		return unicode.ToLower(r)
	}, raw)
	if cleaned == "" || strings.HasPrefix(cleaned, "#") || strings.HasPrefix(cleaned, "/") {
		return true
	}
	for _, prefix := range []string{"http://", "https://", "mailto:", "tel:", "cid:"} {
		if strings.HasPrefix(cleaned, prefix) {
			return true
		}
	}
	if isSrc {
		for _, prefix := range []string{"data:image/png", "data:image/jpeg", "data:image/jpg", "data:image/gif", "data:image/webp"} {
			if strings.HasPrefix(cleaned, prefix) {
				return true
			}
		}
	}
	// Bare relative path with no scheme colon.
	if !strings.Contains(cleaned, ":") {
		return true
	}
	return false
}

func isSafeEmailStyle(raw string) bool {
	lower := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return -1
		}
		return unicode.ToLower(r)
	}, raw)
	for _, bad := range []string{"javascript:", "vbscript:", "expression(", "@import", "behavior:", "-moz-binding"} {
		if strings.Contains(lower, bad) {
			return false
		}
	}
	if idx := strings.Index(lower, "url("); idx >= 0 {
		rest := strings.TrimLeft(lower[idx+4:], `"'`)
		if !strings.HasPrefix(rest, "https://") && !strings.HasPrefix(rest, "http://") {
			return false
		}
	}
	return true
}

// hasRichEmailFormatting reports whether editorHTML contains formatting beyond
// plain Gmail-style <div>/<br>/text lines.
func hasRichEmailFormatting(editorHTML string) bool {
	if strings.TrimSpace(editorHTML) == "" {
		return false
	}
	root, err := parseHTMLFragment(editorHTML)
	if err != nil {
		return false
	}
	rich := false
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		if rich {
			return
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if child.Type == html.ElementNode {
				tag := strings.ToLower(child.Data)
				if tag != "div" && tag != "br" {
					rich = true
					return
				}
				if tag == "div" {
					for _, attr := range child.Attr {
						if strings.ToLower(attr.Key) != "dir" {
							rich = true
							return
						}
					}
				}
				walk(child)
			}
		}
	}
	walk(root)
	return rich
}

// isPlainDivHTML reports whether editorHTML consists solely of unattributed
// <div>, <br>, and text nodes — the output of emailPlainTextToHTML used by
// older plain-text clients.
func isPlainDivHTML(editorHTML string) bool {
	if strings.TrimSpace(editorHTML) == "" {
		return true
	}
	return !hasRichEmailFormatting(editorHTML)
}

type htmlTextListState struct {
	ordered bool
	count   int
}

// htmlEmailText is the plain text a reader sees in an email body, preserving
// paragraphs, line breaks, ordered/unordered list markers, headings, and table
// rows.
func htmlEmailText(value string) string {
	return renderHTMLEmailText(value, true)
}

// htmlEmailTextWithoutListMarkers renders the same text without list markers or
// list-boundary blank lines, matching the pre-fix extractor so an older client
// with a cached pre-deploy editor_text still diffs cleanly on the server.
func htmlEmailTextWithoutListMarkers(value string) string {
	return renderHTMLEmailText(value, false)
}

func renderHTMLEmailText(value string, withListMarkers bool) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	root, err := parseHTMLFragment(strings.ReplaceAll(value, "\r\n", "\n"))
	if err != nil {
		return ""
	}
	lines := []string{}
	var current strings.Builder
	softBlank := false
	listStack := []htmlTextListState{}
	justOpenedLI := false

	emit := func(line string) {
		if softBlank && len(lines) > 0 && line != "" {
			lines = append(lines, "")
		}
		softBlank = false
		lines = append(lines, line)
	}
	pending := func() string {
		collapsed := collapseInlineSpaces(current.String())
		return strings.TrimSpace(collapsed)
	}
	flush := func(keepEmpty bool) {
		text := pending()
		current.Reset()
		if text != "" || keepEmpty {
			emit(text)
		}
	}

	var walk func(n *html.Node, inPre bool)
	walk = func(n *html.Node, inPre bool) {
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			switch child.Type {
			case html.TextNode:
				if inPre {
					parts := strings.Split(child.Data, "\n")
					for i, part := range parts {
						if i > 0 {
							emit(strings.TrimRight(current.String(), " \t"))
							current.Reset()
						}
						current.WriteString(part)
					}
				} else {
					current.WriteString(child.Data)
				}
			case html.ElementNode:
				tag := strings.ToLower(child.Data)
				if tag == "script" || tag == "style" || tag == "noscript" || tag == "template" {
					continue
				}
				switch tag {
				case "br":
					if isTrailingBRInBlock(child) {
						if pending() == "" && !hasMeaningfulSiblingBefore(child) {
							flush(true)
						}
						continue
					}
					flush(true)
				case "p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote":
					if tag == "p" && justOpenedLI {
						justOpenedLI = false
						walk(child, inPre)
						flush(false)
						break
					}
					justOpenedLI = false
					flush(false)
					if isEmptySpacerBlock(child) {
						flush(true)
					} else {
						walk(child, inPre)
						flush(false)
					}
					softBlank = true
				case "pre":
					flush(false)
					walk(child, true)
					flush(false)
					softBlank = true
				case "ol", "ul":
					flush(false)
					if withListMarkers && len(listStack) == 0 && len(lines) > 0 {
						softBlank = true
					}
					listStack = append(listStack, htmlTextListState{ordered: tag == "ol"})
					walk(child, inPre)
					listStack = listStack[:len(listStack)-1]
					flush(false)
					if withListMarkers && len(listStack) == 0 {
						softBlank = true
					}
				case "li":
					flush(false)
					if withListMarkers && len(listStack) > 0 {
						top := &listStack[len(listStack)-1]
						top.count++
						indent := strings.Repeat("  ", len(listStack)-1)
						if top.ordered {
							current.WriteString(fmt.Sprintf("%s%d. ", indent, top.count))
						} else {
							current.WriteString(indent + "- ")
						}
						justOpenedLI = true
					}
					walk(child, inPre)
					justOpenedLI = false
					flush(false)
				case "table":
					flush(false)
					if withListMarkers && len(lines) > 0 {
						softBlank = true
					}
					walk(child, inPre)
					flush(false)
					if withListMarkers {
						softBlank = true
					}
				case "tr":
					flush(false)
					cells := []string{}
					for cell := child.FirstChild; cell != nil; cell = cell.NextSibling {
						if cell.Type == html.ElementNode {
							ctag := strings.ToLower(cell.Data)
							if ctag == "td" || ctag == "th" {
								cellText := strings.TrimSpace(renderHTMLEmailText(renderHTMLChildren(cell), withListMarkers))
								cellText = strings.ReplaceAll(cellText, "\n", " ")
								cells = append(cells, cellText)
							}
						}
					}
					if len(cells) > 0 {
						emit(strings.Join(cells, "\t"))
					}
				case "hr":
					flush(false)
					emit("---")
					softBlank = true
				case "div", "center", "section", "article", "header", "footer":
					flush(false)
					if isEmptySpacerBlock(child) {
						flush(true)
					} else {
						walk(child, inPre)
						flush(false)
					}
				default:
					walk(child, inPre)
				}
			}
		}
	}

	walk(root, false)
	flush(false)
	for len(lines) > 0 && lines[0] == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return strings.Join(lines, "\n")
}

func collapseInlineSpaces(s string) string {
	var out strings.Builder
	inSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !inSpace {
				out.WriteByte(' ')
				inSpace = true
			}
			continue
		}
		inSpace = false
		out.WriteRune(r)
	}
	return out.String()
}

func isEmptySpacerBlock(n *html.Node) bool {
	sawBR := false
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		switch child.Type {
		case html.TextNode:
			if strings.TrimSpace(child.Data) != "" {
				return false
			}
		case html.ElementNode:
			if strings.ToLower(child.Data) == "br" && !sawBR {
				sawBR = true
				continue
			}
			return false
		}
	}
	return sawBR
}

func isTrailingBRInBlock(br *html.Node) bool {
	for sib := br.NextSibling; sib != nil; sib = sib.NextSibling {
		if sib.Type == html.TextNode && strings.TrimSpace(sib.Data) == "" {
			continue
		}
		return false
	}
	return br.Parent != nil && br.Parent.Type == html.ElementNode && isBlockContainerTag(strings.ToLower(br.Parent.Data))
}

func hasMeaningfulSiblingBefore(n *html.Node) bool {
	for sib := n.PrevSibling; sib != nil; sib = sib.PrevSibling {
		if sib.Type == html.TextNode && strings.TrimSpace(sib.Data) == "" {
			continue
		}
		return true
	}
	return false
}

func isBlockContainerTag(tag string) bool {
	switch tag {
	case "div", "p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre",
		"td", "th", "ul", "ol", "table", "tr", "thead", "tbody", "tfoot",
		"center", "section", "article", "header", "footer", "dl", "dt", "dd":
		return true
	}
	return false
}

// splitEmailQuotedText separates a plain-text email body from its trailing
// quoted reply ("On ... wrote:") or forwarded-message header block.
func splitEmailQuotedText(bodyText string) (string, string) {
	normalized := strings.ReplaceAll(bodyText, "\r\n", "\n")
	if idx := strings.Index(normalized, "\n---------- Forwarded message ---------"); idx >= 0 {
		return strings.TrimRight(normalized[:idx], "\n"), strings.TrimSpace(normalized[idx+1:])
	}
	lines := strings.Split(normalized, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "On ") && strings.HasSuffix(trimmed, " wrote:") {
			return strings.TrimRight(strings.Join(lines[:i], "\n"), "\n"), strings.TrimSpace(strings.Join(lines[i:], "\n"))
		}
	}
	return normalized, ""
}

func normalizeForNoOpCompare(value string) string {
	lines := strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(collapseInlineSpaces(strings.ReplaceAll(line, "\u00a0", " ")))
	}
	for len(lines) > 0 && lines[0] == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return strings.Join(lines, "\n")
}

// logicalLine represents one visible line extracted from an HTML DOM fragment,
// owning the exact text nodes and DOM range belonging to that line.
type logicalLine struct {
	text         string
	textNodes    []*html.Node // text nodes belonging strictly to this line
	segmentNodes []*html.Node // top-level nodes inside parent for a bare/br segment
	blockNode    *html.Node   // non-nil when this line is the sole line of a block element
	parentNode   *html.Node   // parent element holding segmentNodes (for br/bare segments)
	trailingBR   *html.Node   // <br> immediately ending this segment, if any
	leadingBR    *html.Node   // <br> immediately preceding this segment, if any
	cells        []*html.Node // for <tr>: <td>/<th> elements
	isList       bool
	isTR         bool
	isHR         bool
	inPre        bool
}

func collectLogicalLines(root *html.Node) []logicalLine {
	var out []logicalLine

	var walkBlock func(container *html.Node, inList bool)
	walkBlock = func(container *html.Node, inList bool) {
		var segNodes []*html.Node
		var segTextNodes []*html.Node
		var segBuf strings.Builder
		var prevBR *html.Node
		firstLineInContainer := true

		flushSegment := func(endBR *html.Node) {
			txt := strings.TrimSpace(collapseInlineSpaces(segBuf.String()))
			if txt != "" {
				// Determine if this segment is the sole content of a non-root block element.
				var blk *html.Node
				if container != root && endBR == nil && prevBR == nil && !hasBlockChild(container) && !hasBRChild(container) {
					blk = container
				}
				out = append(out, logicalLine{
					text:         txt,
					textNodes:    append([]*html.Node(nil), segTextNodes...),
					segmentNodes: append([]*html.Node(nil), segNodes...),
					blockNode:    blk,
					parentNode:   container,
					leadingBR:    prevBR,
					trailingBR:   endBR,
					isList:       inList && firstLineInContainer,
				})
				firstLineInContainer = false
			}
			segNodes = nil
			segTextNodes = nil
			segBuf.Reset()
			prevBR = endBR
		}

		var collectInline func(n *html.Node)
		collectInline = func(n *html.Node) {
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				switch c.Type {
				case html.TextNode:
					segTextNodes = append(segTextNodes, c)
					segBuf.WriteString(c.Data)
				case html.ElementNode:
					tag := strings.ToLower(c.Data)
					if tag == "script" || tag == "style" || tag == "noscript" || tag == "template" {
						continue
					}
					if tag == "br" {
						// Nested <br> inside an inline wrapper: treat as line break text.
						segBuf.WriteByte('\n')
						continue
					}
					collectInline(c)
				}
			}
		}

		for child := container.FirstChild; child != nil; child = child.NextSibling {
			switch child.Type {
			case html.TextNode:
				segNodes = append(segNodes, child)
				segTextNodes = append(segTextNodes, child)
				segBuf.WriteString(child.Data)
			case html.ElementNode:
				tag := strings.ToLower(child.Data)
				if tag == "script" || tag == "style" || tag == "noscript" || tag == "template" {
					continue
				}
				if tag == "br" {
					if isTrailingBRInBlock(child) && strings.TrimSpace(segBuf.String()) != "" {
						continue
					}
					flushSegment(child)
					continue
				}
				if tag == "hr" {
					flushSegment(nil)
					out = append(out, logicalLine{
						text:       "---",
						blockNode:  child,
						parentNode: container,
						isHR:       true,
					})
					firstLineInContainer = false
					continue
				}
				if tag == "tr" {
					flushSegment(nil)
					var cells []*html.Node
					var cellTexts []string
					for cell := child.FirstChild; cell != nil; cell = cell.NextSibling {
						if cell.Type == html.ElementNode {
							ctag := strings.ToLower(cell.Data)
							if ctag == "td" || ctag == "th" {
								cells = append(cells, cell)
								ct := strings.TrimSpace(renderHTMLEmailText(renderHTMLChildren(cell), true))
								ct = strings.ReplaceAll(ct, "\n", " ")
								cellTexts = append(cellTexts, ct)
							}
						}
					}
					if len(cells) > 0 {
						out = append(out, logicalLine{
							text:       strings.Join(cellTexts, "\t"),
							blockNode:  child,
							parentNode: container,
							cells:      cells,
							isTR:       true,
						})
						firstLineInContainer = false
					}
					continue
				}
				if tag == "pre" {
					flushSegment(nil)
					// Represent <pre> as a single block line-preserving node only if 1 line,
					// otherwise let verifier catch complex multi-line <pre> edits cleanly.
					preText := collectDirectTextNodesOnly(child)
					preLines := strings.Split(strings.ReplaceAll(preText, "\r\n", "\n"), "\n")
					if len(preLines) == 1 && strings.TrimSpace(preLines[0]) != "" {
						out = append(out, logicalLine{
							text:       strings.TrimSpace(preLines[0]),
							textNodes:  collectAllTextNodes(child),
							blockNode:  child,
							parentNode: container,
							inPre:      true,
						})
					} else {
						for _, pl := range preLines {
							if strings.TrimSpace(pl) != "" {
								out = append(out, logicalLine{
									text:       strings.TrimSpace(pl),
									blockNode:  child,
									parentNode: container,
									inPre:      true,
								})
							}
						}
					}
					firstLineInContainer = false
					continue
				}
				if isBlockContainerTag(tag) {
					flushSegment(nil)
					childInList := tag == "li"
					// If this is a <p> directly inside an <li> and is the first line of the <li>,
					// carry the list flag onto its first line.
					if inList && firstLineInContainer && tag == "p" {
						childInList = true
					}
					walkBlock(child, childInList)
					firstLineInContainer = false
					continue
				}
				// Inline element: stays part of the current segment.
				segNodes = append(segNodes, child)
				collectInline(child)
			}
		}
		flushSegment(nil)
	}

	walkBlock(root, false)
	return out
}

func hasBlockChild(n *html.Node) bool {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && isBlockContainerTag(strings.ToLower(c.Data)) {
			return true
		}
	}
	return false
}

func hasBRChild(n *html.Node) bool {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && strings.ToLower(c.Data) == "br" {
			return true
		}
	}
	return false
}

func collectAllTextNodes(n *html.Node) []*html.Node {
	var out []*html.Node
	var walk func(cur *html.Node)
	walk = func(cur *html.Node) {
		for c := cur.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.TextNode {
				out = append(out, c)
			} else if c.Type == html.ElementNode {
				tag := strings.ToLower(c.Data)
				if tag != "script" && tag != "style" {
					walk(c)
				}
			}
		}
	}
	walk(n)
	return out
}

func collectDirectTextNodesOnly(n *html.Node) string {
	var sb strings.Builder
	for _, tn := range collectAllTextNodes(n) {
		sb.WriteString(tn.Data)
	}
	return sb.String()
}

func stripListPrefix(line string) string {
	return strings.TrimSpace(emailListPrefixPattern.ReplaceAllString(line, ""))
}

func normalizeLineKey(text string, stripList bool) string {
	t := strings.TrimSpace(collapseInlineSpaces(strings.ReplaceAll(text, "\u00a0", " ")))
	if stripList {
		t = stripListPrefix(t)
	}
	return t
}

func normalizePlainEditText(value string) string {
	lines := strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}
	for len(lines) > 0 && lines[0] == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return strings.Join(lines, "\n")
}

type newLineInfo struct {
	text       string
	blankAbove bool
	blankCount int
	hasListPfx bool
}

func parseNewPlainLines(normalized string) []newLineInfo {
	raw := strings.Split(normalized, "\n")
	var out []newLineInfo
	blankCount := 0
	for _, line := range raw {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if len(out) > 0 {
				blankCount++
			}
			continue
		}
		out = append(out, newLineInfo{
			text:       trimmed,
			blankAbove: blankCount > 0,
			blankCount: blankCount,
			hasListPfx: emailListPrefixPattern.MatchString(trimmed),
		})
		blankCount = 0
	}
	return out
}

type idxMatch struct {
	oldIdx int
	newIdx int
}

func computeLCS(m, n int, eq func(i, j int) bool) []idxMatch {
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i := m - 1; i >= 0; i-- {
		for j := n - 1; j >= 0; j-- {
			if eq(i, j) {
				dp[i][j] = dp[i+1][j+1] + 1
			} else if dp[i+1][j] >= dp[i][j+1] {
				dp[i][j] = dp[i+1][j]
			} else {
				dp[i][j] = dp[i][j+1]
			}
		}
	}
	var out []idxMatch
	i, j := 0, 0
	for i < m && j < n {
		if eq(i, j) {
			out = append(out, idxMatch{oldIdx: i, newIdx: j})
			i++
			j++
		} else if dp[i+1][j] >= dp[i][j+1] {
			i++
		} else {
			j++
		}
	}
	return out
}

const (
	maxOldClientLCSLines = 1500
	maxOldClientLCSCells = 1_000_000
	maxWordLCSTokens     = 2000
	maxTotalWordLCSCells = 5_000_000
)

func normalizedWordBag(text string) []string {
	fields := strings.Fields(strings.ReplaceAll(text, "\u00a0", " "))
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		w := strings.ToLower(strings.TrimFunc(f, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}))
		if w != "" {
			out = append(out, w)
		}
	}
	return out
}

// lineKeepsMostWords reports whether newText preserves at least half of the
// words of oldText (and vice versa), preventing a multi-line equal-count hunk
// from shifting kept text into a deleted neighbor's block.
func lineKeepsMostWords(oldText, newText string) bool {
	ow := normalizedWordBag(oldText)
	nw := normalizedWordBag(newText)
	if len(ow) == 0 || len(nw) == 0 {
		return false
	}
	counts := make(map[string]int, len(ow))
	for _, w := range ow {
		counts[w]++
	}
	overlap := 0
	for _, w := range nw {
		if counts[w] > 0 {
			counts[w]--
			overlap++
		}
	}
	maxLen := len(ow)
	if len(nw) > maxLen {
		maxLen = len(nw)
	}
	return overlap > 0 && overlap*2 >= maxLen
}

func isCommonStopWord(w string) bool {
	switch strings.ToLower(strings.TrimFunc(w, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})) {
	case "", "a", "an", "the", "to", "of", "in", "on", "at", "for", "and", "or", "is", "be", "it", "as", "by", "my", "we", "us":
		return true
	}
	return false
}

// isSpacerBlockNode reports whether n is an empty <div>/<p> spacer such as
// <div><br></div>, <p><br></p>, <p>&nbsp;</p>, or <div>&nbsp;</div>.
func isSpacerBlockNode(n *html.Node) bool {
	if n == nil || n.Type != html.ElementNode {
		return false
	}
	tag := strings.ToLower(n.Data)
	if tag != "div" && tag != "p" {
		return false
	}
	sawBR := false
	sawNBSP := false
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		switch child.Type {
		case html.TextNode:
			if strings.ContainsRune(child.Data, '\u00a0') {
				sawNBSP = true
			}
			if strings.TrimSpace(child.Data) != "" {
				return false
			}
		case html.ElementNode:
			if strings.ToLower(child.Data) == "br" && !sawBR {
				sawBR = true
				continue
			}
			return false
		}
	}
	return sawBR || sawNBSP
}

func nextNonWhitespaceSibling(n *html.Node) *html.Node {
	for sib := n.NextSibling; sib != nil; sib = sib.NextSibling {
		if sib.Type == html.TextNode && strings.TrimSpace(sib.Data) == "" {
			continue
		}
		return sib
	}
	return nil
}

func prevNonWhitespaceSibling(n *html.Node) *html.Node {
	for sib := n.PrevSibling; sib != nil; sib = sib.PrevSibling {
		if sib.Type == html.TextNode && strings.TrimSpace(sib.Data) == "" {
			continue
		}
		return sib
	}
	return nil
}

type patchPlan struct {
	lineMap      map[int]int // newLineIdx -> origLineIdx for unchanged and in-place replaced lines
	deletedImgs  int
	deletedLinks int
}

func countTagInNodes(nodes []*html.Node, targetTag string) int {
	total := 0
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		if n == nil {
			return
		}
		if n.Type == html.ElementNode && strings.ToLower(n.Data) == targetTag {
			total++
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	for _, n := range nodes {
		walk(n)
	}
	return total
}

func countTagInLogicalLine(ol logicalLine, tag string) int {
	if ol.blockNode != nil {
		return countTagInNodes([]*html.Node{ol.blockNode}, tag)
	}
	return countTagInNodes(ol.segmentNodes, tag)
}

// patchRichHTMLWithPlainTextEdit applies a plain-text edit (from an older
// mobile APK) onto an existing rich HTML body, then strictly verifies that the
// patched HTML's visible words, block tags/attributes, links, images, and
// blank lines match the edit. Only four safe shapes are accepted:
//
//	(a) in-place edits where a hunk has the same number of old and new lines,
//	(b) pure insertion of whole lines next to an unchanged line,
//	(c) pure deletion of whole lines,
//	(d) blank-line changes where the edited text's blank state differs.
//
// Anything else returns ErrOldClientEditUnsupported (HTTP 409).
func patchRichHTMLWithPlainTextEdit(existingEditorHTML string, newPlainText string) (string, error) {
	if !hasRichEmailFormatting(existingEditorHTML) {
		return emailPlainTextToHTML(newPlainText), nil
	}
	normalizedNew := normalizePlainEditText(newPlainText)
	if normalizedNew == "" {
		return "<div><br></div>", nil
	}
	normCompare := normalizeForNoOpCompare(newPlainText)
	if normCompare == normalizeForNoOpCompare(htmlEmailText(existingEditorHTML)) ||
		normCompare == normalizeForNoOpCompare(htmlEmailTextWithoutListMarkers(existingEditorHTML)) {
		return existingEditorHTML, nil
	}

	root, err := parseHTMLFragment(existingEditorHTML)
	if err != nil {
		return "", ErrOldClientEditUnsupported
	}

	oldLines := collectLogicalLines(root)
	if len(oldLines) == 0 {
		return emailPlainTextToHTML(newPlainText), nil
	}
	newLines := parseNewPlainLines(normalizedNew)
	if len(newLines) == 0 {
		return "<div><br></div>", nil
	}

	// Precompute normalized keys once so LCS does zero regex work in its inner loop.
	oldRawKeys := make([]string, len(oldLines))
	oldListKeys := make([]string, len(oldLines))
	for i, l := range oldLines {
		oldRawKeys[i] = normalizeLineKey(l.text, false)
		if l.isList {
			oldListKeys[i] = stripListPrefix(oldRawKeys[i])
		}
	}
	newRawKeys := make([]string, len(newLines))
	newStrippedKeys := make([]string, len(newLines))
	for i, l := range newLines {
		newRawKeys[i] = normalizeLineKey(l.text, false)
		newStrippedKeys[i] = stripListPrefix(newRawKeys[i])
	}
	linesEqual := func(i, j int) bool {
		if oldRawKeys[i] == newRawKeys[j] {
			return true
		}
		if oldLines[i].isList && (oldRawKeys[i] == newStrippedKeys[j] || oldListKeys[i] == newStrippedKeys[j]) {
			return true
		}
		return false
	}

	// Trim common prefix and suffix before running DP LCS so large emails with
	// localized edits finish in < 1 ms.
	prefix := 0
	for prefix < len(oldLines) && prefix < len(newLines) && linesEqual(prefix, prefix) {
		prefix++
	}
	oldEnd := len(oldLines)
	newEnd := len(newLines)
	for oldEnd > prefix && newEnd > prefix && linesEqual(oldEnd-1, newEnd-1) {
		oldEnd--
		newEnd--
	}
	midOld := oldEnd - prefix
	midNew := newEnd - prefix
	if midOld > maxOldClientLCSLines || midNew > maxOldClientLCSLines || midOld*midNew > maxOldClientLCSCells {
		return "", ErrOldClientEditUnsupported
	}

	var matches []idxMatch
	for i := 0; i < prefix; i++ {
		matches = append(matches, idxMatch{oldIdx: i, newIdx: i})
	}
	if midOld > 0 && midNew > 0 {
		midMatches := computeLCS(midOld, midNew, func(i, j int) bool {
			return linesEqual(prefix+i, prefix+j)
		})
		for _, m := range midMatches {
			matches = append(matches, idxMatch{oldIdx: prefix + m.oldIdx, newIdx: prefix + m.newIdx})
		}
	}
	for i := oldEnd; i < len(oldLines); i++ {
		matches = append(matches, idxMatch{oldIdx: i, newIdx: newEnd + (i - oldEnd)})
	}

	type hunk struct {
		oldStart, oldEnd int
		newStart, newEnd int
	}
	var hunks []hunk
	prevOld, prevNew := 0, 0
	for _, m := range matches {
		if m.oldIdx > prevOld || m.newIdx > prevNew {
			hunks = append(hunks, hunk{prevOld, m.oldIdx, prevNew, m.newIdx})
		}
		prevOld = m.oldIdx + 1
		prevNew = m.newIdx + 1
	}
	if prevOld < len(oldLines) || prevNew < len(newLines) {
		hunks = append(hunks, hunk{prevOld, len(oldLines), prevNew, len(newLines)})
	}

	plan := patchPlan{
		lineMap: make(map[int]int, len(newLines)),
	}
	for _, m := range matches {
		plan.lineMap[m.newIdx] = m.oldIdx
	}

	// (d) Blank-line-only edit (no text hunks).
	if len(hunks) == 0 && len(matches) == len(oldLines) && len(matches) == len(newLines) {
		if applyBlankLineEdits(root, newLines) {
			patched := strings.TrimSpace(renderHTMLChildren(root))
			if err := verifyPatchedEditorHTML(existingEditorHTML, patched, newLines, plan); err == nil {
				return patched, nil
			}
		}
		return "", ErrOldClientEditUnsupported
	}

	// Validate hunk shapes first: accept only (a) equal-count in-place edits
	// (for multi-line hunks, every pair must keep most of its words so kept
	// text can never shift into a deleted neighbor's block), (b) pure
	// insertions, or (c) pure deletions. Refuse mixed-count rewrites.
	for _, h := range hunks {
		oldCount := h.oldEnd - h.oldStart
		newCount := h.newEnd - h.newStart
		if oldCount > 0 && newCount > 0 && oldCount != newCount {
			return "", ErrOldClientEditUnsupported
		}
		if oldCount == newCount && oldCount > 1 {
			for k := 0; k < oldCount; k++ {
				ol := oldLines[h.oldStart+k]
				nl := newLines[h.newStart+k]
				oldTxt := ol.text
				newTxt := nl.text
				if ol.isList {
					oldTxt = stripListPrefix(oldTxt)
					if nl.hasListPfx {
						newTxt = stripListPrefix(newTxt)
					}
				}
				if !lineKeepsMostWords(oldTxt, newTxt) {
					return "", ErrOldClientEditUnsupported
				}
			}
		}
	}

	wordCellBudget := maxTotalWordLCSCells

	// Apply hunks in reverse order so DOM mutations do not invalidate earlier indices.
	for hi := len(hunks) - 1; hi >= 0; hi-- {
		h := hunks[hi]
		oldCount := h.oldEnd - h.oldStart
		newCount := h.newEnd - h.newStart

		switch {
		case oldCount == newCount:
			// (a) In-place edits paired in order, preserving each line's own block/row/cell/list/link.
			for k := 0; k < oldCount; k++ {
				ol := oldLines[h.oldStart+k]
				nl := newLines[h.newStart+k]
				if ol.isHR || ol.inPre {
					return "", ErrOldClientEditUnsupported
				}
				plan.lineMap[h.newStart+k] = h.oldStart + k
				target := nl.text
				if ol.isList && nl.hasListPfx {
					target = stripListPrefix(target)
				}
				if ol.isTR {
					if err := patchTableRowInPlace(ol, target, &wordCellBudget); err != nil {
						return "", err
					}
				} else if err := patchLineTextNodes(ol.textNodes, target, &wordCellBudget); err != nil {
					return "", err
				}
			}
		case newCount == 0:
			// (c) Pure deletion of whole lines.
			for oi := h.oldEnd - 1; oi >= h.oldStart; oi-- {
				plan.deletedImgs += countTagInLogicalLine(oldLines[oi], "img")
				plan.deletedLinks += countTagInLogicalLine(oldLines[oi], "a")
				deleteLogicalLine(oldLines[oi])
			}
		case oldCount == 0:
			// (b) Pure insertion of whole lines next to an unchanged line.
			if err := insertNewLinesAtHunk(root, oldLines, h.oldStart, h.oldEnd, newLines[h.newStart:h.newEnd]); err != nil {
				return "", err
			}
		}
	}

	// (d) Apply blank-line changes only where a gap's blank state differs.
	applyBlankLineEdits(root, newLines)

	patched := strings.TrimSpace(renderHTMLChildren(root))
	if err := verifyPatchedEditorHTML(existingEditorHTML, patched, newLines, plan); err != nil {
		return "", err
	}
	return patched, nil
}

// applyBlankLineEdits adjusts blank-line spacers between adjacent <div> blocks
// or <br>-delimited segments so each gap's spacer count matches newLines[i].blankCount.
// Untouched multi-blank gaps (e.g. two <div><br></div> spacers) have blankCount == 2
// in both the original and edited text, so they are preserved unchanged.
func applyBlankLineEdits(root *html.Node, newLines []newLineInfo) bool {
	curLines := collectLogicalLines(root)
	if len(curLines) != len(newLines) {
		return false
	}
	changed := false
	for i := 1; i < len(curLines); i++ {
		prev := curLines[i-1]
		cur := curLines[i]
		wantCount := newLines[i].blankCount

		// Case 1: sibling <div> blocks.
		if prev.blockNode != nil && cur.blockNode != nil && prev.blockNode.Parent == cur.blockNode.Parent &&
			strings.ToLower(prev.blockNode.Data) == "div" && strings.ToLower(cur.blockNode.Data) == "div" {
			var spacers []*html.Node
			for sib := prev.blockNode.NextSibling; sib != nil && sib != cur.blockNode; sib = sib.NextSibling {
				if isSpacerBlockNode(sib) {
					spacers = append(spacers, sib)
				}
			}
			for len(spacers) < wantCount {
				spacer := &html.Node{Type: html.ElementNode, DataAtom: atom.Div, Data: "div"}
				spacer.AppendChild(&html.Node{Type: html.ElementNode, DataAtom: atom.Br, Data: "br"})
				cur.blockNode.Parent.InsertBefore(spacer, cur.blockNode)
				spacers = append(spacers, spacer)
				changed = true
			}
			for len(spacers) > wantCount {
				sp := spacers[len(spacers)-1]
				sp.Parent.RemoveChild(sp)
				spacers = spacers[:len(spacers)-1]
				changed = true
			}
			continue
		}

		// Case 2: <br>-delimited segments in the same parent.
		if prev.blockNode == nil && cur.blockNode == nil && prev.parentNode != nil && prev.parentNode == cur.parentNode &&
			prev.trailingBR != nil && len(cur.segmentNodes) > 0 {
			firstCur := cur.segmentNodes[0]
			var brs []*html.Node
			for sib := prev.trailingBR; sib != nil && sib != firstCur; sib = sib.NextSibling {
				if sib.Type == html.ElementNode && strings.ToLower(sib.Data) == "br" {
					brs = append(brs, sib)
				}
			}
			wantBRs := wantCount + 1
			for len(brs) < wantBRs && len(brs) > 0 {
				br := &html.Node{Type: html.ElementNode, DataAtom: atom.Br, Data: "br"}
				prev.parentNode.InsertBefore(br, brs[len(brs)-1].NextSibling)
				brs = append(brs, br)
				changed = true
			}
			for len(brs) > wantBRs {
				extra := brs[len(brs)-1]
				extra.Parent.RemoveChild(extra)
				brs = brs[:len(brs)-1]
				changed = true
			}
		}
	}
	return changed
}

func patchTableRowInPlace(ol logicalLine, newLine string, wordCellBudget *int) error {
	parts := strings.Split(newLine, "\t")
	if len(parts) != len(ol.cells) {
		return ErrOldClientEditUnsupported
	}
	for i, cell := range ol.cells {
		cellTextNodes := collectAllTextNodes(cell)
		want := strings.TrimSpace(parts[i])
		if err := patchLineTextNodes(cellTextNodes, want, wordCellBudget); err != nil {
			return err
		}
	}
	return nil
}

type wordToken struct {
	word     string
	node     *html.Node
	startOff int // byte offset in node.Data
	endOff   int // byte offset in node.Data
}

func isInlinePunct(b byte) bool {
	return b == '.' || b == ',' || b == ';' || b == ':' || b == '!' || b == '?' || b == ')' || b == ']' || b == '"' || b == '\''
}

func isOpeningPunct(b byte) bool {
	return b == '(' || b == '[' || b == '"' || b == '\''
}

func isTokenNoSpaceBefore(w string) bool {
	if w == "" {
		return false
	}
	if w == "'s" || w == "\u2019s" || w == "/" {
		return true
	}
	if len(w) == 1 && isInlinePunct(w[0]) {
		return true
	}
	return w == "\u201d" || w == "\u2019"
}

func isTokenNoSpaceAfter(w string) bool {
	if w == "" {
		return false
	}
	if w == "/" {
		return true
	}
	if len(w) == 1 && isOpeningPunct(w[0]) {
		return true
	}
	return w == "\u201c" || w == "\u2018"
}

func endsWithSpaceOrOpen(s string) bool {
	if s == "" {
		return false
	}
	last := s[len(s)-1]
	if last == ' ' || last == '\t' || last == '\n' || last == '\r' || isOpeningPunct(last) || last == '/' {
		return true
	}
	return strings.HasSuffix(s, "\u00a0") || strings.HasSuffix(s, "\u201c") || strings.HasSuffix(s, "\u2018")
}

func startsWithSpaceOrClose(s string) bool {
	if s == "" {
		return false
	}
	first := s[0]
	if first == ' ' || first == '\t' || first == '\n' || first == '\r' || isInlinePunct(first) || first == '/' {
		return true
	}
	return strings.HasPrefix(s, "\u00a0") || strings.HasPrefix(s, "\u201d") || strings.HasPrefix(s, "\u2019") || strings.HasPrefix(s, "'s") || strings.HasPrefix(s, "\u2019s")
}

type pieceSpan struct {
	text  string
	start int // byte offset within raw word
	end   int
}

// splitWordPieces splits a whitespace-delimited word into punctuation and word
// pieces (opening/closing parens & quotes, trailing sentence punct, 's, and
// non-URL slashes) so HTML text nodes adjacent to <a>/<b> tokenize identically
// to plain text.
func splitWordPieces(w string) []pieceSpan {
	if w == "" {
		return nil
	}
	var out []pieceSpan
	lo, hi := 0, len(w)
	// Strip leading opening punctuation.
	for lo < hi {
		if isOpeningPunct(w[lo]) {
			out = append(out, pieceSpan{w[lo : lo+1], lo, lo + 1})
			lo++
			continue
		}
		if strings.HasPrefix(w[lo:hi], "\u201c") || strings.HasPrefix(w[lo:hi], "\u2018") {
			sz := len("\u201c")
			out = append(out, pieceSpan{w[lo : lo+sz], lo, lo + sz})
			lo += sz
			continue
		}
		break
	}
	// Strip trailing punctuation (keep URLs intact except trailing sentence/closing punct).
	var tail []pieceSpan
	for hi > lo {
		if isInlinePunct(w[hi-1]) {
			tail = append(tail, pieceSpan{w[hi-1 : hi], hi - 1, hi})
			hi--
			continue
		}
		if strings.HasSuffix(w[lo:hi], "\u201d") || strings.HasSuffix(w[lo:hi], "\u2019") {
			sz := len("\u201d")
			tail = append(tail, pieceSpan{w[hi-sz : hi], hi - sz, hi})
			hi -= sz
			continue
		}
		break
	}
	// Strip trailing possessive 's / ’s if preceded by word chars.
	if hi-lo > 2 && strings.HasSuffix(w[lo:hi], "'s") {
		tail = append(tail, pieceSpan{"'s", hi - 2, hi})
		hi -= 2
	} else if hi-lo > len("\u2019s") && strings.HasSuffix(w[lo:hi], "\u2019s") {
		sz := len("\u2019s")
		tail = append(tail, pieceSpan{"\u2019s", hi - sz, hi})
		hi -= sz
	}
	core := w[lo:hi]
	if core != "" {
		lowerCore := strings.ToLower(core)
		isURL := strings.HasPrefix(lowerCore, "http://") || strings.HasPrefix(lowerCore, "https://") || strings.HasPrefix(lowerCore, "mailto:")
		if !isURL && strings.Contains(core, "/") {
			pos := lo
			for pos < hi {
				idx := strings.IndexByte(w[pos:hi], '/')
				if idx < 0 {
					out = append(out, pieceSpan{w[pos:hi], pos, hi})
					break
				}
				if idx > 0 {
					out = append(out, pieceSpan{w[pos : pos+idx], pos, pos + idx})
				}
				out = append(out, pieceSpan{"/", pos + idx, pos + idx + 1})
				pos += idx + 1
			}
		} else {
			out = append(out, pieceSpan{core, lo, hi})
		}
	}
	for i := len(tail) - 1; i >= 0; i-- {
		out = append(out, tail[i])
	}
	return out
}

func tokenizeTextNodes(nodes []*html.Node) []wordToken {
	var tokens []wordToken
	for _, tn := range nodes {
		data := tn.Data
		i := 0
		for i < len(data) {
			for i < len(data) {
				r := rune(data[i])
				if r < 128 && (r == ' ' || r == '\t' || r == '\n' || r == '\r') {
					i++
					continue
				}
				if strings.HasPrefix(data[i:], "\u00a0") {
					i += len("\u00a0")
					continue
				}
				break
			}
			if i >= len(data) {
				break
			}
			start := i
			for i < len(data) {
				r := rune(data[i])
				if r < 128 && (r == ' ' || r == '\t' || r == '\n' || r == '\r') {
					break
				}
				if strings.HasPrefix(data[i:], "\u00a0") {
					break
				}
				i++
			}
			for _, ps := range splitWordPieces(data[start:i]) {
				tokens = append(tokens, wordToken{
					word:     ps.text,
					node:     tn,
					startOff: start + ps.start,
					endOff:   start + ps.end,
				})
			}
		}
	}
	return tokens
}

func tokenizePlainWords(line string) []string {
	rawWords := strings.Fields(strings.ReplaceAll(line, "\u00a0", " "))
	out := make([]string, 0, len(rawWords))
	for _, w := range rawWords {
		for _, ps := range splitWordPieces(w) {
			out = append(out, ps.text)
		}
	}
	return out
}

func joinTokenWords(words []string) string {
	var sb strings.Builder
	for i, w := range words {
		if i > 0 && !isTokenNoSpaceBefore(w) && !isTokenNoSpaceAfter(words[i-1]) {
			sb.WriteByte(' ')
		}
		sb.WriteString(w)
	}
	return sb.String()
}

func inlineAncestorsOf(tn *html.Node) []*html.Node {
	var out []*html.Node
	for cur := tn.Parent; cur != nil; cur = cur.Parent {
		if cur.Type == html.ElementNode {
			tag := strings.ToLower(cur.Data)
			if isBlockContainerTag(tag) {
				break
			}
			if emailInlineFormatTags[tag] {
				out = append(out, cur)
			}
		}
	}
	return out
}

// patchLineTextNodes applies word-level edits across a line's text nodes so
// multiple edits in the same paragraph and HTML source newlines never strip
// inline formatting tags (<strong>, <a>, <em>, etc.) between edits.
func patchLineTextNodes(textNodes []*html.Node, newLine string, wordCellBudget *int) error {
	if len(textNodes) == 0 {
		return ErrOldClientEditUnsupported
	}
	oldTokens := tokenizeTextNodes(textNodes)
	newWords := tokenizePlainWords(newLine)
	if len(oldTokens) == 0 || len(newWords) == 0 || len(oldTokens) > maxWordLCSTokens || len(newWords) > maxWordLCSTokens {
		return ErrOldClientEditUnsupported
	}
	cells := len(oldTokens) * len(newWords)
	if wordCellBudget != nil {
		if *wordCellBudget < cells {
			return ErrOldClientEditUnsupported
		}
		*wordCellBudget -= cells
	}

	matches := computeLCS(len(oldTokens), len(newWords), func(i, j int) bool {
		return oldTokens[i].word == newWords[j]
	})

	type wordHunk struct {
		oldStart, oldEnd int
		newStart, newEnd int
	}
	var hunks []wordHunk
	prevOld, prevNew := 0, 0
	for _, m := range matches {
		if m.oldIdx > prevOld || m.newIdx > prevNew {
			hunks = append(hunks, wordHunk{prevOld, m.oldIdx, prevNew, m.newIdx})
		}
		prevOld = m.oldIdx + 1
		prevNew = m.newIdx + 1
	}
	if prevOld < len(oldTokens) || prevNew < len(newWords) {
		hunks = append(hunks, wordHunk{prevOld, len(oldTokens), prevNew, len(newWords)})
	}
	if len(hunks) == 0 {
		return nil
	}

	// Guard against spreading a partial inline link/bold/italic run onto
	// unrelated new words when no substantive word of that run survives.
	matchedOld := make([]bool, len(oldTokens))
	for _, m := range matches {
		matchedOld[m.oldIdx] = true
	}
	type inlineRunStats struct {
		tag         string
		total       int
		kept        int
		keptNonStop int
		touched     bool
		crossBound  bool
	}
	runStats := map[*html.Node]*inlineRunStats{}
	tokenInlines := make([][]*html.Node, len(oldTokens))
	for i, tok := range oldTokens {
		ancs := inlineAncestorsOf(tok.node)
		tokenInlines[i] = ancs
		for _, el := range ancs {
			st := runStats[el]
			if st == nil {
				st = &inlineRunStats{tag: strings.ToLower(el.Data)}
				runStats[el] = st
			}
			st.total++
			if matchedOld[i] {
				st.kept++
				if !isCommonStopWord(tok.word) {
					st.keptNonStop++
				}
			}
		}
	}
	for _, h := range hunks {
		if h.oldStart == h.oldEnd {
			continue
		}
		inHunk := map[*html.Node]int{}
		for i := h.oldStart; i < h.oldEnd; i++ {
			for _, el := range tokenInlines[i] {
				inHunk[el]++
			}
		}
		hunkLen := h.oldEnd - h.oldStart
		for el, cnt := range inHunk {
			st := runStats[el]
			st.touched = true
			if cnt < hunkLen {
				st.crossBound = true
			}
		}
	}
	for _, st := range runStats {
		if !st.touched || st.total >= len(oldTokens) {
			// Untouched run, or the inline element spans the entire line (e.g.
			// <p><a>Donate</a></p> or <li><b>Milk</b></li>): in-place retype is safe.
			continue
		}
		if st.tag == "a" && (st.keptNonStop == 0 || st.crossBound) {
			return ErrOldClientEditUnsupported
		}
		if st.tag != "a" && st.kept == 0 && st.crossBound {
			return ErrOldClientEditUnsupported
		}
	}

	// Apply word hunks in reverse order so byte offsets inside earlier text nodes stay valid.
	for hi := len(hunks) - 1; hi >= 0; hi-- {
		h := hunks[hi]
		replacement := joinTokenWords(newWords[h.newStart:h.newEnd])

		if h.oldStart == h.oldEnd {
			// Pure insertion of words.
			sep := " "
			if startsWithSpaceOrClose(replacement) {
				sep = ""
			}
			if h.oldStart > 0 {
				prevTok := oldTokens[h.oldStart-1]
				if prevTok.endOff == len(prevTok.node.Data) &&
					prevTok.node.NextSibling == nil && prevTok.node.Parent != nil &&
					emailInlineFormatTags[strings.ToLower(prevTok.node.Parent.Data)] &&
					prevTok.node.Parent.Parent != nil {
					inlineEl := prevTok.node.Parent
					inlineEl.Parent.InsertBefore(&html.Node{Type: html.TextNode, Data: sep + replacement}, inlineEl.NextSibling)
				} else {
					prevTok.node.Data = prevTok.node.Data[:prevTok.endOff] + sep + replacement + prevTok.node.Data[prevTok.endOff:]
				}
			} else {
				firstTok := oldTokens[0]
				if firstTok.startOff == 0 && firstTok.node.PrevSibling == nil &&
					firstTok.node.Parent != nil &&
					emailInlineFormatTags[strings.ToLower(firstTok.node.Parent.Data)] &&
					firstTok.node.Parent.Parent != nil {
					inlineEl := firstTok.node.Parent
					inlineEl.Parent.InsertBefore(&html.Node{Type: html.TextNode, Data: replacement + " "}, inlineEl)
				} else {
					firstTok.node.Data = firstTok.node.Data[:firstTok.startOff] + replacement + " " + firstTok.node.Data[firstTok.startOff:]
				}
			}
			continue
		}

		// Replace or delete oldTokens[h.oldStart : h.oldEnd].
		first := oldTokens[h.oldStart]
		last := oldTokens[h.oldEnd-1]
		if first.node == last.node {
			before := first.node.Data[:first.startOff]
			after := first.node.Data[last.endOff:]
			rep := replacement
			if rep == "" {
				if strings.HasSuffix(before, " ") && strings.HasPrefix(after, " ") {
					after = after[1:]
				}
			} else {
				if before != "" && !endsWithSpaceOrOpen(before) && !startsWithSpaceOrClose(rep) {
					rep = " " + rep
				}
				if after != "" && !startsWithSpaceOrClose(after) && !endsWithSpaceOrOpen(rep) {
					rep = rep + " "
				}
			}
			first.node.Data = before + rep + after
		} else {
			before := first.node.Data[:first.startOff]
			rep := replacement
			if rep != "" && before != "" && !endsWithSpaceOrOpen(before) && !startsWithSpaceOrClose(rep) {
				rep = " " + rep
			}
			first.node.Data = before + rep
			seen := map[*html.Node]bool{first.node: true}
			for ti := h.oldStart + 1; ti < h.oldEnd-1; ti++ {
				tn := oldTokens[ti].node
				if !seen[tn] {
					tn.Data = ""
					seen[tn] = true
				}
			}
			if !seen[last.node] {
				after := last.node.Data[last.endOff:]
				if rep != "" && after != "" && !startsWithSpaceOrClose(after) && !endsWithSpaceOrOpen(rep) {
					after = " " + after
				}
				last.node.Data = after
			}
		}
	}

	return nil
}

func deleteLogicalLine(ol logicalLine) {
	if ol.blockNode != nil && ol.blockNode.Parent != nil {
		parent := ol.blockNode.Parent
		// Remove one adjacent spacer block (e.g. <div><br></div> or <p>&nbsp;</p>)
		// so deleting a paragraph does not leave a double blank line or stray edge spacer.
		if next := nextNonWhitespaceSibling(ol.blockNode); next != nil && isSpacerBlockNode(next) {
			parent.RemoveChild(next)
		} else if prev := prevNonWhitespaceSibling(ol.blockNode); prev != nil && isSpacerBlockNode(prev) {
			parent.RemoveChild(prev)
		}
		parent.RemoveChild(ol.blockNode)
		if (strings.ToLower(parent.Data) == "ol" || strings.ToLower(parent.Data) == "ul") && !hasElementChild(parent, "li") && parent.Parent != nil {
			parent.Parent.RemoveChild(parent)
		}
		return
	}
	// <br>-delimited or bare text segment inside a container.
	for _, n := range ol.segmentNodes {
		if n.Parent != nil {
			n.Parent.RemoveChild(n)
		}
	}
	if ol.trailingBR != nil && ol.trailingBR.Parent != nil {
		if next := nextNonWhitespaceSibling(ol.trailingBR); next != nil && next.Type == html.ElementNode && strings.ToLower(next.Data) == "br" {
			next.Parent.RemoveChild(next)
		}
		ol.trailingBR.Parent.RemoveChild(ol.trailingBR)
	} else if ol.leadingBR != nil && ol.leadingBR.Parent != nil {
		if prev := prevNonWhitespaceSibling(ol.leadingBR); prev != nil && prev.Type == html.ElementNode && strings.ToLower(prev.Data) == "br" {
			prev.Parent.RemoveChild(prev)
		}
		ol.leadingBR.Parent.RemoveChild(ol.leadingBR)
	}
}

func insertNewLinesAtHunk(root *html.Node, oldLines []logicalLine, afterOldIdx int, beforeOldIdx int, toInsert []newLineInfo) error {
	// Case 1: Inserting inside a <br>-delimited block (including bare root <br> body).
	if afterOldIdx > 0 && beforeOldIdx < len(oldLines) {
		prev := oldLines[afterOldIdx-1]
		next := oldLines[beforeOldIdx]
		if prev.parentNode != nil && prev.parentNode == next.parentNode && prev.blockNode == nil && next.blockNode == nil && prev.trailingBR != nil && prev.trailingBR.Parent == prev.parentNode {
			ref := prev.trailingBR
			for _, nl := range toInsert {
				if nl.blankAbove {
					// Ensure a blank line (double <br>) before the inserted text.
					extraBR := &html.Node{Type: html.ElementNode, DataAtom: atom.Br, Data: "br"}
					prev.parentNode.InsertBefore(extraBR, ref.NextSibling)
					ref = extraBR
				}
				tn := &html.Node{Type: html.TextNode, Data: nl.text}
				br := &html.Node{Type: html.ElementNode, DataAtom: atom.Br, Data: "br"}
				prev.parentNode.InsertBefore(tn, ref.NextSibling)
				prev.parentNode.InsertBefore(br, tn.NextSibling)
				ref = br
			}
			return nil
		}
	}
	// Appending a <br> line at the end of a <br>-delimited block (or bare root <br> body).
	if afterOldIdx > 0 && len(toInsert) > 0 && !toInsert[0].hasListPfx {
		prev := oldLines[afterOldIdx-1]
		if prev.blockNode == nil && prev.parentNode != nil && len(prev.segmentNodes) > 0 &&
			(prev.parentNode != root || prev.leadingBR != nil || prev.trailingBR != nil) {
			lastSeg := prev.segmentNodes[len(prev.segmentNodes)-1]
			if lastSeg.Parent == prev.parentNode && (!toInsert[0].blankAbove || prev.parentNode == root) {
				ref := lastSeg
				for _, nl := range toInsert {
					if nl.blankAbove && prev.parentNode != root {
						break
					}
					br := &html.Node{Type: html.ElementNode, DataAtom: atom.Br, Data: "br"}
					prev.parentNode.InsertBefore(br, ref.NextSibling)
					ref = br
					if nl.blankAbove {
						br2 := &html.Node{Type: html.ElementNode, DataAtom: atom.Br, Data: "br"}
						prev.parentNode.InsertBefore(br2, ref.NextSibling)
						ref = br2
					}
					tn := &html.Node{Type: html.TextNode, Data: nl.text}
					prev.parentNode.InsertBefore(tn, ref.NextSibling)
					ref = tn
				}
				return nil
			}
		}
	}

	// Determine block anchor for sibling insertion.
	var anchor *html.Node
	var insertParent *html.Node
	insertAfter := false
	// If inserting list items right before an existing list item (e.g. adding
	// "- Zero" before "- One"), anchor before that <li> so it joins the list.
	if len(toInsert) > 0 && toInsert[0].hasListPfx && beforeOldIdx < len(oldLines) {
		nextAnchor := blockAnchorForLine(oldLines[beforeOldIdx])
		if nextAnchor != nil && strings.ToLower(nextAnchor.Data) == "li" {
			anchor = nextAnchor
			insertAfter = false
		}
	}
	if anchor == nil {
		if afterOldIdx > 0 {
			prev := oldLines[afterOldIdx-1]
			anchor = blockAnchorForLine(prev)
			insertAfter = true
		} else if beforeOldIdx < len(oldLines) {
			next := oldLines[beforeOldIdx]
			anchor = blockAnchorForLine(next)
			insertAfter = false
		}
	}

	// If anchor is a <tr> inside a <table>/<tbody>, step out to the <table>
	// so new paragraphs never land inside <tbody>.
	if anchor != nil && strings.ToLower(anchor.Data) == "tr" {
		for cur := anchor.Parent; cur != nil; cur = cur.Parent {
			if cur.Type == html.ElementNode && strings.ToLower(cur.Data) == "table" {
				anchor = cur
				break
			}
		}
	}

	// If anchor is an <li> inside <ul>/<ol>, only insert as <li> when the new
	// line actually starts with a list marker AND has no blank line separating
	// it from the list. Otherwise step out of the <ul>/<ol> and insert a <p>
	// after the list (fixes sign-off/paragraph after list becoming list items).
	wantListItem := false
	if anchor != nil && strings.ToLower(anchor.Data) == "li" {
		if len(toInsert) > 0 && toInsert[0].hasListPfx && (!insertAfter || !toInsert[0].blankAbove) {
			wantListItem = true
		} else {
			if anchor.Parent != nil && (strings.ToLower(anchor.Parent.Data) == "ul" || strings.ToLower(anchor.Parent.Data) == "ol") {
				anchor = anchor.Parent
			}
		}
	}

	if anchor != nil && anchor.Parent != nil {
		insertParent = anchor.Parent
	} else {
		insertParent = root
		anchor = nil
	}

	tag := "p"
	if wantListItem {
		tag = "li"
	} else if anchor != nil && strings.ToLower(anchor.Data) == "div" {
		tag = "div"
	}

	// Copy plain neighbor style/class attributes onto inserted <p>/<div>/<li> blocks.
	var copyAttrs []html.Attribute
	if anchor != nil && strings.ToLower(anchor.Data) == tag {
		for _, a := range anchor.Attr {
			k := strings.ToLower(a.Key)
			if k == "style" || k == "class" || k == "dir" {
				copyAttrs = append(copyAttrs, a)
			}
		}
	}

	var openP *html.Node
	for idx, nl := range toInsert {
		lineText := nl.text
		if tag == "li" {
			lineText = stripListPrefix(lineText)
		}
		// Consecutive non-blank lines inserted as <p> share one <p> with <br>
		// (e.g. "Best,\nZach" -> <p>Best,<br/>Zach</p>).
		if tag == "p" && idx > 0 && !nl.blankAbove && openP != nil {
			openP.AppendChild(&html.Node{Type: html.ElementNode, DataAtom: atom.Br, Data: "br"})
			openP.AppendChild(&html.Node{Type: html.TextNode, Data: lineText})
			continue
		}
		el := &html.Node{
			Type:     html.ElementNode,
			DataAtom: atom.Lookup([]byte(tag)),
			Data:     tag,
			Attr:     append([]html.Attribute(nil), copyAttrs...),
		}
		el.AppendChild(&html.Node{Type: html.TextNode, Data: lineText})
		if anchor != nil && insertParent != nil {
			if insertAfter {
				insertParent.InsertBefore(el, anchor.NextSibling)
				anchor = el
			} else {
				insertParent.InsertBefore(el, anchor)
			}
		} else if insertParent != nil {
			insertParent.AppendChild(el)
		}
		if tag == "p" {
			openP = el
		}
	}
	return nil
}

func blockAnchorForLine(ol logicalLine) *html.Node {
	if ol.blockNode != nil {
		// If this <p> is inside an <li>, anchor on the <li>.
		if ol.blockNode.Parent != nil && strings.ToLower(ol.blockNode.Parent.Data) == "li" {
			return ol.blockNode.Parent
		}
		return ol.blockNode
	}
	if ol.parentNode != nil && ol.parentNode.Parent != nil {
		return ol.parentNode
	}
	return nil
}

func hasElementChild(n *html.Node, tag string) bool {
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == html.ElementNode && strings.ToLower(child.Data) == tag {
			return true
		}
	}
	return false
}

type visibleLine struct {
	text     string
	isList   bool
	blockSig string
}

func elementBlockSignature(n *html.Node) string {
	var parts []string
	for cur := n; cur != nil && cur.Parent != nil; cur = cur.Parent {
		if cur.Type != html.ElementNode {
			continue
		}
		tag := strings.ToLower(cur.Data)
		if !isBlockContainerTag(tag) {
			continue
		}
		var attrs []string
		for _, a := range cur.Attr {
			k := strings.ToLower(strings.TrimSpace(a.Key))
			v := strings.TrimSpace(a.Val)
			if v != "" {
				attrs = append(attrs, k+"="+v)
			}
		}
		sort.Strings(attrs)
		if len(attrs) > 0 {
			parts = append(parts, tag+"["+strings.Join(attrs, ";")+"]")
		} else {
			parts = append(parts, tag)
		}
	}
	return strings.Join(parts, "<")
}

// visibleLinesOfHTML independently extracts visible lines along with their
// list membership and enclosing block/style signature for post-patch verification.
func visibleLinesOfHTML(h string) []visibleLine {
	root, err := parseHTMLFragment(h)
	if err != nil {
		return nil
	}
	var out []visibleLine
	var curBuf strings.Builder
	curList := false
	curSig := ""

	flush := func() {
		l := strings.TrimSpace(strings.Join(strings.Fields(strings.ReplaceAll(curBuf.String(), "\u00a0", " ")), " "))
		if l != "" {
			out = append(out, visibleLine{text: l, isList: curList, blockSig: curSig})
		}
		curBuf.Reset()
		curList = false
		curSig = ""
	}

	var walk func(n *html.Node, inCell bool, inLI bool)
	walk = func(n *html.Node, inCell bool, inLI bool) {
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.TextNode {
				if strings.TrimSpace(strings.ReplaceAll(c.Data, "\u00a0", " ")) != "" && curSig == "" {
					curSig = elementBlockSignature(n)
					curList = inLI
				}
				curBuf.WriteString(c.Data)
				continue
			}
			if c.Type == html.ElementNode {
				tag := strings.ToLower(c.Data)
				switch tag {
				case "style", "script", "noscript", "template":
					continue
				case "br":
					if inCell {
						curBuf.WriteByte(' ')
					} else {
						flush()
					}
					continue
				case "hr":
					flush()
					out = append(out, visibleLine{text: "---", blockSig: elementBlockSignature(c)})
					continue
				case "td", "th":
					if curSig == "" {
						curSig = elementBlockSignature(n)
					}
					walk(c, true, false)
					curBuf.WriteByte('\t')
					continue
				}
				blk := isBlockContainerTag(tag)
				if blk {
					if inCell {
						curBuf.WriteByte(' ')
					} else {
						flush()
					}
				}
				walk(c, inCell, inLI || tag == "li")
				if blk {
					if inCell {
						curBuf.WriteByte(' ')
					} else {
						flush()
					}
				}
			}
		}
	}
	walk(root, false, false)
	flush()
	return out
}

func htmlStructureSummary(h string) (runs map[string]int, emptyTags int, imgCount int, linkCount int) {
	root, err := parseHTMLFragment(h)
	if err != nil {
		return nil, 0, 0, 0
	}
	runs = map[string]int{}
	var hasImg func(n *html.Node) bool
	hasImg = func(n *html.Node) bool {
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode && (strings.ToLower(c.Data) == "img" || hasImg(c)) {
				return true
			}
		}
		return false
	}
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode {
				tag := strings.ToLower(c.Data)
				if tag == "img" {
					imgCount++
				}
				if tag == "a" {
					linkCount++
				}
				if emailInlineFormatTags[tag] {
					t := strings.Join(strings.Fields(strings.ReplaceAll(collectDirectTextNodesOnly(c), "\u00a0", " ")), " ")
					if t == "" && !hasImg(c) {
						emptyTags++
					} else if t != "" {
						runs[tag+"|"+t]++
					}
				}
				walk(c)
			}
		}
	}
	walk(root)
	return runs, emptyTags, imgCount, linkCount
}

func htmlEdgeSpacerCounts(h string) (leading, trailing, dupPSpacers int) {
	root, err := parseHTMLFragment(h)
	if err != nil {
		return 0, 0, 0
	}
	isEdgeSpacer := func(n *html.Node) bool {
		return isSpacerBlockNode(n) || (n.Type == html.ElementNode && strings.ToLower(n.Data) == "br")
	}
	var nodes []*html.Node
	for c := root.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.TextNode && strings.TrimSpace(c.Data) == "" {
			continue
		}
		nodes = append(nodes, c)
	}
	for _, n := range nodes {
		if isEdgeSpacer(n) {
			leading++
		} else {
			break
		}
	}
	for i := len(nodes) - 1; i >= 0; i-- {
		if isEdgeSpacer(nodes[i]) {
			trailing++
		} else {
			break
		}
	}
	var countDupP func(parent *html.Node)
	countDupP = func(parent *html.Node) {
		prevPSpacer := false
		for c := parent.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.TextNode && strings.TrimSpace(c.Data) == "" {
				continue
			}
			isPSp := c.Type == html.ElementNode && strings.ToLower(c.Data) == "p" && isSpacerBlockNode(c)
			if isPSp && prevPSpacer {
				dupPSpacers++
			}
			prevPSpacer = isPSp
			if c.Type == html.ElementNode {
				countDupP(c)
			}
		}
	}
	countDupP(root)
	return leading, trailing, dupPSpacers
}

func verifyPatchedEditorHTML(origHTML string, patchedHTML string, newLines []newLineInfo, plan patchPlan) error {
	gotLines := visibleLinesOfHTML(patchedHTML)
	wantRaw := make([]string, 0, len(newLines))
	for _, nl := range newLines {
		l := strings.TrimSpace(strings.Join(strings.Fields(strings.ReplaceAll(nl.text, "\u00a0", " ")), " "))
		if l != "" {
			wantRaw = append(wantRaw, l)
		}
	}
	if len(wantRaw) != len(gotLines) {
		return ErrOldClientEditUnsupported
	}
	wantFlatParts := make([]string, len(wantRaw))
	for i := range wantRaw {
		want := wantRaw[i]
		if gotLines[i].isList {
			want = stripListPrefix(want)
		}
		if want != gotLines[i].text {
			return ErrOldClientEditUnsupported
		}
		wantFlatParts[i] = want
	}

	// Verify exact blank-line counts match the edited plain text, and no stray
	// leading/trailing spacers or duplicate <p>&nbsp;</p> spacers were introduced.
	// Only permit a missing blank line directly after an <li> when the incoming
	// plain text came from the pre-deploy markerless format (no list prefix).
	gotPlain := parseNewPlainLines(normalizePlainEditText(htmlEmailText(patchedHTML)))
	if len(gotPlain) != len(newLines) {
		return ErrOldClientEditUnsupported
	}
	for i := 1; i < len(newLines); i++ {
		if newLines[i].blankCount != gotPlain[i].blankCount {
			if newLines[i].blankCount == 0 && gotPlain[i].blankCount == 1 && gotLines[i-1].isList && !newLines[i-1].hasListPfx {
				continue
			}
			return ErrOldClientEditUnsupported
		}
	}
	origLead, origTrail, origDupP := htmlEdgeSpacerCounts(origHTML)
	gotLead, gotTrail, gotDupP := htmlEdgeSpacerCounts(patchedHTML)
	if gotLead > origLead || gotTrail > origTrail || gotDupP > origDupP {
		return ErrOldClientEditUnsupported
	}

	// Verify EVERY unchanged and in-place-edited line kept its exact block tag,
	// attributes, and ancestor path (including table row/cell and list membership),
	// and that no kept line or kept >=3-word run ended up in a different block.
	origLines := visibleLinesOfHTML(origHTML)
	origLineSigs := make(map[string]map[string]bool, len(origLines))
	origShingleSigs := make(map[string]map[string]bool)
	for _, ol := range origLines {
		if origLineSigs[ol.text] == nil {
			origLineSigs[ol.text] = map[string]bool{}
		}
		origLineSigs[ol.text][ol.blockSig] = true
		words := strings.Fields(ol.text)
		for i := 0; i+3 <= len(words); i++ {
			shingle := words[i] + " " + words[i+1] + " " + words[i+2]
			if origShingleSigs[shingle] == nil {
				origShingleSigs[shingle] = map[string]bool{}
			}
			origShingleSigs[shingle][ol.blockSig] = true
		}
	}
	for newIdx, origIdx := range plan.lineMap {
		if origIdx >= 0 && origIdx < len(origLines) && newIdx >= 0 && newIdx < len(gotLines) {
			if origLines[origIdx].blockSig != gotLines[newIdx].blockSig {
				return ErrOldClientEditUnsupported
			}
		}
	}
	for _, gl := range gotLines {
		if sigs, ok := origLineSigs[gl.text]; ok && !sigs[gl.blockSig] {
			return ErrOldClientEditUnsupported
		}
		words := strings.Fields(gl.text)
		for i := 0; i+3 <= len(words); i++ {
			shingle := words[i] + " " + words[i+1] + " " + words[i+2]
			if sigs, ok := origShingleSigs[shingle]; ok && !sigs[gl.blockSig] {
				return ErrOldClientEditUnsupported
			}
		}
	}

	// Verify no images or links were lost outside explicitly deleted lines, no
	// inline formatting tags were emptied, and no untouched inline runs were lost.
	beforeRuns, beforeEmpty, beforeImgs, beforeLinks := htmlStructureSummary(origHTML)
	afterRuns, afterEmpty, afterImgs, afterLinks := htmlStructureSummary(patchedHTML)
	if afterEmpty > beforeEmpty {
		return ErrOldClientEditUnsupported
	}
	if afterImgs != beforeImgs-plan.deletedImgs {
		return ErrOldClientEditUnsupported
	}
	if afterLinks < beforeLinks-plan.deletedLinks {
		return ErrOldClientEditUnsupported
	}
	editedFlat := strings.Join(wantFlatParts, " ")
	for key, beforeCount := range beforeRuns {
		txt := key[strings.IndexByte(key, '|')+1:]
		if len(txt) > 3 && strings.Contains(editedFlat, txt) && afterRuns[key] < beforeCount {
			return ErrOldClientEditUnsupported
		}
	}
	return nil
}

// reconcileUpdatedGmailBody preserves rich HTML formatting, signature, and
// quoted history when a reviewer edits an email, including edits sent by older
// plain-text clients that only emit <div>-wrapped lines. It sanitizes incoming
// editor HTML, keeps body_text consistent with body_html, and refuses unmappable
// old-client edits rather than sending wrong text or broken formatting.
func reconcileUpdatedGmailBody(existingMessage map[string]any, mergedMessage map[string]any, input UpdateGmailEmailMutationInput) error {
	existingHTML := strings.TrimSpace(stringFromAny(existingMessage["body_html"]))
	existingText := stringFromAny(existingMessage["body_text"])

	if input.BodyEdited != nil && !*input.BodyEdited {
		mergedMessage["body_html"] = existingHTML
		mergedMessage["body_text"] = existingText
		return nil
	}

	_, hasIncomingHTML := input.Message["body_html"]
	_, hasIncomingText := input.Message["body_text"]
	if !hasIncomingHTML && !hasIncomingText {
		return nil
	}

	incomingHTML := strings.TrimSpace(stringFromAny(mergedMessage["body_html"]))
	incomingText := stringFromAny(mergedMessage["body_text"])

	existingBodyPart, existingQuotedHTML := splitGmailQuotedHTML(existingHTML)
	existingEditorHTML, existingSigHTML := splitEmailBodyAndSignatureHTML(existingBodyPart)
	existingQuotedText := ""
	if strings.TrimSpace(existingQuotedHTML) != "" {
		_, existingQuotedText = splitEmailQuotedText(existingText)
	}

	incomingBodyPart, incomingQuotedHTML := splitGmailQuotedHTML(incomingHTML)
	incomingEditorHTML, incomingSigHTML := splitEmailBodyAndSignatureHTML(incomingBodyPart)

	sigHTML := existingSigHTML
	if sigHTML == "" {
		sigHTML = incomingSigHTML
	}
	quotedHTML := existingQuotedHTML
	if quotedHTML == "" {
		quotedHTML = incomingQuotedHTML
	}

	var finalEditorHTML string
	deriveBodyText := input.EditorFormat == "html" || strings.TrimSpace(incomingText) == ""
	switch {
	case input.EditorFormat == "html":
		finalEditorHTML = sanitizeEmailEditorHTML(incomingEditorHTML)
	case existingEditorHTML != "" && (incomingEditorHTML == "" || isPlainDivHTML(incomingEditorHTML)) &&
		normalizeForNoOpCompare(htmlEmailText(incomingEditorHTML)) == normalizeForNoOpCompare(htmlEmailText(existingEditorHTML)):
		finalEditorHTML = existingEditorHTML
	case existingEditorHTML != "" && hasRichEmailFormatting(existingEditorHTML) && (incomingEditorHTML == "" || isPlainDivHTML(incomingEditorHTML)):
		// An older client (such as an un-updated mobile APK) sent plain text
		// or <div>-wrapped plain text against a rich HTML email. Patch only the
		// changed text onto the stored rich HTML DOM and verify the result.
		var newPlain string
		plainFromText := ""
		if strings.TrimSpace(incomingText) != "" {
			plainWithoutQuote := incomingText
			if strings.TrimSpace(quotedHTML) != "" {
				plainWithoutQuote, _ = splitEmailQuotedText(incomingText)
			}
			plainFromText = emailTextWithoutSignature(plainWithoutQuote)
		}
		if incomingEditorHTML != "" {
			newPlain = htmlEmailText(incomingEditorHTML)
			// Prefer incomingText when it carries the same text (preserving
			// tab cell separators that HTML whitespace collapsing drops).
			if plainFromText != "" && normalizeForNoOpCompare(plainFromText) == normalizeForNoOpCompare(newPlain) {
				newPlain = plainFromText
			}
		} else {
			newPlain = plainFromText
		}
		patched, err := patchRichHTMLWithPlainTextEdit(existingEditorHTML, newPlain)
		if err != nil {
			return err
		}
		finalEditorHTML = patched
		deriveBodyText = true
	case incomingEditorHTML != "":
		finalEditorHTML = sanitizeEmailEditorHTML(incomingEditorHTML)
	default:
		// Pure plain-text email with no existing HTML.
		if existingHTML == "" && incomingHTML == "" {
			return nil
		}
		plainWithoutQuote := incomingText
		if strings.TrimSpace(quotedHTML) != "" {
			plainWithoutQuote, _ = splitEmailQuotedText(incomingText)
		}
		finalEditorHTML = emailPlainTextToHTML(emailTextWithoutSignature(plainWithoutQuote))
	}

	// If nothing in the HTML body actually changed, keep the stored body_html
	// and body_text verbatim so no-op saves never perturb markup or whitespace.
	if finalEditorHTML == existingEditorHTML && sigHTML == existingSigHTML && quotedHTML == existingQuotedHTML && existingHTML != "" {
		mergedMessage["body_html"] = existingHTML
		if existingText != "" {
			mergedMessage["body_text"] = existingText
		}
		return nil
	}

	htmlParts := []string{}
	if strings.TrimSpace(finalEditorHTML) != "" {
		htmlParts = append(htmlParts, strings.TrimSpace(finalEditorHTML))
	}
	if strings.TrimSpace(sigHTML) != "" {
		htmlParts = append(htmlParts, strings.TrimSpace(sigHTML))
	}
	if strings.TrimSpace(quotedHTML) != "" {
		htmlParts = append(htmlParts, strings.TrimSpace(quotedHTML))
	}
	mergedMessage["body_html"] = strings.Join(htmlParts, "<div><br></div>")

	if !deriveBodyText {
		return nil
	}

	// Keep body_text consistent with the resulting HTML while preserving
	// original "> "-prefixed quoted text only when quotedHTML is present.
	textParts := []string{}
	if editorText := strings.TrimSpace(htmlEmailText(finalEditorHTML)); editorText != "" {
		textParts = append(textParts, editorText)
	}
	if sigText := strings.TrimSpace(htmlFragmentText(sigHTML)); sigText != "" {
		textParts = append(textParts, sigText)
	}
	if strings.TrimSpace(quotedHTML) != "" {
		quotedText := existingQuotedText
		if quotedText == "" {
			quotedText = strings.TrimSpace(htmlEmailText(quotedHTML))
		}
		if quotedText != "" {
			textParts = append(textParts, quotedText)
		}
	}
	if len(textParts) > 0 {
		mergedMessage["body_text"] = strings.Join(textParts, "\n\n") + "\n"
	}
	return nil
}
