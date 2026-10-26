package screentimelapse

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"image"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// The card guard. A moment whose accessibility tree holds a payment card
// number is never sent as written: the tree goes up with the number replaced,
// a screenshot whose display the hit can be placed on exactly goes up with a
// red box over the number and the card's other fields, and every other
// screenshot of the moment is withheld. The tree only sees the frontmost app,
// so a screen the box does not touch may show the card in pixels alone (on
// 2026-10-21 the full card was on the second display while the tree had it in
// a checkout form on the first).

const cardReason = "card number in accessibility tree"

const redactedCardNumber = "[redacted card number]"

// cardGroupings are the separated layouts a card number is printed or typed
// in. A run of digit groups is checked only in these shapes, so a line of
// spaced figures in a terminal or spreadsheet does not offer every span of it
// to Luhn.
var cardGroupings = [][]int{
	{4, 4, 4, 4},    // 16: Visa, Mastercard, Discover
	{4, 6, 5},       // 15: Amex
	{4, 4, 4, 4, 3}, // 19: Visa, Discover
	{4, 3, 3, 3},    // 13: Visa
}

// findCardNumbers returns the byte spans of payment card numbers in text: 13
// to 19 digits, unbroken or in a card grouping joined by single spaces or
// dashes, that pass Luhn and carry a known issuer prefix for their length.
func findCardNumbers(text string) [][2]int {
	var spans [][2]int
	for i := 0; i < len(text); {
		if !isDigit(text[i]) || (i > 0 && isDigit(text[i-1])) {
			i++
			continue
		}
		groups := digitGroups(text, i)
		end := groups[len(groups)-1][1]
		if bounded(text, i, end) {
			spans = append(spans, cardSpans(text, groups)...)
		}
		i = end
	}
	return mergeSpans(spans)
}

// digitGroups reads the run of digit groups starting at start, each group
// joined to the next by exactly one space or dash.
func digitGroups(text string, start int) [][2]int {
	var groups [][2]int
	i := start
	for {
		j := i
		for j < len(text) && isDigit(text[j]) {
			j++
		}
		groups = append(groups, [2]int{i, j})
		if j+1 < len(text) && (text[j] == ' ' || text[j] == '-') && isDigit(text[j+1]) {
			i = j + 1
			continue
		}
		return groups
	}
}

// bounded is false for a run glued to a word ("id4111..."), unless the letter
// is a JSON escape ("\n4111..."), and for a URL path segment: a 19-digit
// Instagram story id passed Luhn with a Visa prefix in a 2026-10-25 tree.
func bounded(text string, start, end int) bool {
	if start > 0 && isLetter(text[start-1]) && !(start > 1 && text[start-2] == '\\') {
		return false
	}
	if start > 0 && text[start-1] == '/' {
		return false
	}
	return end >= len(text) || !isLetter(text[end])
}

func cardSpans(text string, groups [][2]int) [][2]int {
	if len(groups) == 1 {
		if digits := text[groups[0][0]:groups[0][1]]; isCardNumber(digits) {
			return [][2]int{groups[0]}
		}
		return nil
	}
	var spans [][2]int
	for first := range groups {
		for _, shape := range cardGroupings {
			last := first + len(shape) - 1
			if last >= len(groups) || !groupsHaveShape(groups[first:last+1], shape) {
				continue
			}
			var digits strings.Builder
			for _, g := range groups[first : last+1] {
				digits.WriteString(text[g[0]:g[1]])
			}
			if isCardNumber(digits.String()) {
				spans = append(spans, [2]int{groups[first][0], groups[last][1]})
			}
		}
	}
	return spans
}

func groupsHaveShape(groups [][2]int, shape []int) bool {
	for i, g := range groups {
		if g[1]-g[0] != shape[i] {
			return false
		}
	}
	return true
}

// isCardNumber is Luhn plus an issuer prefix valid for the length: Visa 4
// (13, 16, 19), Mastercard 51-55 and 2221-2720 (16), Amex 34 and 37 (15),
// Discover 6011 and 65 (16-19).
func isCardNumber(digits string) bool {
	n := len(digits)
	if n < 13 || n > 19 || !luhn(digits) {
		return false
	}
	prefix := func(k int) int {
		v, _ := strconv.Atoi(digits[:k])
		return v
	}
	switch {
	case digits[0] == '4':
		return n == 13 || n == 16 || n == 19
	case prefix(2) >= 51 && prefix(2) <= 55, prefix(4) >= 2221 && prefix(4) <= 2720:
		return n == 16
	case prefix(2) == 34 || prefix(2) == 37:
		return n == 15
	case prefix(4) == 6011 || prefix(2) == 65:
		return n >= 16
	}
	return false
}

func luhn(digits string) bool {
	sum := 0
	double := false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}

func mergeSpans(spans [][2]int) [][2]int {
	if len(spans) < 2 {
		return spans
	}
	// Spans arrive in text order of their first group.
	out := [][2]int{spans[0]}
	for _, s := range spans[1:] {
		last := &out[len(out)-1]
		if s[0] <= last[1] {
			if s[1] > last[1] {
				last[1] = s[1]
			}
			continue
		}
		out = append(out, s)
	}
	return out
}

func redactCardNumbers(text string, spans [][2]int) string {
	var b strings.Builder
	at := 0
	for _, s := range spans {
		b.WriteString(text[at:s[0]])
		b.WriteString(redactedCardNumber)
		at = s[1]
	}
	b.WriteString(text[at:])
	return b.String()
}

func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isLetter(c byte) bool { return (c|0x20) >= 'a' && (c|0x20) <= 'z' }

// screen is a display in the tree's coordinate space, and the integer scales
// an image of it may have been captured at.
type screen struct {
	rect   image.Rectangle
	scales []int
}

// treeScan is what a moment's tree says about card numbers.
type treeScan struct {
	hits int
	// redacted is the tree with every number replaced, compressed as the
	// file was; nil when there is no hit.
	redacted []byte
	// placeable: every hit sits on a node with a frame and the tree names
	// its screens, so boxes can be drawn.
	placeable  bool
	hitRects   []image.Rectangle
	boxRects   []image.Rectangle // hits plus the card's other fields
	screens    []screen
	unreadable bool
}

// axNode is one element of a tree, from any of the three formats.
type axNode struct {
	texts    []string // every string the node itself carries
	labels   []string // what names it: title, description, identifier, hint
	value    string
	frame    *image.Rectangle
	parent   *axNode
	children []*axNode
}

var gzipMagic = []byte{0x1f, 0x8b}

// scanTree reads a tree file's bytes: gzipped when they say so (the Mac's
// .ax.json.gz and the Android phone's .xml.gz), plain otherwise (the
// iPhone's .json).
func scanTree(content []byte) (treeScan, error) {
	gzipped := bytes.HasPrefix(content, gzipMagic)
	text := content
	if gzipped {
		reader, err := gzip.NewReader(bytes.NewReader(content))
		if err != nil {
			return treeScan{unreadable: true}, nil
		}
		text, err = io.ReadAll(reader)
		if err != nil {
			return treeScan{unreadable: true}, nil
		}
	}
	spans := findCardNumbers(string(text))
	if len(spans) == 0 {
		return treeScan{}, nil
	}
	scan := treeScan{hits: len(spans)}
	redacted := []byte(redactCardNumbers(string(text), spans))
	if gzipped {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(redacted); err != nil {
			return treeScan{}, err
		}
		if err := zw.Close(); err != nil {
			return treeScan{}, err
		}
		redacted = buf.Bytes()
	}
	scan.redacted = redacted

	root, screens, ok := parseTree(text)
	if !ok {
		return scan, nil
	}
	scan.screens = screens
	nodeHits := 0
	placeable := len(screens) > 0
	boxes := map[image.Rectangle]bool{}
	var addBox = func(r image.Rectangle) {
		if !boxes[r] {
			boxes[r] = true
			scan.boxRects = append(scan.boxRects, r)
		}
	}
	walkNodes(root, func(n *axNode) {
		count := 0
		for _, s := range n.texts {
			count += len(findCardNumbers(s))
		}
		if count == 0 {
			return
		}
		nodeHits += count
		if n.frame == nil || n.frame.Empty() {
			placeable = false
			return
		}
		scan.hitRects = append(scan.hitRects, *n.frame)
		addBox(*n.frame)
		for _, r := range cardFieldsNear(n) {
			addBox(r)
		}
	})
	// A number the node walk cannot attribute (in a key, split by an
	// escape) has no frame to box.
	scan.placeable = placeable && nodeHits >= scan.hits
	return scan, nil
}

// cardDetailSearchDepth bounds how far up from a hit the card's other fields
// are looked for, so a label elsewhere in the window does not count.
const cardDetailSearchDepth = 6

var expiryValue = regexp.MustCompile(`^\s*(0[1-9]|1[0-2])\s*/\s*(\d{2}|\d{4})\s*$`)

var cardDetailLabels = []string{"cvc", "cvv", "csc", "security code", "expir", "cardholder", "name on card", "billing", "address", "zip", "postal"}

func isCardDetail(n *axNode) bool {
	if n.frame == nil || n.frame.Empty() {
		return false
	}
	if expiryValue.MatchString(n.value) {
		return true
	}
	for _, label := range n.labels {
		label = strings.ToLower(label)
		for _, want := range cardDetailLabels {
			if strings.Contains(label, want) {
				return true
			}
		}
	}
	return false
}

// cardFieldsNear is the frames of the expiry, CVC, name and address fields
// under the hit's lowest ancestor that holds any: the card form or widget.
func cardFieldsNear(hit *axNode) []image.Rectangle {
	ancestor := hit.parent
	for depth := 0; ancestor != nil && depth < cardDetailSearchDepth; depth++ {
		var found []image.Rectangle
		walkNodes(ancestor, func(n *axNode) {
			if n != hit && isCardDetail(n) {
				found = append(found, *n.frame)
			}
		})
		if len(found) > 0 {
			return found
		}
		ancestor = ancestor.parent
	}
	return nil
}

func walkNodes(n *axNode, visit func(*axNode)) {
	if n == nil {
		return
	}
	visit(n)
	for _, c := range n.children {
		walkNodes(c, visit)
	}
}

// parseTree reads the Mac's or the iPhone's JSON, or the Android phone's
// uiautomator XML, into one node shape plus the screens the frames sit on.
func parseTree(text []byte) (*axNode, []screen, bool) {
	trimmed := bytes.TrimSpace(text)
	if len(trimmed) == 0 {
		return nil, nil, false
	}
	if trimmed[0] == '<' {
		return parseUIAutomator(trimmed)
	}
	var doc any
	if err := json.Unmarshal(trimmed, &doc); err != nil {
		return nil, nil, false
	}
	root := jsonNode(doc, nil)
	var screens []screen
	if top, ok := doc.(map[string]any); ok {
		// The Mac's snapshot: frames and displays in global points, and
		// a Retina display is imaged at 2x.
		if displays, ok := top["displays"].([]any); ok {
			for _, d := range displays {
				if r, ok := jsonRect(d); ok {
					screens = append(screens, screen{rect: r, scales: []int{1, 2, 3}})
				}
			}
		}
	}
	return root, screens, true
}

// jsonNode makes every JSON object a node; arrays of objects under any key
// (children, apps, windows, screen_windows) are its children.
func jsonNode(v any, parent *axNode) *axNode {
	n := &axNode{parent: parent}
	switch t := v.(type) {
	case map[string]any:
		for key, value := range t {
			switch value := value.(type) {
			case string:
				n.texts = append(n.texts, value)
				switch key {
				case "title", "description", "identifier", "placeholder":
					n.labels = append(n.labels, value)
				case "value":
					n.value = value
				}
			case []any:
				if key == "frame" || key == "bounds" {
					if r, ok := jsonRect(value); ok {
						n.frame = &r
					}
					continue
				}
				for _, item := range value {
					n.children = append(n.children, jsonNode(item, n))
				}
			case map[string]any:
				n.children = append(n.children, jsonNode(value, n))
			}
		}
	case []any:
		for _, item := range t {
			n.children = append(n.children, jsonNode(item, n))
		}
	case string:
		n.texts = append(n.texts, t)
	}
	return n
}

func jsonRect(v any) (image.Rectangle, bool) {
	values, ok := v.([]any)
	if !ok || len(values) != 4 {
		return image.Rectangle{}, false
	}
	var xywh [4]int
	for i, value := range values {
		f, ok := value.(float64)
		if !ok {
			return image.Rectangle{}, false
		}
		xywh[i] = int(f)
	}
	return image.Rect(xywh[0], xywh[1], xywh[0]+xywh[2], xywh[1]+xywh[3]), true
}

var uiautomatorBounds = regexp.MustCompile(`^\[(-?\d+),(-?\d+)\]\[(-?\d+),(-?\d+)\]$`)

// parseUIAutomator reads Android's dump: bounds are the screenshot's own
// pixels, and the screen is what the top-level nodes cover.
func parseUIAutomator(text []byte) (*axNode, []screen, bool) {
	decoder := xml.NewDecoder(bytes.NewReader(text))
	root := &axNode{}
	current := root
	var screenRect image.Rectangle
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, false
		}
		switch t := token.(type) {
		case xml.StartElement:
			if t.Name.Local != "node" {
				continue
			}
			n := &axNode{parent: current}
			for _, a := range t.Attr {
				switch a.Name.Local {
				case "text":
					n.texts = append(n.texts, a.Value)
					n.labels = append(n.labels, a.Value)
					n.value = a.Value
				case "content-desc", "hint", "resource-id":
					n.texts = append(n.texts, a.Value)
					n.labels = append(n.labels, a.Value)
				case "bounds":
					if m := uiautomatorBounds.FindStringSubmatch(a.Value); m != nil {
						x1, _ := strconv.Atoi(m[1])
						y1, _ := strconv.Atoi(m[2])
						x2, _ := strconv.Atoi(m[3])
						y2, _ := strconv.Atoi(m[4])
						r := image.Rect(x1, y1, x2, y2)
						n.frame = &r
					}
				}
			}
			if current == root && n.frame != nil {
				screenRect = screenRect.Union(*n.frame)
			}
			current.children = append(current.children, n)
			current = n
		case xml.EndElement:
			if t.Name.Local == "node" && current.parent != nil {
				current = current.parent
			}
		}
	}
	var screens []screen
	if !screenRect.Empty() && screenRect.Min == (image.Point{}) {
		screens = []screen{{rect: screenRect, scales: []int{1}}}
	}
	return root, screens, true
}

// boxPadding is drawn around every frame, in image pixels.
const boxPadding = 8

// imageBoxes decides, for every screenshot of a moment given by its pixel
// size, the boxes to draw on it. ok is false when the screenshots cannot be
// tied to the tree's screens with certainty: an image whose size fits no
// screen or more than one, two images on one screen, a hit on no imaged
// screen. Then nothing of the moment's pixels can be sent. A nil entry means
// the boxes do not touch that image.
func imageBoxes(scan treeScan, sizes []image.Point) ([][]image.Rectangle, error) {
	if !scan.placeable {
		return nil, fmt.Errorf("the hit has no frame on a known screen")
	}
	type placement struct {
		screen screen
		scale  int
	}
	placements := make([]placement, len(sizes))
	used := map[int]bool{}
	for i, size := range sizes {
		matched := -1
		for s, scr := range scan.screens {
			for _, scale := range scr.scales {
				if size.X == scr.rect.Dx()*scale && size.Y == scr.rect.Dy()*scale {
					if matched >= 0 && matched != s {
						return nil, fmt.Errorf("an image of %dx%d fits more than one screen", size.X, size.Y)
					}
					matched = s
					placements[i] = placement{screen: scr, scale: scale}
				}
			}
		}
		if matched < 0 {
			return nil, fmt.Errorf("an image of %dx%d fits no screen", size.X, size.Y)
		}
		if used[matched] {
			return nil, fmt.Errorf("two images fit one screen")
		}
		used[matched] = true
	}
	for _, hit := range scan.hitRects {
		onImage := false
		for _, p := range placements {
			if hit.Overlaps(p.screen.rect) {
				onImage = true
			}
		}
		if !onImage {
			return nil, fmt.Errorf("a hit is on no imaged screen")
		}
	}
	out := make([][]image.Rectangle, len(sizes))
	for i, p := range placements {
		bounds := image.Rect(0, 0, sizes[i].X, sizes[i].Y)
		for _, r := range scan.boxRects {
			clipped := r.Intersect(p.screen.rect)
			if clipped.Empty() {
				continue
			}
			px := image.Rect(
				(clipped.Min.X-p.screen.rect.Min.X)*p.scale-boxPadding,
				(clipped.Min.Y-p.screen.rect.Min.Y)*p.scale-boxPadding,
				(clipped.Max.X-p.screen.rect.Min.X)*p.scale+boxPadding,
				(clipped.Max.Y-p.screen.rect.Min.Y)*p.scale+boxPadding,
			).Intersect(bounds)
			if !px.Empty() {
				out[i] = append(out[i], px)
			}
		}
	}
	return out, nil
}
