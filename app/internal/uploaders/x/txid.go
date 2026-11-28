package x

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// xtidEpoch is the custom epoch (2023-05-01T07:00:00Z) X's web client
	// subtracts from Unix seconds when signing a request.
	xtidEpoch int64 = 1682924400
	// xtidKeyword and xtidAdditionalNumber are the constants baked into X's
	// ondemand.s transaction-id script.
	xtidKeyword          = "obfiowerehiring"
	xtidAdditionalNumber = byte(3)
)

var (
	verificationMetaRe1 = regexp.MustCompile(`<meta[^>]+name=["']twitter-site-verification["'][^>]+content=["']([^"']+)["']`)
	verificationMetaRe2 = regexp.MustCompile(`<meta[^>]+content=["']([^"']+)["'][^>]+name=["']twitter-site-verification["']`)
	ondemandChunkIdxRe  = regexp.MustCompile(`[,{](\d+):["']ondemand\.s["']`)
	ondemandDirectMapRe = regexp.MustCompile(`["']ondemand\.s["']:\s*["']([0-9a-f]+)["']`)
	ondemandScriptURLRe = regexp.MustCompile(`https?://[A-Za-z0-9.:-]+/responsive-web/client-web(?:-legacy)?/ondemand\.s\.[0-9a-f]+a\.js`)
	ondemandIndicesRe   = regexp.MustCompile(`\(\w+\[(\d{1,2})\],\s*16\)`)
	animSVGRe           = regexp.MustCompile(`(?s)<svg[^>]*id=["']loading-x-anim-([0-3])["'][^>]*>(.*?)</svg>`)
	svgPathDRe          = regexp.MustCompile(`d=["']([^"']+)["']`)
	digitsRe            = regexp.MustCompile(`\d+`)
)

// TransactionGenerator computes the x-client-transaction-id header that the
// x.com web client attaches to GraphQL requests (required by endpoints such as
// UserTweetsAndReplies, which otherwise answer HTTP 404).
type TransactionGenerator struct {
	keyBytes     []byte
	animationKey string
	now          func() time.Time
	randByte     func() byte
}

// ParseOndemandScriptURL finds the ondemand.s script URL referenced by the
// signed-in x.com /home HTML. Returns "" when the page does not reference one.
func ParseOndemandScriptURL(html string, baseURL string) string {
	if u := ondemandScriptURLRe.FindString(html); u != "" {
		return u
	}
	var hash string
	if m := ondemandChunkIdxRe.FindStringSubmatch(html); m != nil {
		hashRe := regexp.MustCompile(`[,{]` + regexp.QuoteMeta(m[1]) + `:["']([0-9a-f]+)["']`)
		if hm := hashRe.FindStringSubmatch(html); hm != nil {
			hash = hm[1]
		}
	}
	if hash == "" {
		if m := ondemandDirectMapRe.FindStringSubmatch(html); m != nil {
			hash = m[1]
		}
	}
	if hash == "" {
		return ""
	}
	cdn := "https://abs.twimg.com"
	if baseURL != "" && baseURL != DefaultBaseURL {
		cdn = strings.TrimRight(baseURL, "/")
	}
	return cdn + "/responsive-web/client-web/ondemand.s." + hash + "a.js"
}

// NewTransactionGenerator builds a generator from the /home HTML (carrying the
// twitter-site-verification meta tag and loading-x-anim-0..3 SVGs) and the
// ondemand.s JS bundle (carrying the key-byte indices).
func NewTransactionGenerator(homeHTML, ondemandJS string) (*TransactionGenerator, error) {
	key, err := parseVerificationKey(homeHTML)
	if err != nil {
		return nil, err
	}
	keyBytes, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("decode twitter-site-verification key: %w", err)
	}
	if len(keyBytes) < 6 {
		return nil, fmt.Errorf("twitter-site-verification key too short (%d bytes)", len(keyBytes))
	}
	rowIdx, timeIndices, err := parseOndemandIndices(ondemandJS)
	if err != nil {
		return nil, err
	}
	frames, err := parseAnimFrames(homeHTML)
	if err != nil {
		return nil, err
	}
	animKey, err := computeAnimationKey(keyBytes, rowIdx, timeIndices, frames)
	if err != nil {
		return nil, err
	}
	return &TransactionGenerator{
		keyBytes:     keyBytes,
		animationKey: animKey,
		now:          func() time.Time { return time.Now().UTC() },
		randByte:     func() byte { return byte(rand.IntN(256)) },
	}, nil
}

// AnimationKey returns the derived animation key (exposed for testing).
func (g *TransactionGenerator) AnimationKey() string { return g.animationKey }

// Generate returns a fresh x-client-transaction-id header value for an HTTP
// method and request path (without query string, e.g.
// "/i/api/graphql/<queryId>/UserTweetsAndReplies").
func (g *TransactionGenerator) Generate(method, path string) string {
	return g.GenerateAt(method, path, g.now().Unix(), g.randByte())
}

// GenerateAt is Generate with explicit Unix seconds and mask byte, for
// deterministic tests.
func (g *TransactionGenerator) GenerateAt(method, path string, unixSec int64, mask byte) string {
	timeNow := uint32(unixSec - xtidEpoch)
	timeBytes := [4]byte{
		byte(timeNow),
		byte(timeNow >> 8),
		byte(timeNow >> 16),
		byte(timeNow >> 24),
	}
	msg := fmt.Sprintf("%s!%s!%d%s%s", method, path, timeNow, xtidKeyword, g.animationKey)
	sum := sha256.Sum256([]byte(msg))

	payload := make([]byte, 0, len(g.keyBytes)+4+16+1)
	payload = append(payload, g.keyBytes...)
	payload = append(payload, timeBytes[:]...)
	payload = append(payload, sum[:16]...)
	payload = append(payload, xtidAdditionalNumber)

	out := make([]byte, 1+len(payload))
	out[0] = mask
	for i, b := range payload {
		out[1+i] = b ^ mask
	}
	return base64.RawStdEncoding.EncodeToString(out)
}

func parseVerificationKey(html string) (string, error) {
	if m := verificationMetaRe1.FindStringSubmatch(html); m != nil {
		return m[1], nil
	}
	if m := verificationMetaRe2.FindStringSubmatch(html); m != nil {
		return m[1], nil
	}
	return "", errors.New("missing twitter-site-verification meta tag in x.com page")
}

func parseOndemandIndices(js string) (int, []int, error) {
	matches := ondemandIndicesRe.FindAllStringSubmatch(js, -1)
	if len(matches) < 2 {
		return 0, nil, errors.New("could not find key-byte indices in ondemand.s bundle")
	}
	indices := make([]int, 0, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, nil, err
		}
		indices = append(indices, n)
	}
	return indices[0], indices[1:], nil
}

// parseAnimFrames extracts the 4 loading-x-anim-0..3 SVG second-path 2D integer
// arrays from the /home HTML.
func parseAnimFrames(html string) ([4][][]int, error) {
	var frames [4][][]int
	var seen [4]bool
	for _, m := range animSVGRe.FindAllStringSubmatch(html, -1) {
		idx, err := strconv.Atoi(m[1])
		if err != nil || idx < 0 || idx >= 4 {
			continue
		}
		paths := svgPathDRe.FindAllStringSubmatch(m[2], -1)
		if len(paths) < 2 {
			continue
		}
		d := paths[1][1]
		if len(d) < 9 {
			continue
		}
		segments := strings.Split(d[9:], "C")
		rows := make([][]int, 0, len(segments))
		for _, seg := range segments {
			nums := digitsRe.FindAllString(seg, -1)
			row := make([]int, 0, len(nums))
			for _, s := range nums {
				n, err := strconv.Atoi(s)
				if err != nil {
					return frames, err
				}
				row = append(row, n)
			}
			rows = append(rows, row)
		}
		frames[idx] = rows
		seen[idx] = true
	}
	for i, ok := range seen {
		if !ok {
			return frames, fmt.Errorf("missing loading-x-anim-%d SVG in x.com page", i)
		}
	}
	return frames, nil
}

func computeAnimationKey(keyBytes []byte, rowIdx int, timeIndices []int, frames [4][][]int) (string, error) {
	if rowIdx >= len(keyBytes) {
		return "", fmt.Errorf("row index %d out of range for %d key bytes", rowIdx, len(keyBytes))
	}
	rowIndex := int(keyBytes[rowIdx]) % 16
	frameTime := 1
	for _, idx := range timeIndices {
		if idx >= len(keyBytes) {
			return "", fmt.Errorf("key-byte index %d out of range for %d key bytes", idx, len(keyBytes))
		}
		frameTime *= int(keyBytes[idx]) % 16
	}
	frameTime = int(jsRound(float64(frameTime)/10.0)) * 10

	arr := frames[int(keyBytes[5])%4]
	if rowIndex >= len(arr) {
		return "", fmt.Errorf("animation row %d out of range (%d rows)", rowIndex, len(arr))
	}
	frameRow := arr[rowIndex]
	if len(frameRow) < 11 {
		return "", fmt.Errorf("animation row %d too short (%d values)", rowIndex, len(frameRow))
	}
	targetTime := float64(frameTime) / 4096.0
	return animateFrame(frameRow, targetTime), nil
}

func animateFrame(frame []int, targetTime float64) string {
	fromColor := [4]float64{float64(frame[0]), float64(frame[1]), float64(frame[2]), 1}
	toColor := [4]float64{float64(frame[3]), float64(frame[4]), float64(frame[5]), 1}
	toRotation := solveScale(float64(frame[6]), 60.0, 360.0, true)
	var curves [4]float64
	for i := 0; i < 4; i++ {
		minVal := 0.0
		if i%2 != 0 {
			minVal = -1.0
		}
		curves[i] = solveScale(float64(frame[7+i]), minVal, 1.0, false)
	}
	val := cubicBezierValue(curves, targetTime)

	var parts []string
	for i := 0; i < 3; i++ {
		c := fromColor[i]*(1-val) + toColor[i]*val
		if c < 0 {
			c = 0
		} else if c > 255 {
			c = 255
		}
		parts = append(parts, strconv.FormatInt(int64(math.RoundToEven(c)), 16))
	}
	rotation := toRotation * val
	rad := rotation * math.Pi / 180.0
	matrix := [4]float64{math.Cos(rad), -math.Sin(rad), math.Sin(rad), math.Cos(rad)}
	for _, v := range matrix {
		rounded := math.RoundToEven(v*100) / 100
		if rounded < 0 {
			rounded = -rounded
		}
		hexVal := floatToHex(rounded)
		switch {
		case strings.HasPrefix(hexVal, "."):
			parts = append(parts, "0"+strings.ToLower(hexVal))
		case hexVal != "":
			parts = append(parts, hexVal)
		default:
			parts = append(parts, "0")
		}
	}
	parts = append(parts, "0", "0")
	joined := strings.Join(parts, "")
	joined = strings.ReplaceAll(joined, ".", "")
	joined = strings.ReplaceAll(joined, "-", "")
	return joined
}

func solveScale(value, minVal, maxVal float64, floorResult bool) float64 {
	res := value*(maxVal-minVal)/255.0 + minVal
	if floorResult {
		return math.Floor(res)
	}
	return math.RoundToEven(res*100) / 100
}

func jsRound(num float64) float64 {
	x := math.Floor(num)
	if (num - x) >= 0.5 {
		x = math.Ceil(num)
	}
	return math.Copysign(x, num)
}

func cubicBezierValue(c [4]float64, t float64) float64 {
	if t <= 0.0 {
		var grad float64
		if c[0] > 0.0 {
			grad = c[1] / c[0]
		} else if c[1] == 0.0 && c[2] > 0.0 {
			grad = c[3] / c[2]
		}
		return grad * t
	}
	if t >= 1.0 {
		var grad float64
		if c[2] < 1.0 {
			grad = (c[3] - 1.0) / (c[2] - 1.0)
		} else if c[2] == 1.0 && c[0] < 1.0 {
			grad = (c[1] - 1.0) / (c[0] - 1.0)
		}
		return 1.0 + grad*(t-1.0)
	}
	start, end := 0.0, 1.0
	var mid float64
	for start < end {
		mid = (start + end) / 2.0
		xEst := cubicCalc(c[0], c[2], mid)
		if math.Abs(t-xEst) < 0.00001 {
			return cubicCalc(c[1], c[3], mid)
		}
		if xEst < t {
			start = mid
		} else {
			end = mid
		}
	}
	return cubicCalc(c[1], c[3], mid)
}

func cubicCalc(a, b, m float64) float64 {
	om := 1.0 - m
	return 3.0*a*om*om*m + 3.0*b*om*m*m + m*m*m
}

func floatToHex(x float64) string {
	var digits []byte
	quotient := int(x)
	fraction := x - float64(quotient)
	cur := x
	for quotient > 0 {
		quotient = int(cur / 16.0)
		rem := int(cur - float64(quotient)*16.0)
		var ch byte
		if rem > 9 {
			ch = byte(rem + 55)
		} else {
			ch = byte('0' + rem)
		}
		digits = append([]byte{ch}, digits...)
		cur = float64(quotient)
	}
	if fraction == 0 {
		return string(digits)
	}
	digits = append(digits, '.')
	for fraction > 0 {
		fraction *= 16.0
		integer := int(fraction)
		fraction -= float64(integer)
		if integer > 9 {
			digits = append(digits, byte(integer+55))
		} else {
			digits = append(digits, byte('0'+integer))
		}
	}
	return string(digits)
}
