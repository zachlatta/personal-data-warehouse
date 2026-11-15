package server

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Independent generator; the same unsigned LCG, fixture configuration and
// construction order in tests/card_redaction_properties.py produce this corpus.
func TestGeneratedNumericProseAndInsertedCardProperties(t *testing.T) {
	data, err := os.ReadFile("../../../tests/fixtures/card_redaction_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Property struct {
			Seed         uint32
			Iterations   int
			Groupings    [][]int
			Delimiters   []string
			CorpusSHA256 string `json:"corpus_sha256"`
		}
	}
	if json.Unmarshal(data, &fixture) != nil {
		t.Fatal("invalid property fixture")
	}
	config := fixture.Property
	state := config.Seed
	digest := sha256.New()
	rand := func(n int) int { state = 1664525*state + 1013904223; return int(state % uint32(n)) }
	digits := func(n int) string {
		var s strings.Builder
		for j := 0; j < n; j++ {
			s.WriteByte(byte('0' + rand(10)))
		}
		return s.String()
	}
	valid := func(s string) bool {
		sum := 0
		for i, c := range []byte(s) {
			v := int(c - '0')
			if (len(s)-1-i)%2 == 1 {
				v *= 2
				if v > 9 {
					v -= 9
				}
			}
			sum += v
		}
		return sum%10 == 0
	}
	number := func(n int) string {
		prefix := "4" + digits(n-2)
		for j := 0; j < 10; j++ {
			s := prefix + string(rune('0'+j))
			if valid(s) {
				return s
			}
		}
		panic("missing checksum digit")
	}
	type format struct {
		groups    []int
		separator string
	}
	formats := []format{}
	for _, group := range config.Groupings {
		seps := []string{" ", "-"}
		if len(group) == 1 {
			seps = []string{""}
		}
		for _, sep := range seps {
			formats = append(formats, format{group, sep})
		}
	}
	if config.Iterations < 3000 {
		t.Fatal("property corpus must include thousands of cases")
	}
	for i := 0; i < config.Iterations; i++ {
		area := fmt.Sprint(200 + rand(700))
		exchange := fmt.Sprint(200 + rand(700))
		line := digits(4)
		phone := area + "-" + exchange + "-" + line
		phone2 := "(202) " + fmt.Sprint(200+rand(700)) + "-" + digits(4)
		date := fmt.Sprintf("%04d-%02d-%02d", 2000+rand(40), 1+rand(12), 1+rand(28))
		clock := fmt.Sprintf("%02d:%02d:%02d", rand(24), rand(60), rand(60))
		uuidParts := []string{}
		for _, n := range []int{8, 4, 4, 4, 12} {
			var part strings.Builder
			for j := 0; j < n; j++ {
				part.WriteByte("0123456789abcdef"[rand(16)])
			}
			uuidParts = append(uuidParts, part.String())
		}
		order := number(16)
		order = order[:15] + string(rune('0'+(int(order[15]-'0')+1)%10))
		tokens := []string{phone, "+1-" + phone, phone2, date + "T" + clock + "Z", strings.ReplaceAll(date, "-", "/"), clock, strings.Join(uuidParts, "-"), order, strings.Repeat("9", 40+rand(40)), digits(3) + "." + digits(2)}
		for j := len(tokens) - 1; j > 0; j-- {
			k := rand(j + 1)
			tokens[j], tokens[k] = tokens[k], tokens[j]
		}
		joiner := []string{" ", "; ", "\n", " | ", "\t"}[rand(5)]
		safe := strings.Join(tokens, joiner)
		fmt.Fprintf(digest, "preserve-%d\x00%s\x00%s\n", i, safe, safe)
		if redactCardText(safe, false) != safe {
			t.Fatalf("numeric prose changed at case %d", i)
		}
		f := formats[i%len(formats)]
		delimiter := config.Delimiters[(i/len(formats))%len(config.Delimiters)]
		length := 0
		for _, n := range f.groups {
			length += n
		}
		pan := number(length)
		parts := []string{}
		pos := 0
		for _, n := range f.groups {
			parts = append(parts, pan[pos:pos+n])
			pos += n
		}
		grouped := strings.Join(parts, f.separator)
		replacement := "[card ****" + pan[len(pan)-4:] + "]"
		bareEnd := !strings.ContainsRune("_-/:", rune(delimiter[0])) // all configured delimiters begin with punctuation/space
		if f.separator == " " && fmt.Sprint(f.groups) == "[4 4 4 4 3]" && valid(pan[:16]) && bareEnd {
			replacement = "[card ****" + pan[12:16] + "] [redacted]"
		}
		at := rand(len(tokens) + 1)
		prefix := strings.Join(tokens[:at], joiner) + " | "
		suffix := delimiter + " | " + strings.Join(tokens[at:], joiner)
		value, want := prefix+grouped+suffix, prefix+replacement+suffix
		fmt.Fprintf(digest, "insert-%d\x00%s\x00%s\n", i, value, want)
		if redactCardText(value, false) != want {
			t.Fatalf("inserted card or surrounding prose changed at case %d, grouping %v, delimiter %d", i, f.groups, (i/len(formats))%len(config.Delimiters))
		}
	}
	if fmt.Sprintf("%x", digest.Sum(nil)) != config.CorpusSHA256 {
		t.Fatal("property corpus differs from the shared fixed-seed corpus")
	}
}
