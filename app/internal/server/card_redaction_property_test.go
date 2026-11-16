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
			Seed               uint32
			Iterations         int
			EncodingIterations int `json:"encoding_iterations"`
			Groupings          [][]int
			SeparatorVariants  []string `json:"separator_variants"`
			PrecedingContexts  []string `json:"preceding_contexts"`
			FollowingContexts  []struct {
				Input    string
				Expected string
			} `json:"following_contexts"`
			PermutationTokens  []string `json:"permutation_tokens"`
			PermutationJoiners []string `json:"permutation_joiners"`
			CorpusSHA256       string   `json:"corpus_sha256"`
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
		groups  []int
		variant string
	}
	formats := []format{}
	for _, group := range config.Groupings {
		seps := config.SeparatorVariants
		if len(group) == 1 {
			seps = []string{"plain"}
		}
		for _, sep := range seps {
			formats = append(formats, format{group, sep})
		}
	}
	if config.Iterations < 20000 {
		t.Fatal("property corpus must include thousands of cases")
	}
	for i := 0; i < config.Iterations; i++ {
		area := fmt.Sprint(200 + rand(700))
		exchange := fmt.Sprint(200 + rand(700))
		line := digits(4)
		phone := area + "-" + exchange + "-" + line
		phone2 := "(202) " + fmt.Sprint(200+rand(700)) + "-" + digits(4)
		date := fmt.Sprintf("%04d-%02d-%02d", 1000+rand(2000), 1+rand(12), 1+rand(28))
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
		quantity := ""
		for n := 10; n < 100; n++ {
			if valid(order + fmt.Sprint(n)) {
				quantity = fmt.Sprint(n)
				break
			}
		}
		aggregate := number(18)
		aggregateBad := aggregate[:17] + string(rune('0'+(int(aggregate[17]-'0')+1)%10))
		nonLayoutGroups := []int{4, 4, 4, 5 + i%3}
		if i%3 == 0 {
			nonLayoutGroups = []int{6, 6, 6}
		}
		nonLayoutLength := 0
		for _, n := range nonLayoutGroups {
			nonLayoutLength += n
		}
		nonLayout := propertyRenderGroups(number(nonLayoutLength), nonLayoutGroups, config.SeparatorVariants[i%6])
		tokens := []string{phone, "+1-" + phone, phone2, date + "T" + clock + "Z", strings.ReplaceAll(date, "-", "/"), clock, strings.Join(uuidParts, "-"), order, strings.Repeat("9", 40+rand(40)), digits(3) + "." + digits(2), order + " " + quantity, aggregate[:12] + " " + aggregate[12:16] + " " + aggregate[16:], aggregateBad[:12] + " " + aggregateBad[12:16] + " " + aggregateBad[16:], "id=" + nonLayout + ";"}
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
		context := config.PrecedingContexts[(i/len(formats))%len(config.PrecedingContexts)]
		contextLine := line
		for valid(contextLine) || valid(contextLine+"0") {
			contextLine = contextLine[:3] + string(rune('0'+(int(contextLine[3]-'0')+1)%10))
		}
		contextPhone := area + "-" + exchange + "-" + contextLine
		if replacement, ok := map[string]string{"$four_digits ": contextLine + " ", "$phone ": contextPhone + " ", "$phone_prefix ": area + " " + exchange + " ", "$date ": date + " ", "$order ": order + " "}[context]; ok {
			context = replacement
		}
		following := config.FollowingContexts[(i/(len(formats)*len(config.PrecedingContexts)))%len(config.FollowingContexts)]
		length := 0
		for _, n := range f.groups {
			length += n
		}
		pan, grouped := "", ""
		for attempt := 0; attempt < 1000; attempt++ {
			pan = number(length)
			grouped = propertyRenderGroups(pan, f.groups, f.variant)
			if propertyUnambiguousInsertion(context, grouped, following.Input, config.Groupings, valid) {
				break
			}
			if attempt == 999 {
				t.Fatal("could not construct unambiguous synthetic PAN")
			}
		}
		replacement := "[card ****" + pan[len(pan)-4:] + "]"
		at := rand(len(tokens) + 1)
		prefix := strings.Join(tokens[:at], joiner) + " | " + context
		suffix := " | " + strings.Join(tokens[at:], joiner)
		value, want := prefix+grouped+following.Input+suffix, prefix+replacement+following.Expected+suffix
		fmt.Fprintf(digest, "insert-%d\x00%s\x00%s\n", i, value, want)
		if redactCardText(value, false) != want {
			t.Fatalf("inserted card or surrounding prose changed at case %d, grouping %v, variant %s", i, f.groups, f.variant)
		}
	}
	for i, a := range config.PermutationTokens {
		for j, b := range config.PermutationTokens {
			for k, c := range config.PermutationTokens {
				if i == j || j == k || i == k {
					continue
				}
				for n, sep := range config.PermutationJoiners {
					value := strings.Join([]string{a, b, c}, sep)
					fmt.Fprintf(digest, "permutation-preserve-%d-%d-%d-%d\x00%s\x00%s\n", i, j, k, n, value, value)
					if redactCardText(value, false) != value {
						t.Fatal("numeric permutation changed")
					}
					input, want := "4111111111111111 | "+value, "[card ****1111] | "+value
					fmt.Fprintf(digest, "permutation-insert-%d-%d-%d-%d\x00%s\x00%s\n", i, j, k, n, input, want)
					if redactCardText(input, false) != want {
						t.Fatal("PAN-adjacent numeric permutation changed")
					}
				}
			}
		}
	}
	if config.EncodingIterations < 3000 {
		t.Fatal("encoded corpus must include thousands of cases")
	}
	for i := 0; i < config.EncodingIterations; i++ {
		f := formats[i%len(formats)]
		length := 0
		for _, n := range f.groups {
			length += n
		}
		pan, grouped := "", ""
		for attempt := 0; attempt < 1000; attempt++ {
			pan = number(length)
			grouped = propertyRenderGroups(pan, f.groups, f.variant)
			if propertyUnambiguousInsertion("", grouped, " CVV2: 987", config.Groupings, valid) {
				break
			}
			if attempt == 999 {
				t.Fatal("could not construct unambiguous encoded PAN")
			}
		}
		depth, mixed, slash := 1+i%3, (i/6)%2 == 1, (i/3)%2 == 1
		value, want := propertyEncodedDocument(grouped, "[card ****"+pan[len(pan)-4:]+"]", depth, mixed, slash)
		fmt.Fprintf(digest, "encoded-insert-%d\x00%s\x00%s\n", i, value, want)
		got := redactCardText(value, false)
		if got != want || cardCount(value) != 1 || cardCount(got) != 0 {
			t.Fatalf("encoded card failed at case %d", i)
		}
		value, want = propertyEncodedDocument("4111111111111112", "", depth, mixed, slash)
		fmt.Fprintf(digest, "encoded-preserve-%d\x00%s\x00%s\n", i, value, want)
		if redactCardText(value, false) != want || cardCount(value) != 0 {
			t.Fatalf("encoded non-card changed at case %d", i)
		}
	}
	if fmt.Sprintf("%x", digest.Sum(nil)) != config.CorpusSHA256 {
		t.Fatal("property corpus differs from the shared fixed-seed corpus")
	}
}

func propertyEncodedDocument(pan, replacement string, depth int, mixed, slash bool) (string, string) {
	var encoded strings.Builder
	for i, c := range pan {
		if c >= '0' && c <= '9' && (!mixed || i%2 == 0) {
			fmt.Fprintf(&encoded, `\u%04x`, c)
		} else {
			encoded.WriteRune(c)
		}
	}
	value := `{ "text":"` + encoded.String() + ` CVV2: 987", "keep":1e400, "dup":1,"dup":2, "u":"\u00e9" }`
	want := value
	if replacement != "" {
		want = strings.ReplaceAll(strings.ReplaceAll(value, encoded.String(), replacement), " CVV2: 987", " CVV2: [redacted]")
	}
	if slash {
		value = strings.ReplaceAll(value, `\`, `\u005c`)
		want = strings.ReplaceAll(want, `\`, `\u005c`)
	}
	for j := 0; j < depth-1; j++ {
		v, _ := json.Marshal(map[string]string{"payload": value})
		w, _ := json.Marshal(map[string]string{"payload": want})
		value, want = string(v), string(w)
		if slash {
			value = strings.ReplaceAll(value, `\\`, `\u005c`)
			want = strings.ReplaceAll(want, `\\`, `\u005c`)
		}
	}
	return value, want
}

func propertyRenderGroups(number string, group []int, variant string) string {
	options := map[string][]string{"plain": {""}, "space": {" "}, "dash": {"-"}, "double": {"  "}, "triple": {"   "}, "mixed": {"-", " "}, "mixed_runs": {" -", "- ", " - "}}[variant]
	var result strings.Builder
	pos := 0
	for i, n := range group {
		if i > 0 {
			result.WriteString(options[(i-1)%len(options)])
		}
		result.WriteString(number[pos : pos+n])
		pos += n
	}
	return result.String()
}

// Independent exhaustive checksum oracle, not a call into the redactor. Exact
// insertion ground truth has one valid reading; ambiguity has separate fixtures.
func propertyUnambiguousInsertion(context, pan, following string, layouts [][]int, checksum func(string) bool) bool {
	value := context + pan + following
	atoms := [][2]int{}
	for i := 0; i < len(value); {
		if value[i] < '0' || value[i] > '9' {
			i++
			continue
		}
		start := i
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			i++
		}
		atoms = append(atoms, [2]int{start, i})
	}
	count := 0
	correct := false
	for i, a := range atoms {
		digits := ""
		groups := []int{}
		for j := i; j < len(atoms); j++ {
			if j > i {
				sep := value[atoms[j-1][1]:atoms[j][0]]
				ok := len(sep) >= 1 && len(sep) <= 3
				for _, c := range sep {
					if c != ' ' && c != '-' {
						ok = false
					}
				}
				if !ok {
					break
				}
			}
			digits += value[atoms[j][0]:atoms[j][1]]
			groups = append(groups, atoms[j][1]-atoms[j][0])
			if len(digits) > 19 {
				break
			}
			layout := false
			for _, expected := range layouts {
				if len(groups) != len(expected) {
					continue
				}
				equal := true
				for k, n := range groups {
					if n != expected[k] {
						equal = false
						break
					}
				}
				if equal {
					layout = true
					break
				}
			}
			if layout && checksum(digits) {
				count++
				correct = a[0] == len(context) && atoms[j][1] == len(context)+len(pan)
			}
		}
	}
	return count == 1 && correct
}
