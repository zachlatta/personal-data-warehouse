package server

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

func TestRandomEncodedTokensArePreserved(t *testing.T) {
	raw, err := os.ReadFile("../../../tests/fixtures/card_encoded_token_property.json")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Seed       uint32
		Iterations int
		Alphabet   string
		Labels     []string
		Lengths    []int
	}
	if json.Unmarshal(raw, &config) != nil {
		t.Fatal("invalid encoding property config")
	}
	state := config.Seed
	rand := func(n int) int { state = 1664525*state + 1013904223; return int(state % uint32(n)) }
	text := func() string {
		s := "Q"
		n := rand(17)
		for j := 0; j < n; j++ {
			s += string(config.Alphabet[rand(len(config.Alphabet))])
		}
		return s
	}
	number := func(n int) string {
		prefix := "4" + strings.Repeat("1", n-2)
		for j := 0; j < 10; j++ {
			v := prefix + string(rune('0'+j))
			if validCard(v) {
				return v
			}
		}
		panic("checksum")
	}
	actual := number(16)
	if config.Iterations < 5000 {
		t.Fatal("too few encoded property cases")
	}
	for i := 0; i < config.Iterations; i++ {
		pan := number(config.Lengths[i%len(config.Lengths)])
		left, right, label := text(), text(), config.Labels[i%len(config.Labels)]
		if i%4 == 3 {
			label = "CVV-987"
		}
		before, after := "/", "/"
		if i%3 == 1 {
			before = "/" + label + "/"
		}
		if i%3 == 2 {
			after = "/" + label + "/"
		}
		value := left + before + pan + after + right
		codec := base64.StdEncoding.Strict()
		if i%2 == 1 {
			value = strings.NewReplacer("+", "-", "/", "_").Replace(value)
			codec = base64.URLEncoding.Strict()
		}
		if i%4 == 3 {
			value = strings.ReplaceAll(value, "_", "-")
		}
		value += strings.Repeat("A", (4-len(value)%4)%4)
		decoded, err := codec.DecodeString(value)
		if err != nil || codec.EncodeToString(decoded) != value {
			t.Fatalf("invalid encoded generator at %d", i)
		}
		if redactCardText(value, false) != value || cardCount(value) != 0 {
			t.Fatalf("encoded token changed at %d", i)
		}
		body, _ := json.Marshal(map[string]string{"text": value, "actual": actual})
		cleaned, err := redactConversationBatch(conversationTestBatch(string(body)), 100000)
		if err != nil {
			t.Fatal(err)
		}
		r, err := gzip.NewReader(bytes.NewReader(cleaned))
		if err != nil {
			t.Fatal(err)
		}
		out, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]string
		if json.Unmarshal(out, &got) != nil || got["text"] != value || got["actual"] == actual {
			t.Fatalf("batch field isolation failed at %d", i)
		}
	}
}

func TestMenuCompatibilityRequiresWholeTokenOrRealField(t *testing.T) {
	pan := "4" + strings.Repeat("1", 15)
	for _, label := range []string{"CVV", "CVC2"} {
		value := "menu" + pan + "/" + label + ": 987"
		if redactCardText(value, false) != "menu[card ****1111]/"+label+": [redacted]" {
			t.Fatal("menu with real field escaped masking")
		}
	}
	value := "menu" + pan + "/exp/Zm9v"
	if redactCardText(value, false) != value {
		t.Fatal("menu path token changed")
	}
}
