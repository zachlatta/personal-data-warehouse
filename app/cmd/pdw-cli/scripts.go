package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zachlatta/personal-data-warehouse/app/internal/cliclient"
	"io"
	"os"
	"strings"
	"time"
)

func runScript(client *cliclient.Client, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var file string
	var input json.RawMessage
	timeout := 30000
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--args":
			i++
			if i >= len(args) || !strings.HasPrefix(args[i], "@") {
				fmt.Fprintln(stderr, "pdw run: --args requires @file.json")
				return 2
			}
			b, e := os.ReadFile(strings.TrimPrefix(args[i], "@"))
			if e != nil || !json.Valid(b) {
				fmt.Fprintln(stderr, "pdw run: args must be a readable JSON file")
				return 2
			}
			input = b
		case "--timeout-ms":
			i++
			if i >= len(args) {
				return 2
			}
			if _, e := fmt.Sscan(args[i], &timeout); e != nil {
				return 2
			}
		default:
			if file != "" || (strings.HasPrefix(args[i], "-") && args[i] != "-") {
				fmt.Fprintln(stderr, "pdw run: pass one script file or - for stdin")
				return 2
			}
			file = args[i]
		}
	}
	if file == "" {
		fmt.Fprintln(stderr, "pdw run: pdw run script.js [--args @file.json] or pdw run -")
		return 2
	}
	var reader io.Reader = stdin
	var source *os.File
	if file != "-" {
		var e error
		source, e = os.Open(file)
		if e != nil {
			fmt.Fprintln(stderr, "pdw run:", e)
			return 2
		}
		defer source.Close()
		reader = source
	}
	code, e := io.ReadAll(io.LimitReader(reader, 32769))
	if e != nil || len(code) > 32768 || len(input) > 32768 {
		fmt.Fprintln(stderr, "pdw run: source and args must each fit 32 KiB")
		return 2
	}
	body, _ := json.Marshal(map[string]any{"code": string(code), "args": input, "timeout_ms": timeout})
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	result, e := client.CallTool(ctx, "run", body)
	if e != nil {
		fmt.Fprintln(stderr, "pdw run:", e)
		return 1
	}
	fmt.Fprintln(stdout, string(result))
	var response struct {
		Error any `json:"error"`
	}
	_ = json.Unmarshal(result, &response)
	if response.Error != nil {
		return 1
	}
	return 0
}
