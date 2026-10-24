package androidmessages

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/zachlatta/personal-data-warehouse/app/internal/ingestclient"
)

const deviceUsage = `Usage: pdw mutations android-messages-device <send|find> [flags]

The phone half of android_messages.send_message, run as root on the Pixel by
porygon's "pdw mutations android-messages" worker over SSH. Not for humans.

  send   read a JSON request {mutation_id, recipient, text, since} on stdin,
         send the text through Google Messages unless Messages already holds
         an outgoing copy since "since", and print a JSON result
  find   read-only: print the newest outgoing message matching
         --recipient and --text since --since (RFC 3339), or nothing

Flags:
  --db PATH   Messages' database (default ` + DefaultDatabasePath + `)
`

// RunDevice is the `pdw mutations android-messages-device` entry point.
func RunDevice(args []string, stdin io.Reader, stdout, stderr io.Writer, _ func(string) string, _ ingestclient.Config) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(stdout, deviceUsage)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	command := args[0]
	flags := flag.NewFlagSet("android-messages-device "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	dbPath := flags.String("db", DefaultDatabasePath, "Messages' database")
	recipient := flags.String("recipient", "", "find: the number")
	text := flags.String("text", "", "find: the text")
	since := flags.String("since", "", "find: RFC 3339 start time")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	bugle := BugleDB{Path: *dbPath}
	encoder := json.NewEncoder(stdout)
	switch command {
	case "send":
		var request DeviceRequest
		if err := json.NewDecoder(stdin).Decode(&request); err != nil {
			_ = encoder.Encode(DeviceResult{Outcome: OutcomeRefused, Error: "invalid request JSON: " + err.Error()})
			return 0
		}
		_ = encoder.Encode((&Device{Bugle: bugle}).Send(request))
		return 0
	case "find":
		start, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			fmt.Fprintf(stderr, "--since must be RFC 3339: %v\n", err)
			return 2
		}
		number, err := NormalizeRecipient(*recipient)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		message, found, err := bugle.FindSent(number, *text, start)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if found {
			_ = encoder.Encode(message)
		}
		return 0
	}
	fmt.Fprintf(stderr, "pdw mutations android-messages-device: unknown command %q\n\n", command)
	fmt.Fprint(stderr, deviceUsage)
	return 2
}
