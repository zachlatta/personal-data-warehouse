package scanner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

// Brother's "Scan to PC" button. A computer puts itself on the panel by
// writing a registration string to one SNMP object on the scanner (what
// Brother's brscan-skey does): the panel then lists its USER name under
// Scan > to PC > Image/OCR/E-mail/File. Pressing Start sends a UDP datagram
// naming that USER to the registered HOST:port, and the scanner shows
// "Connecting to PC" until the computer connects to scan (native.go).
//
// A registration lives DURATION seconds, so it is written again well before
// it lapses (and after the scanner restarts, which forgets it). The scanner
// must allow SNMP v1/v2c writes (Network > Protocol > SNMP: "SNMP v1/v2c
// read-write access"), with the write community `internal`; newer firmware
// ships read-only, and the setting took a power cycle to apply on an
// MFC-L2710DW.

// snmpPort is replaced in tests.
var snmpPort = "161"

const (
	registrationOID = "1.3.6.1.4.1.2435.2.3.9.2.11.1.1.0"
	// RegistrationLifetime is the DURATION each registration asks for.
	RegistrationLifetime = 360 * time.Second
)

// buttonFunctions are the panel's Scan to PC functions and the APPNUM
// brscan-skey registers each with. Every PDW name is registered under all
// four, so any of them reaches the service.
var buttonFunctions = []struct {
	name   string
	appnum int
}{{"IMAGE", 1}, {"OCR", 3}, {"EMAIL", 2}, {"FILE", 5}}

// RegistrationValue is the string written for one panel name and function.
func RegistrationValue(user, function, hostPort string, appnum int) string {
	return fmt.Sprintf(`TYPE=BR;BUTTON=SCAN;USER="%s";FUNC=%s;HOST=%s;APPNUM=%d;DURATION=%d;`,
		user, function, hostPort, appnum, int(RegistrationLifetime/time.Second))
}

// Register puts every name on the panel, pointing at hostPort (this
// computer's address and UDP port as the scanner reaches it).
func Register(ctx context.Context, scannerHost, community, hostPort string, names []string) error {
	conn, err := net.Dial("udp", net.JoinHostPort(scannerHost, snmpPort))
	if err != nil {
		return err
	}
	defer conn.Close()
	requestID := int(time.Now().UnixNano() & 0x3fffffff)
	for _, name := range names {
		for _, f := range buttonFunctions {
			requestID++
			value := RegistrationValue(name, f.name, hostPort, f.appnum)
			if err := snmpSet(ctx, conn, community, requestID, registrationOID, value); err != nil {
				return fmt.Errorf("register %q (%s): %w", name, f.name, err)
			}
		}
	}
	return nil
}

// snmpErrorNames are the SNMP error-status values a refused write returns.
var snmpErrorNames = map[int]string{
	1: "tooBig", 2: "noSuchName (SNMP v1/v2c writes are off on the scanner)", 3: "badValue", 4: "readOnly",
	5: "genErr", 6: "noAccess (SNMP v1/v2c writes are off on the scanner)", 17: "notWritable",
}

func snmpSet(ctx context.Context, conn net.Conn, community string, requestID int, oid, value string) error {
	packet := berSeq(berInt(0), berOctets([]byte(community)),
		berTLV(0xa3, append(append(append(berInt(requestID), berInt(0)...), berInt(0)...),
			berSeq(berSeq(berOID(oid), berOctets([]byte(value))))...)))
	deadline := time.Now().Add(3 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	if _, err := conn.Write(packet); err != nil {
		return err
	}
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		// A community the scanner does not know gets no answer at all.
		return fmt.Errorf("no SNMP answer (wrong community, or SNMP off): %w", err)
	}
	status, err := parseSetResponse(buf[:n], requestID)
	if err != nil {
		return err
	}
	if status != 0 {
		name := snmpErrorNames[status]
		if name == "" {
			name = fmt.Sprintf("error %d", status)
		}
		return fmt.Errorf("scanner refused the SNMP write: %s", name)
	}
	return nil
}

// parseSetResponse reads the error-status of a GetResponse.
func parseSetResponse(packet []byte, requestID int) (int, error) {
	r := berReader{data: packet}
	msg, err := r.next(0x30)
	if err != nil {
		return 0, err
	}
	m := berReader{data: msg}
	if _, err := m.next(0x02); err != nil {
		return 0, err
	}
	if _, err := m.next(0x04); err != nil {
		return 0, err
	}
	pdu, err := m.next(0xa2)
	if err != nil {
		return 0, err
	}
	p := berReader{data: pdu}
	id, err := p.nextInt()
	if err != nil {
		return 0, err
	}
	if id != requestID {
		return 0, fmt.Errorf("SNMP answer to another request")
	}
	return p.nextInt()
}

// ButtonEvent is one press of Start on a registered name.
type ButtonEvent struct {
	User     string
	Function string
	RegID    string
	Seq      string
}

var eventFieldRE = regexp.MustCompile(`([A-Z0-9]+)=("[^"]*"|[^;]*);`)

// ParseButtonEvent reads the scanner's datagram ("\x02\x00{0TYPE=BR;BUTTON=SCAN;
// USER="PDW-Color";FUNC=FILE;...;REGID=46310;SEQ=1;"). ok is false for
// anything that is not a scan button press.
func ParseButtonEvent(packet []byte) (ButtonEvent, bool) {
	text := string(packet)
	start := strings.Index(text, "TYPE=")
	if start < 0 {
		return ButtonEvent{}, false
	}
	fields := map[string]string{}
	for _, m := range eventFieldRE.FindAllStringSubmatch(text[start:], -1) {
		fields[m[1]] = strings.Trim(m[2], `"`)
	}
	if fields["TYPE"] != "BR" || fields["BUTTON"] != "SCAN" || fields["USER"] == "" {
		return ButtonEvent{}, false
	}
	return ButtonEvent{User: fields["USER"], Function: fields["FUNC"], RegID: fields["REGID"], Seq: fields["SEQ"]}, true
}

// --- minimal BER, enough for one SNMP SetRequest ----------------------------

func berTLV(tag byte, value []byte) []byte {
	n := len(value)
	var length []byte
	switch {
	case n < 0x80:
		length = []byte{byte(n)}
	case n < 0x100:
		length = []byte{0x81, byte(n)}
	default:
		length = []byte{0x82, byte(n >> 8), byte(n)}
	}
	return append(append([]byte{tag}, length...), value...)
}

func berSeq(parts ...[]byte) []byte {
	var body []byte
	for _, p := range parts {
		body = append(body, p...)
	}
	return berTLV(0x30, body)
}

func berInt(v int) []byte {
	b := []byte{byte(v)}
	for x := v >> 8; x > 0; x >>= 8 {
		b = append([]byte{byte(x)}, b...)
	}
	if b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}
	return berTLV(0x02, b)
}

func berOctets(v []byte) []byte { return berTLV(0x04, v) }

func berOID(oid string) []byte {
	var parts []int
	for _, s := range strings.Split(oid, ".") {
		var n int
		fmt.Sscanf(s, "%d", &n)
		parts = append(parts, n)
	}
	out := []byte{byte(40*parts[0] + parts[1])}
	for _, n := range parts[2:] {
		enc := []byte{byte(n & 0x7f)}
		for n >>= 7; n > 0; n >>= 7 {
			enc = append([]byte{byte(0x80 | n&0x7f)}, enc...)
		}
		out = append(out, enc...)
	}
	return berTLV(0x06, out)
}

type berReader struct {
	data []byte
	pos  int
}

var errBER = errors.New("malformed SNMP answer")

func (r *berReader) next(tag byte) ([]byte, error) {
	if r.pos+2 > len(r.data) || r.data[r.pos] != tag {
		return nil, errBER
	}
	r.pos++
	n := int(r.data[r.pos])
	r.pos++
	if n&0x80 != 0 {
		count := n & 0x7f
		if count > 2 || r.pos+count > len(r.data) {
			return nil, errBER
		}
		n = 0
		for i := 0; i < count; i++ {
			n = n<<8 | int(r.data[r.pos])
			r.pos++
		}
	}
	if r.pos+n > len(r.data) {
		return nil, errBER
	}
	v := r.data[r.pos : r.pos+n]
	r.pos += n
	return v, nil
}

func (r *berReader) nextInt() (int, error) {
	v, err := r.next(0x02)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, b := range v {
		n = n<<8 | int(b)
	}
	return n, nil
}
