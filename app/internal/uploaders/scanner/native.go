package scanner

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Brother's own network scan protocol, on TCP 54921: what brscan4 speaks,
// and what the panel waits for after a Scan to PC press ("Connecting to PC").
// While a press is pending the scanner will not run a WS-Scan or eSCL job
// (it answers with no image and cancels), so the pages have to come this way.
//
// On connect the scanner sends "+OK 200\r\n" (or "-NG 401\r\n" while busy,
// including for a few seconds after the previous connection). Requests are
// 0x1b, then newline-terminated ASCII fields (the first is the command
// letter), then 0x80:
//
//	I  R=300,300  M=CGRAY               information; the answer is 0x00, a
//	                                    little-endian length, and
//	                                    "xres,yres,?,?,maxX,?,?," + 0x00
//	X  R= M= C=JPEG J= B= N= A= D= G= L=  scan
//
// The scan streams records: a type byte, a little-endian header length, the
// header (whose first two bytes are the page number), and for data records
// (type 0x64, JPEG) a little-endian data length and the data. 0x82 ends a
// page; a lone 0x80 ends the job; 0xc2 means there is nothing to scan. Each
// page's data concatenates to one JPEG. Reverse-engineered notes:
// github.com/jmesmon/brother2 (PROTO), checked on an MFC-L2710DW.

// NativePort is the scanner's native scan port.
const NativePort = "54921"

const (
	recordJPEG    = 0x64
	recordPageEnd = 0x82
	recordJobEnd  = 0x80
	recordNoPaper = 0xc2
)

// NativeClient scans from one Brother scanner.
type NativeClient struct {
	Host string
	// Port is NativePort unless a test says otherwise.
	Port string
	// BusyRetries and BusyDelay bound the wait while the scanner answers
	// "-NG 401" (busy).
	BusyRetries int
	BusyDelay   time.Duration
	// IdleTimeout is how long a read may wait; a long feeder job pauses
	// between pages, never for this long.
	IdleTimeout time.Duration
}

// ErrNoPaper is a scan with nothing to scan.
var ErrNoPaper = errors.New("the scanner had nothing to scan")

// NativeMode is the scan's color mode as the protocol names it.
type NativeMode string

// The modes PDW uses.
const (
	ModeColor NativeMode = "CGRAY"
	ModeGray  NativeMode = "GRAY64"
)

func (c *NativeClient) port() string {
	if c.Port != "" {
		return c.Port
	}
	return NativePort
}

// connect opens a session, waiting out "-NG 401".
func (c *NativeClient) connect(ctx context.Context) (net.Conn, *bufio.Reader, error) {
	retries := c.BusyRetries
	if retries <= 0 {
		retries = 20
	}
	delay := c.BusyDelay
	if delay <= 0 {
		delay = 3 * time.Second
	}
	var dialer net.Dialer
	var lastGreeting string
	for attempt := 0; attempt < retries; attempt++ {
		conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(c.Host, c.port()))
		if err != nil {
			return nil, nil, err
		}
		_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		r := bufio.NewReaderSize(conn, 1<<16)
		greeting, err := r.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("read the scanner's greeting: %w", err)
		}
		lastGreeting = strings.TrimSpace(greeting)
		if strings.HasPrefix(lastGreeting, "+OK") {
			return conn, r, nil
		}
		conn.Close()
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, nil, fmt.Errorf("scanner stayed busy: %q", lastGreeting)
}

func nativeRequest(fields ...string) []byte {
	var b []byte
	b = append(b, 0x1b)
	for _, f := range fields {
		b = append(b, f...)
		b = append(b, '\n')
	}
	return append(b, 0x80)
}

// Scan scans everything loaded (the feeder when it has paper, else the
// glass) and returns one JPEG per page, in page order.
func (c *NativeClient) Scan(ctx context.Context, mode NativeMode, dpi int, heightInches float64) ([][]byte, error) {
	conn, r, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	res := strconv.Itoa(dpi)
	if _, err := conn.Write(nativeRequest("I", "R="+res+","+res, "M="+string(mode))); err != nil {
		return nil, err
	}
	maxX, err := readInfo(r)
	if err != nil {
		return nil, err
	}
	width := maxX + 1
	if letter := int(8.5 * float64(dpi)); width <= 1 || width > letter {
		width = letter
	}
	height := int(heightInches * float64(dpi))
	if _, err := conn.Write(nativeRequest("X", "R="+res+","+res, "M="+string(mode), "C=JPEG", "J=MID",
		"B=50", "N=50", fmt.Sprintf("A=0,0,%d,%d", width, height), "D=SIN", "G=1", "L=128")); err != nil {
		return nil, err
	}
	idle := c.IdleTimeout
	if idle <= 0 {
		idle = 3 * time.Minute
	}
	return readPages(r, func() { _ = conn.SetReadDeadline(time.Now().Add(idle)) })
}

// readInfo reads the I answer and returns its maximum x pixel.
func readInfo(r *bufio.Reader) (int, error) {
	var head [3]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, fmt.Errorf("read the scanner's information: %w", err)
	}
	payload := make([]byte, binary.LittleEndian.Uint16(head[1:]))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, fmt.Errorf("read the scanner's information: %w", err)
	}
	fields := strings.Split(strings.TrimRight(string(payload), "\x00,"), ",")
	if len(fields) < 5 {
		return 0, fmt.Errorf("unexpected scanner information %q", payload)
	}
	return strconv.Atoi(strings.TrimSpace(fields[4]))
}

// readPages reads scan records until the job ends.
func readPages(r *bufio.Reader, extend func()) ([][]byte, error) {
	pages := map[int][]byte{}
	for {
		extend()
		kind, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("scan stream ended early: %w", err)
		}
		switch kind {
		case recordJobEnd:
			if len(pages) == 0 {
				return nil, ErrNoPaper
			}
			numbers := make([]int, 0, len(pages))
			for n := range pages {
				numbers = append(numbers, n)
			}
			sort.Ints(numbers)
			out := make([][]byte, len(numbers))
			for i, n := range numbers {
				out[i] = pages[n]
			}
			return out, nil
		case recordNoPaper:
			return nil, ErrNoPaper
		}
		var lenBuf [2]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil, err
		}
		header := make([]byte, binary.LittleEndian.Uint16(lenBuf[:]))
		if _, err := io.ReadFull(r, header); err != nil {
			return nil, err
		}
		page := 0
		if len(header) >= 2 {
			page = int(binary.LittleEndian.Uint16(header))
		}
		switch {
		case kind == recordPageEnd:
			continue
		case kind == recordJPEG:
			if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
				return nil, err
			}
			data := make([]byte, binary.LittleEndian.Uint16(lenBuf[:]))
			if _, err := io.ReadFull(r, data); err != nil {
				return nil, err
			}
			pages[page] = append(pages[page], data...)
		default:
			return nil, fmt.Errorf("unexpected scan record 0x%02x", kind)
		}
	}
}
