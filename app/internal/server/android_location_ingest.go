package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// stampLocationReceipt replaces any client-supplied receipt time only after
// signature verification. Both compressed and expanded bodies are bounded.
func stampLocationReceipt(body []byte, received time.Time, maxBytes int64) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	raw, err := io.ReadAll(io.LimitReader(zr, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("expanded location batch too large")
	}
	var out bytes.Buffer
	zw := gzip.NewWriter(&out)
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 1024), int(maxBytes))
	count := 0
	for scanner.Scan() {
		var row map[string]any
		if err = json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, err
		}
		if row["source"] != "android_location" || row["record_type"] != "location" {
			return nil, fmt.Errorf("invalid location envelope")
		}
		row["received_at"] = received.UTC().Format(time.RFC3339Nano)
		if err = json.NewEncoder(zw).Encode(row); err != nil {
			return nil, err
		}
		count++
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, fmt.Errorf("empty location batch")
	}
	if err = zw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
