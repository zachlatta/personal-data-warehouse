package applemessages

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// DecodedBody is the message text plus where it came from.
type DecodedBody struct {
	Text                 string
	Source               string
	Status               string
	AttributedBodySHA256 string
	Error                string
}

// DecodeMessageBody prefers the attributedBody archive (the modern column;
// text is often NULL) and falls back to the text column, recording which one
// answered and any decode error.
func DecodeMessageBody(text string, attributedBody []byte) DecodedBody {
	attributedSHA := ""
	if len(attributedBody) > 0 {
		sum := sha256.Sum256(attributedBody)
		attributedSHA = hex.EncodeToString(sum[:])
	}
	decoded := ""
	decodeError := ""
	if len(attributedBody) > 0 {
		value, err := decodeAttributedBodySafely(attributedBody)
		if err != nil {
			decodeError = err.Error()
		} else {
			decoded = value
		}
	}
	if decoded != "" {
		return DecodedBody{Text: decoded, Source: "attributedBody", Status: "ok", AttributedBodySHA256: attributedSHA}
	}
	if text != "" {
		status := "ok"
		if decodeError != "" {
			status = "fallback_text"
		}
		return DecodedBody{Text: text, Source: "text", Status: status, AttributedBodySHA256: attributedSHA, Error: decodeError}
	}
	if len(attributedBody) > 0 {
		if decodeError == "" {
			decodeError = "attributedBody did not contain a decodable string"
		}
		return DecodedBody{Text: "", Source: "attributedBody", Status: "error", AttributedBodySHA256: attributedSHA, Error: decodeError}
	}
	return DecodedBody{Status: "empty"}
}

func decodeAttributedBodySafely(data []byte) (text string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("typedstream decode panicked: %v", recovered)
		}
	}()
	return DecodeAttributedBody(data)
}
