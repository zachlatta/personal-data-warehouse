package slack_test

import (
	"fmt"
	"io"
)

var ioEOF = io.EOF

func toString(v any) string { return fmt.Sprint(v) }
