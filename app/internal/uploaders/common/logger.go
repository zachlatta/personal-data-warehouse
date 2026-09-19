package common

import (
	"fmt"
	"io"
	"sync"
)

// Logger is what the runners log through: the CLI prints both levels to
// stdout (the run log), tests record them.
type Logger interface {
	Infof(format string, args ...any)
	Warningf(format string, args ...any)
}

// WriterLogger prints every line to an io.Writer, flushing per line so a
// launchd run log is readable while the run is in progress.
type WriterLogger struct {
	mu  sync.Mutex
	Out io.Writer
}

// NewWriterLogger returns a Logger writing to out.
func NewWriterLogger(out io.Writer) *WriterLogger {
	return &WriterLogger{Out: out}
}

func (l *WriterLogger) Infof(format string, args ...any)    { l.write(format, args...) }
func (l *WriterLogger) Warningf(format string, args ...any) { l.write(format, args...) }

func (l *WriterLogger) write(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(args) == 0 {
		fmt.Fprintln(l.Out, format)
		return
	}
	fmt.Fprintf(l.Out, format+"\n", args...)
}

// RecordingLogger keeps every line for assertions.
type RecordingLogger struct {
	mu       sync.Mutex
	Infos    []string
	Warnings []string
}

func (l *RecordingLogger) Infof(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Infos = append(l.Infos, fmt.Sprintf(format, args...))
}

func (l *RecordingLogger) Warningf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Warnings = append(l.Warnings, fmt.Sprintf(format, args...))
}

// Lines returns every recorded line in order of level, for substring checks.
func (l *RecordingLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := append([]string(nil), l.Infos...)
	return append(out, l.Warnings...)
}
