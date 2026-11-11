//go:build !linux

package scripts

import (
	"fmt"
	"os"
)

func restrictWorker() error                                        { return fmt.Errorf("script workers require Linux seccomp") }
func watchWorker(_ *os.Process, _ <-chan struct{}, _ func(string)) {}
