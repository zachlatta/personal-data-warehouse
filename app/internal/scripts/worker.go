package scripts

import (
	"bufio"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed engine.wasm
var wasm []byte

//go:embed bootstrap.js
var bootstrap string

//go:embed engine.sha256
var engineHash string

func init() {
	if len(os.Args) == 2 && os.Args[1] == "--pdw-script-worker" {
		code := 0
		if err := worker(); err != nil {
			code = 1
		}
		os.Exit(code)
	}
}

func worker() (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("worker panic")
		}
	}()
	if err = restrictWorker(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 4*MaxOperationBytes+MaxOutput)
	if !scanner.Scan() {
		return fmt.Errorf("missing input")
	}
	var in Input
	if err = json.Unmarshal(scanner.Bytes(), &in); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(in.TimeoutMS)*time.Millisecond)
	defer cancel()
	hash := sha256.Sum256(wasm)
	if hex.EncodeToString(hash[:]) != strings.TrimSpace(engineHash) {
		return fmt.Errorf("engine checksum mismatch")
	}
	// Interpreter avoids JIT mappings and keeps the worker's memory predictable.
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfigInterpreter().WithMemoryLimitPages(512).WithCloseOnContextDone(true))
	defer rt.Close(ctx)
	if _, err = wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return err
	}
	// No filesystem mounts, environment, clocks, sleep, network, stdin or stdout
	// are wired into WASI. Only the Go driver holds the two protocol pipes.
	mod, err := rt.InstantiateWithConfig(ctx, wasm, wazero.NewModuleConfig().WithStartFunctions("_initialize"))
	if err != nil {
		return err
	}
	call := func(name string, args ...uint64) (uint64, error) {
		v, e := mod.ExportedFunction(name).Call(ctx, args...)
		if e != nil {
			return 0, e
		}
		if len(v) == 0 {
			return 0, nil
		}
		return v[0], nil
	}
	input := func(name string, b []byte) error {
		ptr, e := call("malloc", uint64(len(b)+1))
		if e != nil {
			return e
		}
		if !mod.Memory().Write(uint32(ptr), append(b, 0)) {
			return fmt.Errorf("invalid memory")
		}
		defer call("free", ptr)
		result, e := call(name, ptr, uint64(len(b)))
		if e != nil {
			return e
		}
		if int32(result) < 0 {
			return fmt.Errorf("engine error")
		}
		return nil
	}
	code, _ := json.Marshal(in.Code)
	source := strings.TrimSuffix(bootstrap, "})(__CODE__, __ARGS__)\n") + "})(" + string(code) + "," + string(in.Args) + ")"
	if err = input("initialize", []byte(source)); err != nil {
		return err
	}
	for {
		length, e := call("poll")
		if e != nil || int32(length) < 0 {
			return fmt.Errorf("engine failed: %v", e)
		}
		if length > MaxOperationBytes+MaxOutput {
			return fmt.Errorf("output too large")
		}
		ptr, e := call("result")
		if e != nil {
			return e
		}
		b, ok := readMemory(mod, uint32(ptr), uint32(length))
		if !ok {
			return fmt.Errorf("invalid result")
		}
		var p packet
		if err = json.Unmarshal(b, &p); err != nil {
			return err
		}
		if p.Done || len(p.Calls) > 0 {
			if _, err = os.Stdout.Write(append(b, '\n')); err != nil {
				return err
			}
			if p.Done {
				return nil
			}
			if !scanner.Scan() {
				return fmt.Errorf("broker closed")
			}
			if err = input("deliver", scanner.Bytes()); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
func readMemory(mod api.Module, ptr, length uint32) ([]byte, bool) {
	return mod.Memory().Read(ptr, length)
}
