# Read-only script worker

The application embeds `engine.wasm`; the CLI only sends source to the server.
There is no runtime download, Node dependency, CGO, or second service to deploy.
QuickJS 2026-06-04 is built with WASI SDK 25; wazero is pinned to v1.12.0.
Archive URLs and SHA-256 hashes are pinned in `build-engine.py`. The linked binary
hash is pinned in `engine.sha256` and checked in every worker. The engine source
adapter and upstream license are in `engine/`.

To rebuild, verify the two archives against those hashes, extract them, then run:

```sh
python3 build-engine.py /path/to/quickjs-2026-06-04 /path/to/wasi-sdk-25.0-x86_64-linux
sha256sum engine.wasm
cat engine.sha256
```

`engine.sha256` contains only the expected digest; compare the printed digest
from `sha256sum engine.wasm` with it. An intentional engine update must review
the upstream change and replace the pin. Builds run only this repository's
adapter plus QuickJS core, regexp, unicode, dtoa and utilities. The libc/OS
modules, CLI, module loader and worker support are not linked. The Emscripten
configuration disables thread-dependent atomics. The unused setjmp header in
dtoa is suppressed because WASI does not provide setjmp. Wasm traps bound the
stack. The runtime enables wazero context cancellation and uses the interpreter.

Each worker is the application executable with a private worker argument. It
gets no credentials or inherited environment. A Linux seccomp filter is applied
with TSYNC to every runtime thread and inherited by later threads. Setup fails
closed. Open, socket, execution, namespace, ptrace, process-memory, io_uring,
memfd and related escape interfaces are refused. WASI has no mounts, environment,
network, real clocks, sleep, or protocol descriptors. The JavaScript controller
is held by the native adapter outside the script global object; its messages
are nevertheless validated by the parent broker before dispatch.

Wasm memory is capped at 32 MiB. QuickJS additionally caps its allocation budget.
The whole process has a three-second hard CPU rlimit and a parent watchdog for
two seconds cumulative CPU and 128 MiB RSS. RSS sampling is not a cgroup hard
memory cap; `GOMEMLIMIT` is a Go GC target. This works under distroless nonroot
without delegated cgroups or new namespace privileges. The worker inherits the
deployment's user and container boundary. It is not a general-purpose native
code sandbox. Supported Linux architectures are amd64 and arm64; other hosts
fail closed. `go test -race` covers the parent broker separately.

The parent allows four runs, two per authenticated caller, twelve operations
per run and six globally concurrent inner reads. Hybrid search can need three
database connections per inner read; six leaves headroom in the 25-connection
query pool. Query wrappers bound serialized results in Postgres before the
driver materializes them. Caller cancellation, worker death and watchdog limits
cancel outstanding database contexts. SQL uses the existing validator and role
runner and requires `pdw_query`. Tool dispatch never exposes the full registry.

Run metadata is written synchronously with a separate three-second budget. The
writer ensures the cataloged table itself before its first insert, so deployment
does not depend on Python ensure having run first. A failed telemetry write is
an explicit `telemetry_unavailable` result; callers must not blindly retry it.
No source, arguments, final value, citations or SQL text are stored there. Error
messages are reduced to SQLSTATE and fixed categories. Search priorities are
restricted to known tiers. Normal API/MCP result logging is suppressed for run.

Python ensure creates the timestamp index and `marts_ops.script_runs` daily
summary. It also grants query access through the normal catalog policy. The
view is available after that ensure cycle. Existing collectors expand run
telemetry linked by run id into inner operations, without counting the outer
composition request as a second read. Failed executions also enter C17's shape
report. The benchmark proxy forwards source restrictions to the broker, where
they apply before projection; its grade records inner-read counts in the trace.
As with existing benchmark SQL, text-only projections with no source coordinates
still need the benchmark's contamination markers.

After seven days, inspect daily successful/failed runs, latency, inner operations,
bytes, truncation and failures in the new view, then C3/C17 rates and C16 answer
quality. Compare outer round trips with inner reads; a script can reduce the
former without reducing database work. The pilot has no opt-in client filter.
If removed, remove the tool, CLI command, engine, telemetry and guide together.
