"""Rebuild the embedded sandbox engine with a pinned WASI compiler and QuickJS.

Download/extract the two pinned archives, then pass their directories as arguments.
The interpreter deliberately excludes quickjs-libc, module loaders and WASI access.
"""
import pathlib
import hashlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent
# https://bellard.org/quickjs/quickjs-2026-06-04.tar.xz
# SHA256: b376e839b322978313d929fd20663b11ba58b75df5a46c126dd19ea2fa70ad2a
# https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-25/wasi-sdk-25.0-x86_64-linux.tar.gz
# SHA256: 52640dde13599bf127a95499e61d6d640256119456d1af8897ab6725bcf3d89c
quickjs, sdk = map(pathlib.Path, sys.argv[1:])
quickjs, sdk = quickjs.resolve(), sdk.resolve()
subprocess.run([
    str(sdk / 'bin/clang'), '--sysroot=' + str(sdk / 'share/wasi-sysroot'),
    # dtoa.c includes setjmp.h but never uses it; WASI has no setjmp.
    '-O2', '-mexec-model=reactor', '-D_SETJMP_H', '-D_GNU_SOURCE', '-D__EMSCRIPTEN__', '-DCONFIG_VERSION="2026-06-04"', '-I'+str(quickjs),
    '-ffile-prefix-map='+str(quickjs)+'=quickjs',
    '-ffile-prefix-map='+str(ROOT)+'=pdw-scripts',
    '-Wl,--no-entry', '-Wl,--export=initialize', '-Wl,--export=poll',
    '-Wl,--export=deliver', '-Wl,--export=result', '-Wl,--export=malloc',
    '-Wl,--export=free', '-Wl,-z,stack-size=524288', '-Wl,--max-memory=33554432',
    str(ROOT/'engine/engine.c'), *[str(quickjs/p) for p in
        ['quickjs.c','libregexp.c','libunicode.c','cutils.c','dtoa.c']],
    '-lm', '-o', str(ROOT/'engine.wasm'),
], check=True)
actual = hashlib.sha256((ROOT / 'engine.wasm').read_bytes()).hexdigest()
expected = (ROOT / 'engine.sha256').read_text().strip()
if actual != expected:
    raise SystemExit(f'Engine hash changed: {actual}; review the source/compiler change before updating the pin')
print(actual)
