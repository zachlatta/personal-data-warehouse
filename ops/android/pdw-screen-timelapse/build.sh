#!/bin/bash
# Builds the tiny Android DEX helper embedded in the pdw binary
# (app/internal/uploaders/screentimelapse/webp_android.jar) for on-phone PNG ->
# WebP screenshot compression via /system/bin/app_process64.
#
# usage:
#   ops/android/pdw-screen-timelapse/build.sh [out.jar]
#   ops/android/pdw-screen-timelapse/build.sh --check [out.jar]
#
# Needs JDK 17+ and an Android SDK (ANDROID_HOME, ~/Android/Sdk, or the Homebrew
# android-commandlinetools root) with platforms;android-36 and
# build-tools;36.0.0. Packs classes.dex with a fixed zip timestamp so every
# rebuild from the same source produces identical bytes.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$here/../../.." && pwd)"
default_out="$repo_root/app/internal/uploaders/screentimelapse/webp_android.jar"

check_only=0
if [ "${1:-}" = "--check" ]; then
  check_only=1
  shift
fi
out="${1:-$default_out}"

sdk="${ANDROID_HOME:-}"
if [ -z "$sdk" ]; then
  for candidate in "$HOME/Android/Sdk" "/opt/homebrew/share/android-commandlinetools"; do
    if [ -d "$candidate/platforms/android-36" ]; then
      sdk="$candidate"
      break
    fi
  done
fi
platform="$sdk/platforms/android-36/android.jar"
d8="$sdk/build-tools/36.0.0/d8"
[ -f "$platform" ] || { echo "missing $platform (set ANDROID_HOME)" >&2; exit 1; }
[ -x "$d8" ] || { echo "missing $d8 (set ANDROID_HOME)" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/classes" "$work/dex"

javac --release 11 -classpath "$platform" -d "$work/classes" \
  "$here/src/PdwPngCompress.java"
"$d8" --release --min-api 30 --lib "$platform" --output "$work/dex" \
  "$work/classes/com/zachlatta/pdw/screentimelapse/PdwPngCompress.class"

target="$out"
if [ "$check_only" -eq 1 ]; then
  target="$work/expected.jar"
fi

python3 - "$work/dex/classes.dex" "$target" <<'PY'
import sys
import zipfile

dex_path, jar_path = sys.argv[1], sys.argv[2]
info = zipfile.ZipInfo("classes.dex", date_time=(1980, 1, 1, 0, 0, 0))
info.compress_type = zipfile.ZIP_DEFLATED
info.external_attr = 0o644 << 16
with open(dex_path, "rb") as f:
    dex_bytes = f.read()
with zipfile.ZipFile(jar_path, "w") as zf:
    zf.writestr(info, dex_bytes, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
PY

if [ "$check_only" -eq 1 ]; then
  cmp -s "$target" "$out" || {
    echo "$out is out of date with $here/src/PdwPngCompress.java; run ops/android/pdw-screen-timelapse/build.sh" >&2
    exit 1
  }
  echo "verified $out matches source"
else
  echo "built $out ($(wc -c < "$out" | tr -d ' ') bytes)"
fi
