#!/bin/bash
# Builds the PDW usage exporter APK without Gradle: javac against the
# platform android.jar, d8, aapt2 link, zipalign, apksigner.
#
# usage: ops/android/pdw-usage-exporter/build.sh <out.apk>
#
# Needs JDK 17 and an Android SDK (ANDROID_HOME, default the Homebrew
# android-commandlinetools root) with platforms;android-36 and
# build-tools;36.0.0. Signs with $PDW_NOTIFICATION_LISTENER_KEYSTORE
# (default ~/.local/share/pdw-notification-listener/signing.p12), creating
# it on first use: Android installs an update only over an APK signed with
# the same key, so keep that file. It shares the notification listener's key.
set -euo pipefail

out=${1:?usage: build.sh <out.apk>}
here="$(cd "$(dirname "$0")" && pwd)"
sdk=${ANDROID_HOME:-/opt/homebrew/share/android-commandlinetools}
platform=$sdk/platforms/android-36/android.jar
tools=$sdk/build-tools/36.0.0
keystore=${PDW_NOTIFICATION_LISTENER_KEYSTORE:-$HOME/.local/share/pdw-notification-listener/signing.p12}
[ -f "$platform" ] || { echo "missing $platform" >&2; exit 1; }
[ -x "$tools/aapt2" ] || { echo "missing $tools/aapt2" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/classes" "$work/dex"

javac --release 11 -classpath "$platform" -d "$work/classes" \
  $(find "$here/src" -name '*.java')
"$tools/d8" --release --min-api 29 --lib "$platform" --output "$work/dex" \
  $(find "$work/classes" -name '*.class')
"$tools/aapt2" link --manifest "$here/AndroidManifest.xml" -I "$platform" -o "$work/unsigned.apk"
(cd "$work/dex" && zip -q -j "$work/unsigned.apk" classes.dex)
"$tools/zipalign" -f -p 4 "$work/unsigned.apk" "$work/aligned.apk"

if [ ! -f "$keystore" ]; then
  mkdir -p "$(dirname "$keystore")"
  chmod 700 "$(dirname "$keystore")"
  keytool -genkeypair -keystore "$keystore" -storetype PKCS12 -storepass pdwnotifications \
    -alias pdw -keyalg RSA -keysize 3072 -validity 36500 \
    -dname "CN=PDW notification listener" >/dev/null 2>&1
  chmod 600 "$keystore"
  echo "created signing key $keystore" >&2
fi
# The keystore password is not a secret: the file's 0600 mode is the guard.
"$tools/apksigner" sign --ks "$keystore" --ks-pass pass:pdwnotifications --ks-key-alias pdw \
  --out "$out" "$work/aligned.apk"
"$tools/apksigner" verify "$out"
echo "built $out"
