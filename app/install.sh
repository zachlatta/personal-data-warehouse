#!/usr/bin/env sh
# Install the latest pdw release from
# github.com/zachlatta/personal-data-warehouse.
#
# The command installs as `pdw`. Release artifacts retain the historical
# `pdw-cli` name so binaries installed before the rename can still self-update.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/zachlatta/personal-data-warehouse/main/app/install.sh | sh
#
# Environment overrides (legacy PDW_CLI_* names are still honored):
#   PDW_REPO         GitHub repo (default: zachlatta/personal-data-warehouse)
#   PDW_VERSION      Release tag to install (default: latest)
#   PDW_INSTALL_DIR  Install directory (default: /usr/local/bin if writable,
#                    else $HOME/.local/bin)

set -eu

# Name of the installed command, and the file packaged inside the release
# tarball. They differ deliberately: the artifact keeps the pre-rename
# `pdw-cli` name so older installs keep self-updating, while we install it
# on disk as `pdw`.
INSTALL_NAME="pdw"
ASSET_PREFIX="pdw-cli"

REPO="${PDW_REPO:-${PDW_CLI_REPO:-zachlatta/personal-data-warehouse}}"
VERSION="${PDW_VERSION:-${PDW_CLI_VERSION:-latest}}"

log() { printf '==> %s\n' "$*"; }
err() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || err "missing required command: $1"
}

need curl
need tar
need uname

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  linux|darwin) ;;
  *) err "unsupported OS: $os (only linux and darwin are released)" ;;
esac

arch_raw=$(uname -m)
case "$arch_raw" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) err "unsupported architecture: $arch_raw" ;;
esac

api="https://api.github.com/repos/${REPO}/releases"
if [ "$VERSION" = "latest" ]; then
  release_url="${api}/latest"
else
  release_url="${api}/tags/${VERSION}"
fi

log "Resolving release ($VERSION) from $REPO"
release_json=$(curl -fsSL "$release_url") \
  || err "could not fetch release metadata from $release_url"

resolved_tag=$(printf '%s' "$release_json" \
  | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n1)
[ -n "$resolved_tag" ] || err "could not parse tag_name from release metadata"

# Tags look like pdw-cli/v0.0.42-sha.abcdef0; the asset file embeds only the
# version portion after the slash.
asset_version="${resolved_tag#${ASSET_PREFIX}/}"
asset="${ASSET_PREFIX}_${asset_version}_${os}_${arch}.tar.gz"

base="https://github.com/${REPO}/releases/download/${resolved_tag}"
asset_url="${base}/${asset}"
sums_url="${base}/SHA256SUMS"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

log "Downloading $asset"
curl -fSL --progress-bar "$asset_url" -o "$tmp/$asset" \
  || err "failed to download $asset_url"

log "Verifying SHA256"
curl -fsSL "$sums_url" -o "$tmp/SHA256SUMS" \
  || err "failed to download SHA256SUMS"

expected=$(grep " $asset\$" "$tmp/SHA256SUMS" | awk '{print $1}')
[ -n "$expected" ] || err "no SHA256 entry for $asset in SHA256SUMS"

if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$asset" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
  actual=$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')
else
  err "need sha256sum or shasum to verify the download"
fi

[ "$expected" = "$actual" ] \
  || err "checksum mismatch: expected $expected, got $actual"

log "Extracting"
tar -xzf "$tmp/$asset" -C "$tmp"
[ -f "$tmp/$ASSET_PREFIX" ] || err "extracted archive missing $ASSET_PREFIX binary"
chmod +x "$tmp/$ASSET_PREFIX"

# Pick install dir.
if [ -n "${PDW_INSTALL_DIR:-${PDW_CLI_INSTALL_DIR:-}}" ]; then
  install_dir="${PDW_INSTALL_DIR:-$PDW_CLI_INSTALL_DIR}"
elif [ -w /usr/local/bin ] 2>/dev/null; then
  install_dir="/usr/local/bin"
else
  install_dir="$HOME/.local/bin"
fi

mkdir -p "$install_dir"
# The tarball member is named $ASSET_PREFIX (pdw-cli); install it as `pdw`.
mv "$tmp/$ASSET_PREFIX" "$install_dir/$INSTALL_NAME"

log "Installed $install_dir/$INSTALL_NAME ($asset_version)"

case ":$PATH:" in
  *":$install_dir:"*)
    on_path=1
    ;;
  *)
    on_path=0
    ;;
esac

# If the install dir isn't on PATH, append an export line to the user's shell
# rc files. Marker comment keeps re-runs idempotent. Keep the historical
# "pdw-cli installer" wording so machines that ran the pre-rename installer
# don't get a second, duplicate PATH block appended.
marker="# added by pdw-cli installer"
posix_line="export PATH=\"$install_dir:\$PATH\""
fish_line="set -gx PATH $install_dir \$PATH"

append_path() {
  rc="$1"
  line="$2"
  rc_dir=$(dirname "$rc")
  mkdir -p "$rc_dir"
  [ -f "$rc" ] || : > "$rc"
  if ! grep -Fq "$marker" "$rc" 2>/dev/null; then
    {
      printf '\n%s\n' "$marker"
      printf '%s\n' "$line"
    } >> "$rc"
    log "Updated $rc to put $install_dir on PATH"
    updated_rcs="${updated_rcs:+$updated_rcs }$rc"
  fi
}

updated_rcs=""
shell_name=$(basename "${SHELL:-}")
if [ "$on_path" -eq 0 ]; then
  case "$shell_name" in
    zsh)
      append_path "$HOME/.zshrc" "$posix_line"
      ;;
    bash)
      # Linux logins typically source ~/.bashrc; macOS logins source
      # ~/.bash_profile, which usually sources ~/.bashrc. Cover both.
      append_path "$HOME/.bashrc" "$posix_line"
      if [ "$os" = "darwin" ]; then
        append_path "$HOME/.bash_profile" "$posix_line"
      fi
      ;;
    fish)
      append_path "$HOME/.config/fish/config.fish" "$fish_line"
      ;;
    *)
      # Unknown shell: fall back to ~/.profile, which most POSIX shells read.
      append_path "$HOME/.profile" "$posix_line"
      ;;
  esac
  # Make the new PATH visible to the rest of this script (mainly cosmetic).
  PATH="$install_dir:$PATH"
  export PATH
fi

printf '\n'
if [ -n "$updated_rcs" ]; then
  printf 'Open a new terminal, or run this in your current one:\n'
  case "$shell_name" in
    fish) printf '  set -gx PATH %s $PATH\n' "$install_dir" ;;
    *)    printf '  export PATH="%s:$PATH"\n' "$install_dir" ;;
  esac
  printf '\n'
fi

printf 'Next steps:\n'
printf '  pdw login          # store the API URL and token (interactive)\n'
printf '  pdw list           # confirm the connection works\n'
printf '  pdw update         # later: self-update to the latest release\n'
