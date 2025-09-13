#!/usr/bin/env bash
set -euo pipefail

# mount_wormfs.sh
# Convenience script to mount the test WormFS binary.
# Defaults:
#   Mount point: /tmp/mnt/wormfs
#   Target (backing) dir: /tmp/wormfs
#
# Usage:
#   ./scripts/mount_wormfs.sh                 # uses defaults
#   ./scripts/mount_wormfs.sh /mnt/wormfs /path/to/backing --debug
#   sudo ./scripts/mount_wormfs.sh            # will work when invoked as root
#
# Notes:
# - The script prefers the repository-built binary at target/release/wormfs.
# - If not found there, it will try to locate "wormfs" on PATH.
# - The script ensures the mount point and target backing directory exist.
# - The actual mount command is executed with sudo if the script is run as non-root.

DEFAULT_MOUNT="/tmp/mnt/wormfs"
DEFAULT_TARGET="/tmp/wormfs"

MOUNT_POINT="${1:-$DEFAULT_MOUNT}"
TARGET="${2:-$DEFAULT_TARGET}"
# Any further args after the first two are passed to the wormfs binary (e.g. --debug)
EXTRA_ARGS="${@:3}"

# Enable debug logging by default unless the user already provided -d or --debug.
# This prepends --debug to the args when not present so debug logging is active by default.
if [[ " $EXTRA_ARGS " != *" --debug "* && " $EXTRA_ARGS " != *" -d "* ]]; then
  EXTRA_ARGS="--debug ${EXTRA_ARGS}"
fi

# Compute repository root relative to this script (works when invoked from repo)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"

# Prefer local release build
LOCAL_BIN="$REPO_ROOT/target/release/wormfs"
BIN=""
if [ -x "$LOCAL_BIN" ]; then
  BIN="$LOCAL_BIN"
else
  # fallback to PATH
  if command -v wormfs >/dev/null 2>&1; then
    BIN="$(command -v wormfs)"
  fi
fi

if [ -z "$BIN" ] || [ ! -x "$BIN" ]; then
  cat >&2 <<EOF
ERROR: wormfs binary not found.

Expected one of:
  - $LOCAL_BIN (build with: cargo build --release)
  - wormfs on your PATH

Build with:
  cargo build --release

EOF
  exit 1
fi

# Use sudo if not root
if [ "$(id -u)" -ne 0 ]; then
  SUDO="sudo"
else
  SUDO=""
fi

echo "Using wormfs binary: $BIN"
echo "Mount point: $MOUNT_POINT"
echo "Target/backing dir: $TARGET"
if [ -n "$EXTRA_ARGS" ]; then
  echo "Extra args: $EXTRA_ARGS"
fi

echo "Ensuring mount point exists (may require sudo)..."
$SUDO mkdir -p "$MOUNT_POINT"

echo "Ensuring target/backing directory exists (may require sudo)..."
$SUDO mkdir -p "$TARGET"

echo "Mounting WormFS (this will run the wormfs FUSE binary)."
# exec so signals are forwarded to the binary
exec $SUDO "$BIN" "$MOUNT_POINT" --target "$TARGET" $EXTRA_ARGS
