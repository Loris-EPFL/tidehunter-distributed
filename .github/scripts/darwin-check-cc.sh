#!/usr/bin/env bash
# Check-only stand-in for C build scripts on Linux targeting x86_64-apple-darwin.
# Produces empty objects for Rust type checking; nothing is linked. Do not use
# for cargo build or cargo test. cc-rs may pass either -o out or -Foout.
out=""
prev=""
for arg in "$@"; do
  [ "$prev" = "-o" ] && out="$arg"
  case "$arg" in -Fo*) out="${arg#-Fo}" ;; esac
  prev="$arg"
done
if [ -n "$out" ]; then
  : > "$out"
fi
