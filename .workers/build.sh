#!/bin/sh
set -eu

if ! command -v go >/dev/null 2>&1; then
  echo "go not found; skipping module download during project prepare"
  exit 0
fi

go mod download

if [ -f fuzzy/go.mod ]; then
  (cd fuzzy && go mod download)
fi
