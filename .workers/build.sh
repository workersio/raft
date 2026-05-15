#!/usr/bin/env bash
set -euo pipefail

go mod download

if [ -f fuzzy/go.mod ]; then
  (cd fuzzy && go mod download)
fi
