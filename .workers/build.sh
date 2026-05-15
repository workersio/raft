#!/bin/sh
set -eu

go mod download

if [ -f fuzzy/go.mod ]; then
  (cd fuzzy && go mod download)
fi
