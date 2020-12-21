#!/usr/bin/env bash
# Ensure runner is available
EXE_FILE_NAME=$(which $1)
if [[ -z "$EXE_FILE_NAME" ]]; then
  echo "$1 not available. It is not specified explicitly and not found in \$PATH"
  exit 1
else
  "$@"
  exit 0
fi
