#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -f ".env" ]]; then
  echo "Error: .env file not found in $SCRIPT_DIR" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "Error: uv is not installed or not on PATH" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source ".env"
set +a

exec uv run hail
