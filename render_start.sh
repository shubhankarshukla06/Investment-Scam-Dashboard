#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
export CHROME_BIN="$PROJECT_DIR/.render-browser/chrome-linux64/chrome"
export CHROMEDRIVER_PATH="$PROJECT_DIR/.render-browser/chromedriver-linux64/chromedriver"

test -x "$CHROME_BIN"
test -x "$CHROMEDRIVER_PATH"
"$CHROME_BIN" --version
"$CHROMEDRIVER_PATH" --version

exec gunicorn -b "0.0.0.0:${PORT:?PORT is required}" app:app --workers 1 --timeout 600 --graceful-timeout 30
