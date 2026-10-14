#!/usr/bin/env bash
set -Eeuo pipefail

# Keep the browser inside the native-Python deploy artifact instead of relying
# on OS packages installed only in Render's build environment.
PROJECT_DIR="$(pwd -P)"
BROWSER_ROOT="$PROJECT_DIR/.render-browser"
MANIFEST_URL="https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json"
STAGING_DIR="$(mktemp -d "$PROJECT_DIR/.render-browser.stage.XXXXXX")"
MANIFEST_FILE="$(mktemp)"
PROFILE_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$STAGING_DIR" "$PROFILE_DIR"
    rm -f "$MANIFEST_FILE"
}
trap cleanup EXIT

curl --fail --location --show-error --retry 3 --retry-all-errors \
    --connect-timeout 20 --max-time 120 "$MANIFEST_URL" -o "$MANIFEST_FILE"

mapfile -t CFT_METADATA < <(python - "$MANIFEST_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as manifest_file:
    stable = json.load(manifest_file)["channels"]["Stable"]

downloads = stable["downloads"]

def linux_url(kind):
    for item in downloads[kind]:
        if item.get("platform") == "linux64":
            return item["url"]
    raise RuntimeError(f"Stable Chrome for Testing has no linux64 {kind} download")

print(stable["version"])
print(linux_url("chrome"))
print(linux_url("chromedriver"))
PY
)

if [[ "${#CFT_METADATA[@]}" -ne 3 ]]; then
    echo "Could not read the stable Chrome for Testing manifest." >&2
    exit 1
fi

CFT_VERSION="${CFT_METADATA[0]}"
CHROME_URL="${CFT_METADATA[1]}"
CHROMEDRIVER_URL="${CFT_METADATA[2]}"

echo "Installing Chrome for Testing ${CFT_VERSION} for linux64"
curl --fail --location --show-error --retry 3 --retry-all-errors \
    --connect-timeout 20 --max-time 600 "$CHROME_URL" -o "$STAGING_DIR/chrome.zip"
curl --fail --location --show-error --retry 3 --retry-all-errors \
    --connect-timeout 20 --max-time 300 "$CHROMEDRIVER_URL" -o "$STAGING_DIR/chromedriver.zip"

unzip -q "$STAGING_DIR/chrome.zip" -d "$STAGING_DIR"
unzip -q "$STAGING_DIR/chromedriver.zip" -d "$STAGING_DIR"
rm -f "$STAGING_DIR/chrome.zip" "$STAGING_DIR/chromedriver.zip"

CHROME_BIN="$STAGING_DIR/chrome-linux64/chrome"
CHROMEDRIVER_BIN="$STAGING_DIR/chromedriver-linux64/chromedriver"
test -x "$CHROME_BIN"
test -x "$CHROMEDRIVER_BIN"

for executable in "$CHROME_BIN" "$CHROMEDRIVER_BIN"; do
    if ldd "$executable" | grep -F 'not found' >&2; then
        echo "Missing Linux shared library required by $executable" >&2
        exit 1
    fi
done

CHROME_VERSION="$($CHROME_BIN --version | grep -oE '[0-9]+(\.[0-9]+){3}' | head -n 1)"
CHROMEDRIVER_VERSION="$($CHROMEDRIVER_BIN --version | grep -oE '[0-9]+(\.[0-9]+){3}' | head -n 1)"
if [[ "$CHROME_VERSION" != "$CFT_VERSION" || "$CHROMEDRIVER_VERSION" != "$CFT_VERSION" ]]; then
    echo "Chrome for Testing version mismatch: manifest=$CFT_VERSION chrome=$CHROME_VERSION chromedriver=$CHROMEDRIVER_VERSION" >&2
    exit 1
fi

# Prove the downloaded browser can start headlessly in the native runtime.
if ! timeout 30 "$CHROME_BIN" --headless=new --no-sandbox --disable-dev-shm-usage \
    --disable-gpu --user-data-dir="$PROFILE_DIR" --dump-dom 'data:text/html,<title>browser-check</title>' \
    | grep -q 'browser-check'; then
    echo "Chrome for Testing could not start headlessly in this build environment." >&2
    exit 1
fi

rm -rf "$BROWSER_ROOT"
mv "$STAGING_DIR" "$BROWSER_ROOT"
STAGING_DIR=""

printf '%s\n' "$CFT_VERSION" > "$BROWSER_ROOT/VERSION"
printf '%s\n' "$CHROME_URL" > "$BROWSER_ROOT/CHROME_URL"
printf '%s\n' "$CHROMEDRIVER_URL" > "$BROWSER_ROOT/CHROMEDRIVER_URL"

python -m pip install -r requirements.txt
