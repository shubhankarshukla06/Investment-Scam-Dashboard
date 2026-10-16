"""Install matching Chrome for Testing binaries in Render's Python runtime."""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path


MANIFEST_URL = (
    "https://googlechromelabs.github.io/chrome-for-testing/"
    "last-known-good-versions-with-downloads.json"
)
INSTALL_DIR = Path("/opt/render/project/.render/chrome")
CHROME_PATH = INSTALL_DIR / "chrome-linux64" / "chrome"
DRIVER_PATH = INSTALL_DIR / "chromedriver-linux64" / "chromedriver"
CHROME_ZIP = INSTALL_DIR / "chrome-linux64.zip"
DRIVER_ZIP = INSTALL_DIR / "chromedriver-linux64.zip"


def download(url, destination):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ScamIntelligenceDashboard/ChromeInstaller"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        with open(destination, "wb") as output:
            shutil.copyfileobj(response, output)


def extract_archive(archive_path):
    install_root = INSTALL_DIR.resolve()
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            target = (INSTALL_DIR / member.filename).resolve()
            if os.path.commonpath((str(install_root), str(target))) != str(install_root):
                raise RuntimeError(f"Unsafe path in Chrome archive: {member.filename}")
        archive.extractall(INSTALL_DIR)


def stable_download_url(manifest, component):
    downloads = manifest["channels"]["Stable"]["downloads"][component]
    artifact = next(item for item in downloads if item["platform"] == "linux64")
    return artifact["url"]


def binary_version(binary_path):
    result = subprocess.run(
        [str(binary_path), "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    output = (result.stdout + " " + result.stderr).strip()
    if result.returncode != 0:
        return None
    match = re.search(r"\b(\d+\.\d+\.\d+\.\d+)\b", output)
    return match.group(1) if match else None


def chrome_headless_smoke_test():
    with tempfile.TemporaryDirectory(prefix="chrome-smoke-") as profile_dir:
        command = [
            str(CHROME_PATH),
            "--headless=new",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--remote-debugging-port=0",
            f"--user-data-dir={profile_dir}",
            "--dump-dom",
            "about:blank",
        ]
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=45,
        )
    if result.returncode != 0:
        diagnostic = (result.stderr or result.stdout or "No browser output").strip()
        raise RuntimeError(
            "Chrome headless smoke test failed "
            f"(exit {result.returncode}): {diagnostic[-4000:]}"
        )
    print("Chrome headless smoke test: passed", flush=True)


def main():
    if not sys.platform.startswith("linux"):
        print(f"Skipping Chrome for Testing installation on {sys.platform}.")
        return

    INSTALL_DIR.mkdir(parents=True, exist_ok=True)
    try:
        request = urllib.request.Request(
            MANIFEST_URL,
            headers={"User-Agent": "ScamIntelligenceDashboard/ChromeInstaller"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            manifest = json.load(response)

        stable_version = manifest["channels"]["Stable"]["version"]
        chrome_url = stable_download_url(manifest, "chrome")
        driver_url = stable_download_url(manifest, "chromedriver")
        cached_chrome_version = binary_version(CHROME_PATH) if CHROME_PATH.is_file() else None
        cached_driver_version = binary_version(DRIVER_PATH) if DRIVER_PATH.is_file() else None
        cached_pair_is_current = (
            cached_chrome_version == stable_version
            and cached_driver_version == stable_version
        )

        if not cached_pair_is_current:
            if CHROME_PATH.exists() or DRIVER_PATH.exists():
                print(
                    "Cached Chrome pair is missing or does not match the current "
                    f"Stable version {stable_version} "
                    f"(Chrome={cached_chrome_version}, Driver={cached_driver_version}); refreshing.",
                    flush=True,
                )
            print("Downloading matching Chrome for Testing Stable linux64 binaries.", flush=True)
            download(chrome_url, CHROME_ZIP)
            download(driver_url, DRIVER_ZIP)
            extract_archive(CHROME_ZIP)
            extract_archive(DRIVER_ZIP)
        else:
            print(
                f"Chrome and ChromeDriver {stable_version} already exist; skipping downloads.",
                flush=True,
            )
    finally:
        for archive_path in (CHROME_ZIP, DRIVER_ZIP):
            try:
                archive_path.unlink()
            except FileNotFoundError:
                pass

    chrome_ready = CHROME_PATH.is_file()
    driver_ready = DRIVER_PATH.is_file()
    for binary_path in (CHROME_PATH, DRIVER_PATH):
        if binary_path.is_file():
            binary_path.chmod(0o755)

    print(f"Chrome ready: {chrome_ready} | Driver ready: {driver_ready}", flush=True)
    if not (chrome_ready and driver_ready):
        raise RuntimeError("Chrome for Testing installation did not produce both executables")

    chrome_version = binary_version(CHROME_PATH)
    driver_version = binary_version(DRIVER_PATH)
    print(f"Chrome version: {chrome_version} | ChromeDriver version: {driver_version}", flush=True)
    if chrome_version != driver_version:
        raise RuntimeError(
            f"Chrome/ChromeDriver version mismatch: {chrome_version} != {driver_version}"
        )

    result = subprocess.run(
        ["ldd", str(CHROME_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )
    missing_libraries = sorted(set(re.findall(r"([^\s]+)\s+=>\s+not found", result.stdout + result.stderr)))
    print(f"MISSING LIBS: {', '.join(missing_libraries) if missing_libraries else 'none'}", flush=True)
    if result.returncode != 0:
        raise RuntimeError(f"ldd failed for Chrome with exit code {result.returncode}")
    if missing_libraries:
        raise RuntimeError("Chrome has unresolved Linux shared library dependencies")

    chrome_headless_smoke_test()


if __name__ == "__main__":
    main()
