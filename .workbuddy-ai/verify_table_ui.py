import sys, subprocess, time, os
sys.path.insert(0, ".")
from app import app

# Start a simple HTTP server so static/gui-base.css resolves correctly
server = subprocess.Popen(
    [sys.executable, "-m", "http.server", "8765"],
    cwd=r"C:\Users\Acer\OneDrive - Pixeltruth\Code_Hub\Testing_Code",
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
time.sleep(2)

client = app.test_client()
with client.session_transaction() as sess:
    sess["user_id"] = "test-admin"
    sess["email"] = "admin@test.com"
    sess["display_name"] = "Admin Test"
    sess["role"] = "superadmin"
    sess["is_admin"] = True
    sess["allowed_pages"] = [
        "scraping", "sheet", "social", "investment", "insights",
        "website_directory", "allotment", "case_report",
        "qc", "dashboard_management", "lunch",
    ]
    sess["allowed_departments"] = ["ALL"]
    sess["can_view_activity_log"] = True
    sess["logged_in"] = True

pages = {
    "scraping": "/?page=scraping",
    "sheet": "/?page=sheet",
    "social": "/?page=social",
    "investment": "/?page=investment",
    "insights": "/?page=insights",
    "qc": "/qc-gui",
    "website_directory": "/website-directory",
    "allotment": "/scam-website-allotment",
    "case_report": "/case-report",
    "dashboard_management": "/dashboard-management",
    "lunch": "/lunch-break",
}

from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    for name, url in pages.items():
        r = client.get(url)
        html = r.get_data(as_text=True)
        # Inject <base> so relative static URLs resolve to the root server
        html = html.replace("<head>", '<head><base href="http://localhost:8765/">')
        fpath = f".workbuddy-ai/verify_{name}.html"
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(html)
        page.goto(f"http://localhost:8765/{fpath}")
        page.wait_for_timeout(1500)
        out = f".workbuddy-ai/verify_{name}.png"
        page.screenshot(path=out)
        print(name, r.status_code, out)
    browser.close()

server.terminate()
print("done")
