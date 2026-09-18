import sys, subprocess, time
sys.path.insert(0, ".")
from app import app

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
    sess["allowed_pages"] = ["scraping", "investment", "social"]
    sess["allowed_departments"] = ["ALL"]
    sess["can_view_activity_log"] = True
    sess["logged_in"] = True

pages = {
    "scraping": "/?page=scraping",
    "investment": "/?page=investment",
    "social": "/?page=social",
}

from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    for name, url in pages.items():
        r = client.get(url)
        html = r.get_data(as_text=True)
        html = html.replace("<head>", '<head><base href="http://localhost:8765/">')
        page.set_content(html)
        page.wait_for_timeout(1200)

        # Get container rect
        container = page.locator(".container").first
        if container.count():
            c_rect = container.evaluate("el => ({x: el.getBoundingClientRect().x, y: el.getBoundingClientRect().y, w: el.getBoundingClientRect().width})")
        else:
            c_rect = None

        # Get table container rect
        tc = page.locator(".investment-page .table-container, .social-page .table-container, .scraping-page .table-container").first
        if tc.count():
            tc_rect = tc.evaluate("el => ({x: el.getBoundingClientRect().x, y: el.getBoundingClientRect().y, w: el.getBoundingClientRect().width})")
            tc_styles = tc.evaluate("el => ({width: getComputedStyle(el).width, marginLeft: getComputedStyle(el).marginLeft, marginRight: getComputedStyle(el).marginRight, paddingLeft: getComputedStyle(el).paddingLeft, paddingRight: getComputedStyle(el).paddingRight, boxSizing: getComputedStyle(el).boxSizing})")
        else:
            tc_rect = None
            tc_styles = None

        # Get first th (ID column)
        th = page.locator("th").first
        if th.count():
            th_styles = th.evaluate("el => ({width: getComputedStyle(el).width, minWidth: getComputedStyle(el).minWidth, paddingLeft: getComputedStyle(el).paddingLeft, text: el.textContent})")
        else:
            th_styles = None

        print(f"\n=== {name} ===")
        print(f"container: {c_rect}")
        print(f"table-container: {tc_rect}")
        print(f"table-container styles: {tc_styles}")
        print(f"first th: {th_styles}")
    browser.close()

server.terminate()
print("\ndone")
