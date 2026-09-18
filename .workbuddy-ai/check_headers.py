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
    sess["allowed_pages"] = ["scraping","investment","social","qc","website_directory","dashboard_management","allotment","lunch"]
    sess["allowed_departments"] = ["ALL"]
    sess["can_view_activity_log"] = True
    sess["logged_in"] = True

pages = {
    "scraping": "/?page=scraping",
    "investment": "/?page=investment",
    "social": "/?page=social",
    "qc": "/qc-gui",
    "website_directory": "/website-directory",
    "dashboard_management": "/dashboard-management",
    "allotment": "/scam-website-allotment",
    "lunch": "/lunch-break",
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

        first_th = page.locator(".table-container th, .table-wrap th").first
        if first_th.count() == 0:
            print(name, "NO TABLE HEADER")
            continue
        styles = first_th.evaluate("""el => ({
            bg: getComputedStyle(el).backgroundColor,
            color: getComputedStyle(el).color,
            fontWeight: getComputedStyle(el).fontWeight,
            borderRight: getComputedStyle(el).borderRight,
            borderBottom: getComputedStyle(el).borderBottom,
            textTransform: getComputedStyle(el).textTransform,
        })""")
        print(name, styles)
    browser.close()

server.terminate()
print("done")
