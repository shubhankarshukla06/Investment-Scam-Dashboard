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
    sess["allowed_pages"] = ["social"]
    sess["allowed_departments"] = ["ALL"]
    sess["can_view_activity_log"] = True
    sess["logged_in"] = True

from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    r = client.get("/?page=social")
    html = r.get_data(as_text=True)
    html = html.replace("<head>", '<head><base href="http://localhost:8765/">')
    page.set_content(html)
    page.wait_for_timeout(1200)

    # Activate social page
    page.evaluate("""
        document.querySelectorAll('.page-content').forEach(el => el.classList.remove('active'));
        const target = document.querySelector('.social-page');
        if (target) target.classList.add('active');
    """)
    page.wait_for_timeout(500)

    # Check all th elements
    ths = page.locator(".social-page th").all()
    for i, th in enumerate(ths):
        text = th.evaluate("el => el.textContent.trim()")
        visible = th.evaluate("el => getComputedStyle(el).display")
        width = th.evaluate("el => getComputedStyle(el).width")
        min_w = th.evaluate("el => getComputedStyle(el).minWidth")
        pad_left = th.evaluate("el => getComputedStyle(el).paddingLeft")
        print(f"th[{i}]: text='{text}' display='{visible}' width={width} minWidth={min_w} paddingLeft={pad_left}")

    browser.close()

server.terminate()
print("done")
