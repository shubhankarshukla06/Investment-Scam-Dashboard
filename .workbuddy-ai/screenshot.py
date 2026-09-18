from playwright.sync_api import sync_playwright
import pathlib

files = {
    "social": ".workbuddy-ai/test_social.html",
    "investment": ".workbuddy-ai/test_investment.html",
    "insights": ".workbuddy-ai/test_insights.html",
}
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    for name, fpath in files.items():
        page.goto("file://" + str(pathlib.Path(fpath).resolve()))
        page.wait_for_timeout(800)
        out = f".workbuddy-ai/shot_{name}.png"
        page.screenshot(path=out)
        print("saved", out)
    browser.close()
