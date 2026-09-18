from playwright.sync_api import sync_playwright
import pathlib

files = {
    "scraping": ".workbuddy-ai/test_scraping.html",
    "sheet": ".workbuddy-ai/test_sheet.html",
}
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    for name, fpath in files.items():
        page.goto("file://" + str(pathlib.Path(fpath).resolve()))
        page.wait_for_timeout(500)
        result = page.evaluate(
            """() => {
                const out = [];
                document.querySelectorAll('.page-content').forEach(el => {
                    const rect = el.getBoundingClientRect();
                    out.push({
                        cls: '.' + (el.className||'').split(' ').join('.'),
                        inside: !!el.closest('.container'),
                        y: Math.round(rect.y), h: Math.round(rect.height),
                        display: getComputedStyle(el).display,
                        active: el.classList.contains('active')
                    });
                });
                return out;
            }"""
        )
        print(f"\n===== {name} =====")
        for r in result:
            print(f"  {r['cls']} active={r['active']} display={r['display']} insideContainer={r['inside']} y={r['y']} h={r['h']}")
        page.screenshot(path=f".workbuddy-ai/shot_{name}.png")
    browser.close()