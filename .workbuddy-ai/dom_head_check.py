from playwright.sync_api import sync_playwright
import pathlib

files = {
    "social": ".workbuddy-ai/head_social.html",
    "investment": ".workbuddy-ai/head_investment.html",
    "insights": ".workbuddy-ai/head_insights.html",
}
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    for name, fpath in files.items():
        page.goto("file://" + str(pathlib.Path(fpath).resolve()))
        page.wait_for_timeout(300)
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
        print(f"\n===== HEAD {name} =====")
        for r in result:
            print(f"  {r['cls']} active={r['active']} display={r['display']} insideContainer={r['inside']} y={r['y']} h={r['h']}")
    browser.close()
