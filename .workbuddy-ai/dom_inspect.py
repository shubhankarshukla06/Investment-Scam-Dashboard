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
        page.wait_for_timeout(300)
        # Find all .page-content elements
        result = page.evaluate(
            """() => {
                const out = [];
                document.querySelectorAll('.page-content').forEach(el => {
                    // build parent chain
                    let chain = [];
                    let cur = el;
                    while (cur && cur !== document.documentElement) {
                        chain.push(cur.tagName.toLowerCase() + (cur.className && typeof cur.className === 'string' ? '.' + cur.className.split(' ').join('.') : ''));
                        cur = cur.parentElement;
                    }
                    const rect = el.getBoundingClientRect();
                    out.push({
                        cls: '.' + (el.className||'').split(' ').join('.'),
                        chain: chain.slice(0,6),
                        insideContainer: !!el.closest('.container'),
                        rect: {x: Math.round(rect.x), y: Math.round(rect.y), w: Math.round(rect.width), h: Math.round(rect.height)},
                        display: getComputedStyle(el).display,
                        active: el.classList.contains('active')
                    });
                });
                return out;
            }"""
        )
        print(f"\n===== {name} =====")
        for r in result:
            print(f"  {r['cls']}  active={r['active']}  display={r['display']}  insideContainer={r['insideContainer']}")
            print(f"      rect={r['rect']}")
            print(f"      chain={r['chain']}")
    browser.close()
