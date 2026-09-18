from playwright.sync_api import sync_playwright
import pathlib

fpath = ".workbuddy-ai/test_investment.html"
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    page.goto("file://" + str(pathlib.Path(fpath).resolve()))
    page.wait_for_timeout(300)
    info = page.evaluate(
        """() => {
            const container = document.querySelector('.container');
            const body = document.body;
            function kids(el){
                return Array.from(el.children).map(c => c.tagName.toLowerCase() + (c.className && typeof c.className==='string' ? '.'+c.className.split(' ').join('.') : ''));
            }
            return {
                containerExists: !!container,
                containerKids: container ? kids(container) : [],
                bodyKids: kids(body)
            };
        }"""
    )
    print("container exists:", info["containerExists"])
    print("\nCONTAINER direct children:")
    for k in info["containerKids"]:
        print("   ", k)
    print("\nBODY direct children:")
    for k in info["bodyKids"]:
        print("   ", k)
    browser.close()
