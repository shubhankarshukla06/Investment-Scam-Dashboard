from playwright.sync_api import sync_playwright
import pathlib, glob, os

files = sorted(glob.glob(".workbuddy-ai/audit_*.html"))
with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1366, "height": 768})
    for fpath in files:
        name = os.path.basename(fpath)[6:-5]
        page.goto("file://" + str(pathlib.Path(fpath).resolve()))
        page.wait_for_timeout(300)
        info = page.evaluate(
            """() => {
                const out = [];
                document.querySelectorAll('.table-container, .table-wrap').forEach(tc => {
                    const cs = getComputedStyle(tc);
                    const rect = tc.getBoundingClientRect();
                    const table = tc.querySelector('table');
                    const firstTh = tc.querySelector('th');
                    const firstTd = tc.querySelector('tbody td');
                    const fcs = firstTh ? getComputedStyle(firstTh) : null;
                    const fds = firstTd ? getComputedStyle(firstTd) : null;
                    out.push({
                        radius: cs.borderTopLeftRadius,
                        border: cs.borderTopWidth + ' ' + cs.borderStyle,
                        shadow: cs.boxShadow.slice(0,30),
                        overflowX: cs.overflowX,
                        overflowY: cs.overflowY,
                        bg: cs.backgroundColor,
                        w: Math.round(rect.width),
                        firstThPadLeft: fcs ? fcs.paddingLeft : null,
                        firstThWidth: firstTh ? Math.round(firstTh.getBoundingClientRect().width) : null,
                        firstTdPadLeft: fds ? fds.paddingLeft : null,
                        firstCellText: firstTd ? (firstTd.innerText||'').slice(0,20) : null,
                        firstCellScrollW: firstTd ? firstTd.scrollWidth : null,
                        firstCellClientW: firstTd ? firstTd.clientWidth : null,
                        hasTable: !!table
                    });
                });
                return out;
            }"""
        )
        print(f"\n===== {name} (containers: {len(info)}) =====")
        for r in info:
            clip = ""
            if r['firstCellScrollW'] and r['firstCellClientW'] and r['firstCellScrollW'] > r['firstCellClientW']+1:
                clip = "  <-- CELL CLIPPED"
            print(f"  radius={r['radius']} border={r['border']} shadow='{r['shadow']}' ovX={r['overflowX']} ovY={r['overflowY']} bg={r['bg']} w={r['w']}")
            print(f"    firstTh padL={r['firstThPadLeft']} w={r['firstThWidth']} | firstTd padL={r['firstTdPadLeft']} text='{r['firstCellText']}' scrollW={r['firstCellScrollW']} clientW={r['firstCellClientW']}{clip}")
    browser.close()
