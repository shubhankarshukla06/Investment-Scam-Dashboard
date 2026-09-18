import re

def region_div_balance(path, start_marker, end_marker):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    text = re.sub(r'<script\b.*?</script>', '', text, flags=re.DOTALL|re.IGNORECASE)
    lines = text.split("\n")
    # find start/end line indexes (first occurrence)
    si = ei = None
    for i,l in enumerate(lines):
        if si is None and re.search(start_marker, l): si = i
        if si is not None and re.search(end_marker, l): ei = i; break
    region = "\n".join(lines[si:ei+1]) if si is not None and ei is not None else ""
    opens = len(re.findall(r'<div\b', region, re.IGNORECASE))
    closes = len(re.findall(r'</div\s*>', region, re.IGNORECASE))
    return si, ei, opens, closes, opens-closes

# sheet-page block region: from '<div class="page-content sheet-page' to just before '<!-- ===== BS INVESTMENT'
for label, path in [("HEAD", ".workbuddy-ai/head_index.html"), ("CURRENT", ".workbuddy-ai/index_backup.html")]:
    si, ei, o, c, diff = region_div_balance(
        path,
        r'<div class="page-content sheet-page',
        r'BS INVESTMENT SCAM PAGE'
    )
    print(f"{label}: sheet-page region lines {si}-{ei}, opens={o}, closes={c}, diff(o-c)={diff}")
