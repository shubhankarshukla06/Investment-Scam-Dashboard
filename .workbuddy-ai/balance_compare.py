import re

def trace(path, container_line):
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()
    # strip <script>...</script> blocks to avoid false div counting
    text = "".join(lines)
    # remove script blocks
    text = re.sub(r'<script\b.*?</script>', '', text, flags=re.DOTALL|re.IGNORECASE)
    lines = text.split("\n")
    div_open = re.compile(r'<div\b', re.IGNORECASE)
    div_close = re.compile(r'</div\s*>', re.IGNORECASE)
    endif = re.compile(r'{%\s*endif\s*%}', re.IGNORECASE)
    # find container line in original numbering
    # we use the stripped lines; find container
    cont = None
    for i,l in enumerate(lines):
        if re.search(r'<div class="container"', l, re.IGNORECASE):
            cont = i
            break
    balance = 1
    out = []
    for i in range(cont, len(lines)):
        o = len(div_open.findall(lines[i]))
        c = len(div_close.findall(lines[i]))
        e = len(endif.findall(lines[i]))
        if o or c or e:
            balance += o - c
            if balance <= 1:  # near container level
                tag = ("+"*o + "-"*c + ("E" if e else ""))
                out.append((i+1, balance, tag, lines[i].strip()[:55]))
    return out

h = trace(".workbuddy-ai/head_index.html", None)
c = trace("templates/index.html", None)

print("=== HEAD near-container-level events ===")
for ln,b,t,l in h:
    print(f"L{ln} bal={b} {t} {l}")
print("\n=== CURRENT near-container-level events ===")
for ln,b,t,l in c:
    print(f"L{ln} bal={b} {t} {l}")
