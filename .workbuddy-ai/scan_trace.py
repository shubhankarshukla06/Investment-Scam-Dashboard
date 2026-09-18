import re

path = "templates/index.html"
with open(path, encoding="utf-8") as f:
    lines = f.readlines()

div_open = re.compile(r'<div\b', re.IGNORECASE)
div_close = re.compile(r'</div\s*>', re.IGNORECASE)
endif = re.compile(r'{%\s*endif\s*%}', re.IGNORECASE)

# start after the container opens (L1713). balance=1 = just inside container.
balance = 1
start = 1713
print(f"Starting balance trace at L{start} (container open)\n")
for i in range(start, 2400):
    line = lines[i]
    # count tokens
    o = len(div_open.findall(line))
    c = len(div_close.findall(line))
    e = len(endif.findall(line))
    if o or c or e:
        balance += o - c
        # detect container close: balance returns to 0
        tag = ""
        if o: tag += f"+{o}div "
        if c: tag += f"-{c}div "
        if e: tag += "endif "
        marker = "  <<< CONTAINER CLOSE" if balance == 0 else ""
        print(f"L{i+1:4d} bal={balance:3d}  {tag:12s} {line.strip()[:70]}{marker}")
        if balance == 0:
            break
