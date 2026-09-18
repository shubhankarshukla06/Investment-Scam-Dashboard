import re

path = "templates/index.html"
with open(path, encoding="utf-8") as f:
    lines = f.readlines()

div_open = re.compile(r'<div\b', re.IGNORECASE)
div_close = re.compile(r'</div\s*>', re.IGNORECASE)
endif = re.compile(r'{%\s*endif\s*%}', re.IGNORECASE)

balance = 1  # just inside container at L1713
start = 1713
print(f"Full balance trace from L{start}\n")
neg = False
for i in range(start, len(lines)):
    line = lines[i]
    o = len(div_open.findall(line))
    c = len(div_close.findall(line))
    e = len(endif.findall(line))
    if o or c or e:
        balance += o - c
        tag = ""
        if o: tag += f"+{o}d "
        if c: tag += f"-{c}d "
        if e: tag += "-if "
        note = ""
        if balance < 0:
            note = "  <<< NEGATIVE (stray close)"
            neg = True
        if balance == 0 and i > start:
            note = "  <<< back to container level"
        print(f"L{i+1:4d} bal={balance:3d} {tag:10s} {line.strip()[:60]}{note}")
print("\nFINAL balance:", balance)
print("Went negative anywhere:", neg)
