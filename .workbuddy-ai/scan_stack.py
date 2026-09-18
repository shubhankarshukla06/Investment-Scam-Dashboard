import re

path = "templates/index.html"
with open(path, encoding="utf-8") as f:
    lines = f.readlines()

div_open = re.compile(r'<div\b', re.IGNORECASE)
div_close = re.compile(r'</div\s*>', re.IGNORECASE)

stack = []          # stack of line numbers where <div> opened
stray_closes = []   # (close_line, expected_pop)
for i, line in enumerate(lines, start=1):
    opens = div_open.findall(line)
    closes = div_close.findall(line)
    for _ in opens:
        stack.append(i)
    for _ in closes:
        if stack:
            stack.pop()
        else:
            stray_closes.append(i)

print("Total <div> opens:", len(div_open.findall("".join(lines))))
print("Total </div> closes:", len(div_close.findall("".join(lines))))
print("\n=== STRAY </div> (no matching open on stack) ===")
for ln in stray_closes:
    print(f"L{ln}: {lines[ln-1].strip()[:120]}")
print("\nRemaining unclosed <div> opens (stack):", len(stack))
if stack:
    for ln in stack[:10]:
        print(f"  open at L{ln}: {lines[ln-1].strip()[:120]}")
