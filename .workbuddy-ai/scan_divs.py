import re

path = "templates/index.html"
with open(path, encoding="utf-8") as f:
    lines = f.readlines()

# Track div balance. We only look at HTML div tags, not Jinja.
div_open = re.compile(r'<div\b', re.IGNORECASE)
div_close = re.compile(r'</div\s*>', re.IGNORECASE)

balance = 0
flagged = []
for i, line in enumerate(lines, start=1):
    opens = len(div_open.findall(line))
    closes = len(div_close.findall(line))
    balance += opens - closes
    # Flag transitions into negative balance
    if balance < 0:
        flagged.append((i, balance, line.strip()[:120]))
        # reset context: treat this as a stray close; continue but note
        balance = 0

print("=== Lines where balance went negative (stray </div>) ===")
for i, b, l in flagged:
    print(f"L{i}: balance={b}  {l}")

print("\n=== Final balance:", balance, "===")
