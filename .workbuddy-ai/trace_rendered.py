import re

path = ".workbuddy-ai/test_investment.html"
with open(path, encoding="utf-8") as f:
    text = f.read()

# strip script and style blocks to avoid counting JS/string divs
text = re.sub(r'<script\b.*?</script>', '', text, flags=re.DOTALL|re.IGNORECASE)
text = re.sub(r'<style\b.*?</style>', '', text, flags=re.DOTALL|re.IGNORECASE)
lines = text.split("\n")

div_open = re.compile(r'<div\b', re.IGNORECASE)
div_close = re.compile(r'</div\s*>', re.IGNORECASE)

cont = None
for i,l in enumerate(lines):
    if re.search(r'<div class="container"', l, re.IGNORECASE):
        cont = i
        break
print("container at rendered line", cont+1)

balance = 1
for i in range(cont, len(lines)):
    o = len(div_open.findall(lines[i]))
    c = len(div_close.findall(lines[i]))
    if o or c:
        balance += o - c
        if balance <= 1:
            print(f"L{i+1} bal={balance} | {lines[i].strip()[:60]}")
        if balance < 0:
            print("  >>> container closed prematurely here!")
            break
print("final balance", balance)
