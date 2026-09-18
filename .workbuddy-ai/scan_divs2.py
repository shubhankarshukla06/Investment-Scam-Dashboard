import re

path = "templates/index.html"
with open(path, encoding="utf-8") as f:
    lines = f.readlines()

div_open = re.compile(r'<div\b', re.IGNORECASE)
div_close = re.compile(r'</div\s*>', re.IGNORECASE)
container_re = re.compile(r'<div class="container"', re.IGNORECASE)

# Find the container line
container_line = None
for i, line in enumerate(lines, start=1):
    if container_re.search(line):
        container_line = i
        break
print("container opens at L", container_line)

balance = 1  # we count the container itself as open
first_zero = None
for i in range(container_line, len(lines)):
    line = lines[i]
    opens = len(div_open.findall(line))
    closes = len(div_close.findall(line))
    balance += opens - closes
    if balance == 0 and first_zero is None:
        first_zero = i + 1  # line index is 0-based, +1 => next line is the close? actually i is 0-based
        # The close that brought it to 0 is on line i+1 (1-based). print it.
        print(f"FIRST balance==0 at scan line idx {i} => file line {i+1}: {lines[i].strip()[:140]}")
        break

print("first_zero file line:", first_zero)
