from html.parser import HTMLParser
import re

class Tracer(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.void = {"area","base","br","col","embed","hr","img","input","link","meta","param","source","track","wbr"}
        self.in_script = False
        self.in_style = False
        self.lineno = 0
        self.first_problem = None
        self.events = []
    def handle_starttag(self, tag, attrs):
        if tag == "script": self.in_script = True
        if tag == "style": self.in_style = True
        if self.in_script or self.in_style: return
        if tag in self.void: return
        self.stack.append((tag, self.lineno))
    def handle_endtag(self, tag):
        if tag == "script": self.in_script = False; return
        if tag == "style": self.in_style = False; return
        if self.in_script or self.in_style: return
        if tag in self.void: return
        if self.stack and self.stack[-1][0] == tag:
            self.stack.pop()
        elif tag in [t for t,_ in self.stack]:
            while self.stack and self.stack[-1][0] != tag:
                ev = self.stack.pop()
                if self.first_problem is None:
                    self.first_problem = ("auto-close", ev, self.lineno)
            if self.stack: self.stack.pop()
        else:
            if self.first_problem is None:
                self.first_problem = ("stray-end", tag, self.lineno)
    def handle_data(self, data):
        for i,ch in enumerate(data):
            if ch == "\n": self.lineno += 1

with open("templates/index.html", encoding="utf-8") as f:
    lines = f.readlines()
text = "".join(lines)
# track line numbers via a wrapper
p = Tracer()
# inject line tracking by preprocessing: replace with line markers is complex; instead count newlines in feed chunks
# Simpler: feed line by line
p.lineno = 1
for line in lines:
    p.feed(line)
    p.lineno += 1
print("Final stack depth (unclosed):", len(p.stack))
print("Top of stack:", p.stack[-5:])
print("First problem:", p.first_problem)

# Print stack state right after line 2270
p2 = Tracer()
p2.lineno = 1
target = 2270
for idx, line in enumerate(lines):
    p2.feed(line)
    p2.lineno += 1
    if idx + 1 == target:
        print(f"\n--- Stack AFTER source line {target} ---")
        for t,ln in p2.stack:
            print(f"  L{ln}: <{t}>")
        break
