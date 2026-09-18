from html.parser import HTMLParser

class Counter(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.void = {"area","base","br","col","embed","hr","img","input","link","meta","param","source","track","wbr"}
        self.in_script = False
        self.problems = []
    def handle_starttag(self, tag, attrs):
        if tag == "script":
            self.in_script = True
        if self.in_script:
            return
        if tag in self.void:
            return
        self.stack.append(tag)
    def handle_endtag(self, tag):
        if tag == "script":
            self.in_script = False
            return
        if self.in_script:
            return
        if tag in self.void:
            return
        if self.stack and self.stack[-1] == tag:
            self.stack.pop()
        elif tag in self.stack:
            # pop until match
            while self.stack and self.stack[-1] != tag:
                self.problems.append(("auto-close", self.stack[-1]))
                self.stack.pop()
            if self.stack:
                self.stack.pop()
        else:
            self.problems.append(("stray-end", tag))

for name in ["social","investment","insights"]:
    with open(f".workbuddy-ai/test_{name}.html", encoding="utf-8") as f:
        data = f.read()
    p = Counter()
    p.feed(data)
    print(f"\n===== {name} =====")
    print("Unclosed at EOF:", p.stack[:10])
    print("Num auto-closes (mismatched nesting):", len(p.problems))
    # show first few problems with line context
    # find line numbers by reprocessing with line tracking
print("\nDone.")
