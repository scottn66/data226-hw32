"""List text changed between two exports that the last report_patch.md did not propose (= locked).
usage: python3 tools/locked_diff.py iter/<n-1>/doc.txt iter/<n>/doc.txt [iter/<n-1>/report_patch.md]"""
import difflib, re, sys
from pathlib import Path
norm0 = lambda s: re.sub(r"\s+", " ", s).strip()
old = [norm0(l) for l in Path(sys.argv[1]).read_text().splitlines() if norm0(l)]
new = [norm0(l) for l in Path(sys.argv[2]).read_text().splitlines() if norm0(l)]
patch = Path(sys.argv[3]).read_text() if len(sys.argv) > 3 and Path(sys.argv[3]).exists() else ""
norm = lambda s: re.sub(r"\s+", " ", s).strip()
print(f"# locked_diff {sys.argv[1]} -> {sys.argv[2]}" + (f" (patch {sys.argv[3]})" if patch else " (no previous patch)"))
for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, old, new).get_opcodes():
    if tag == "equal":
        continue
    for line in new[j1:j2]:
        if not norm(line):
            continue
        key = lambda s: re.sub(r"[^a-z0-9]+", "", re.sub(r"^\s*[*•◦>-]\s*", "", s).lower())
        origin = "from-patch" if patch and key(line) and key(line) in key(patch) else "LOCKED (someone else's edit)"
        print(f"{origin}: {norm(line)[:240]}")
