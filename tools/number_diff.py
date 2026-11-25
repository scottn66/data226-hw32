"""Deterministic number-diff for the HW 3.2 writeup loop (frozen checker, part 1).

usage: python3 tools/number_diff.py <doc.txt> [evidence_dir] > iter/<n>/number_diff.txt

Every numeral in the doc export must have a same-number hit in evidence/**/*.txt|csv.
Rules (fixed; do not change between rounds):
  * Section split by heading titles (they can sit inside a bullet line in the Docs text export).
  * Removed before extraction: [Screenshot ...] brackets, *.png names, "screenshot NN", "rubric item(s) N",
    month-day dates ("Nov 24", "Nov 20-22", "Dec 14, 2021"), ISO dates, clock times ("1:20 AM"),
    4-digit years 1900-2099 standing alone, ordered-list markers at line start ("1. "),
    and subsection labels 9.1-9.4 at the start of a heading.
  * Canonical form: drop $ % + and thousands commas, unicode minus -> "-", decimals normalized
    (0.600 -> 0.6, 15.98000 -> 15.98). Multi-dot versions (1.10.23) stay verbatim.
  * A token matches if its canonical form (or its absolute value) occurs among evidence tokens, or, for a
    dotted version prefix such as "1.10", if some evidence version token starts with it plus ".".
  * Referenced *.png files must exist under screenshots/ or evidence/.
Rule change log: Nov 24 7:40 PM PT (round 2) - file citations such as evidence/92_eoy_logs.txt and *.txt/csv/jpg/
py/sql/yml names are stripped like *.png names already were; they are references, not claims. Re-running the
changed checker on iter/1/doc.txt, iter/1/doc_patched.txt and iter/2/doc.txt gave the same PASS/FAIL per section.
writeup-comments.md, rerun-steps.md, loop_prompt.md and README files are NOT evidence.
"""
import re
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

HW32 = Path(__file__).resolve().parent.parent

SECTIONS = [  # (key, regex for the heading title)
    ("overview", r"\b1\. Overview"),
    ("item 1", r"\b2\. Environment Setup"),
    ("item 2", r"\b3\. Dataset"),
    ("item 3", r"\b4\. Database, Schema"),
    ("item 4", r"\b5\. Airflow DAG"),
    ("items 5-6", r"\b6\. dbt Models"),
    ("item 7", r"\b7\. dbt Operator"),
    ("item 8", r"\b8\. Run, Verify"),
    ("item 9", r"\b9\. Zero-Copy"),
    ("item 10", r"\b10\. Takeaways"),
    ("item 11", r"\b11\. Extension"),
    ("genai", r"\b12\. GenAI"),
    ("appendix", r"(?m)^\s*Appendix\b"),
]

MONTHS = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?"
STRIP = [
    r"\[Screenshot[^\]]*\]",
    r"\S+\.png\b",
    r"[\w./-]*evidence/[\w./-]+",          # file citations (rule added Nov 24 7:40 PM PT, round 2; see docstring)
    r"[\w./-]+\.(?:txt|csv|jpg|py|sql|yml)\b",
    r"(?i)\bscreenshots? \d+(?:\s*(?:and|,|&)\s*\d+)*",
    r"(?i)\brubric items? \d+(?:\s*[–-]\s*\d+)?",
    rf"\b{MONTHS} \d{{1,2}}(?:\s*[–-]\s*\d{{1,2}})?(?:,? \d{{4}})?",
    r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?",
    r"\b\d{1,2}:\d{2}(?::\d{2})?\s*(?:[AaPp]\.?[Mm]\.?)?",
    r"(?<![\d.])(?:19|20)\d{2}(?![\d.])",
    r"(?m)^\s*\d+\.\s",
    r"(?m)(?:^|\*\s+)9\.[1-4]\b",
]
TOKEN = re.compile(r"(?<![\w.])[-−+]?\$?\d[\d,]*(?:\.\d+)*%?")
VERSION = re.compile(r"^\d+(?:\.\d+){2,}$")


def canon(tok: str) -> str:
    t = tok.replace("−", "-").replace("$", "").replace("%", "").lstrip("+")
    t = re.sub(r"(?<=\d),(?=\d{3}\b)", "", t).rstrip(",")
    if VERSION.match(t.lstrip("-")):
        return t
    try:
        d = Decimal(t)
        s = format(d.normalize(), "f")
        return s
    except InvalidOperation:
        return t


def split_sections(text: str):
    hits = []
    for key, pat in SECTIONS:
        m = re.search(pat, text)
        if m:
            hits.append((m.start(), key))
    hits.sort()
    out = [("header", text[: hits[0][0]] if hits else text)]
    for i, (pos, key) in enumerate(hits):
        end = hits[i + 1][0] if i + 1 < len(hits) else len(text)
        out.append((key, text[pos:end]))
    return out


def tokens(text: str):
    for pat in STRIP:
        text = re.sub(pat, " ", text)
    # drop the heading's own number ("8. Run" -> "Run") and section labels like "6." at heading start
    text = re.sub(r"\b(?:1[0-2]|[1-9])\.(?= [A-Z])", " ", text)
    return [m.group(0) for m in TOKEN.finditer(text)]


def evidence_tokens(ev_dir: Path):
    toks, files = {}, {}
    for f in sorted(list(ev_dir.rglob("*.txt")) + list(ev_dir.rglob("*.csv"))):
        txt = f.read_text(errors="replace")
        for m in TOKEN.finditer(txt):
            c = canon(m.group(0))
            for v in {c, c.lstrip("-")}:
                toks.setdefault(v, f.relative_to(ev_dir.parent).as_posix())
    return toks


def match(c: str, ev: dict, raw: str = ""):
    raw = raw.replace("$", "").replace("%", "").lstrip("+")
    if re.fullmatch(r"\d+\.\d+", raw):
        for k, f in ev.items():
            if VERSION.match(k) and k.startswith(raw + "."):
                return f
    if c in ev:
        return ev[c]
    if c.lstrip("-") in ev:
        return ev[c.lstrip("-")]
    if re.fullmatch(r"\d+\.\d+", c):
        for k, f in ev.items():
            if VERSION.match(k) and k.startswith(c + "."):
                return f
    return None


def main():
    doc = Path(sys.argv[1])
    ev_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else HW32 / "evidence"
    text = doc.read_text(errors="replace")
    ev = evidence_tokens(ev_dir)
    print(f"# number_diff {doc} vs {ev_dir} ({len(ev)} evidence tokens)")
    total_miss = 0
    for key, body in split_sections(text):
        toks = tokens(body)
        misses, hits = [], []
        for t in toks:
            c = canon(t)
            f = match(c, ev, t)
            (hits if f else misses).append((t, c, f))
        total_miss += len(misses)
        status = "PASS" if not misses else "FAIL"
        print(f"\n## {key}: {status}  ({len(hits)} traced, {len(misses)} untraced)")
        for t, c, f in misses:
            print(f"   MISS {t!r} (canon {c})")
        for t, c, f in hits:
            print(f"   ok   {t!r} -> {f}")
        pngs = sorted(set(re.findall(r"[\w.-]+\.png", body)))
        for p in pngs:
            found = list((HW32 / "screenshots").rglob(p)) + list(ev_dir.rglob(p))
            print(f"   png  {p}: {'exists ' + found[0].relative_to(HW32).as_posix() if found else 'MISSING'}")
    print(f"\n# total untraced: {total_miss}")


if __name__ == "__main__":
    main()
