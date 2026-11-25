# Final check — 2026-11-24 20:48 PT (after round 4; deterministic checks only, no scorer run)

source: iter/final/doc.txt + doc.pdf (Drive export; doc modifiedTime 2026-11-25T04:46:19Z = 20:46 PT)

- Round-4 patch, section 11: pasted word for word (compared with iter/4/doc_patched.txt). The item 11 re-check (20:25 PT,
  iter/4/score.md) graded this same text as pass.
- Sections before 11: unchanged since iter/4 (whitespace-normalized diff is empty).
- Number check (tools/number_diff.py): every section PASS, total untraced 0 (iter/final/number_diff.txt).
- Screenshots: the 9 images are the same as in iter/4/doc.pdf (same sizes on the same pages). The section 4 DDL image
  (1074x636, setup.sql from line 21) and screenshot 03 in section 7 (1429x944, the mid-run v1 view) are unchanged, so
  items 3 and 7 still fail.
- Section 12 GenAI: the original template bullet is gone. The section now holds only the round-4 line about this
  evening's evidence capture and drafting. That line is accurate, but it does not cover the earlier Claude Code work on
  Nov 23–24 (writing and rewriting the DAGs, models, tests and config; running the DAGs and the run-3 mutation; the
  35 doc comments and the redrawn diagram), any other AI tools, or what Scott did and checked himself. "Present" for
  the rubric; incomplete as a disclosure. Scott writes it.

Expected score as exported: 23 / 25 (items 3 and 7 failing, 1 pt each), or 25 / 25 with the two screenshot swaps.
