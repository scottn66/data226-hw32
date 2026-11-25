# HW 3.2 writeup loop — summary (Nov 24, 2026)

The loop stopped after round 4. That is the stop rule "4 iterations", and it is still well before the Nov 25,
2:30 PM PT hard stop. Every score below comes from a fresh scorer subagent that used only `iter/scorer_spec.md`, the
round's export and `evidence/`. Before it, `tools/number_diff.py` checked every number in the doc against the evidence.

## Score by round (official Canvas rubric, 25 pts)

| Round | Real export | Failing items (pts) | Patch | Would-be after patch |
|---|---|---|---|---|
| 0 | — | — | Time Travel evidence saved at 6:45 PM | — |
| 1 | 9 / 25 | 1, 3, 5, 7, 9, 10, 11 (16) | §6, item 5: project tree, staging note, 12 tests passing, new screenshot 02 | 16 / 25 (item 1 also passes on new evidence) |
| 2 | 16 / 25 | 3, 7, 9, 10, 11 (9) | §9, item 9: 9.1–9.4 rewritten from the current DAGs and their run logs | 20 / 25 (v1 failed on one sentence; v2 passed) |
| 3 | 20 / 25 | 3, 7, 10, 11 (5) | §10, item 10: five takeaways, each tried / saw / means, plus fresh credits | 22 / 25 |
| 4 | 22 / 25 | 3, 7, 11 (3) | §11, item 11 (Scott's pick): four changes to the assignment, plus one appended GenAI line | 23 / 25 (items 3 and 7 left: screenshot swaps) |

"Would-be" means the score once the patch is pasted. The next round's real export confirmed rounds 1–3 each time.
Round 4 was confirmed by a final export at 8:47 PM (iter/final/check.md): §11 was pasted as written, nothing is
untraced, and the expected score is 23 / 25.

## Left for Scott (the loop stopped before these)

- **Item 3 (1 pt):** retake the §4 DDL screenshot so it starts at `setup.sql` line 6 (the warehouse, `CREATE DATABASE HW32`
  and the two `CREATE SCHEMA` lines). The current image starts at line 21.
- **Item 7 (1 pt):** replace screenshot 03 (a mid-run v1 view where every task shows "No Status") with the Task
  Instances half of screenshot 04 (the 6:18 PM run: 10 tasks, all Success, with the operator column).
- **GenAI disclosure:** the final export shows the template bullet replaced by the one-line note about this evening, so
  the section now leaves out the earlier Claude Code work on Nov 23–24 (DAG, model, test and config code; DAG runs; the
  run-3 mutation; doc comments; the diagram), any other AI tools, and what you did and checked yourself. Yours to write.
- **Byline:** "Group 9 | Author: Scott Nelson". Add the other members, or say it is an individual submission.
- **Appendix:** still "Repo/file listing; full DAG and dbt code". It is not graded, but it is still a placeholder.
- **Optional:** screenshots 05 and 06 (§8) are hard to read at page size; crop them to the `verify` line.

## What to check before submitting

1. Paste the round-4 patch, then export once more and run `python3 tools/number_diff.py <export.txt>`. It should
   report "total untraced: 0".
2. Leave out of any attachment: `airflow/.env`, `airflow/include/rsa_key.p8`, `airflow/include/profiles.yml`.
3. Optional: resolve the open "[Claude]" comments that the patches addressed. Their replies are in each
   `iter/<n>/report_patch.md`.

## Files

- `evidence/` — every number's source. 38 capture files plus 26 source copies in `evidence/src/`, all read-only, redacted, indexed in `evidence/README.md`.
- `iter/<n>/` — doc.txt, doc.pdf, comments.txt, locked.txt, number_diff*.txt, score.md, report_patch.md.
- `tools/` — sfq.sh (read-only SQL), aflog.py (Airflow logs), redact.py, number_diff.py, locked_diff.py,
  capture_batch1–3.sh.
- Nothing was written to Snowflake or triggered in Airflow during the loop. The only write-capable step any patch
  mentioned (`dbt build`, as an alternative for screenshot 02) was left to Scott, and it was not needed.
