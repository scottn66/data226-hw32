# HW 3.2 writeup loop — handoff prompt (pasted by Scott, Nov 24, 2026 ~6:45 PM PT)

Saved verbatim-in-substance so later rounds can re-read it. The box files it mentions (doc_review.md,
HW3.2_report_skeleton.md, loop_spec.md at /workspace/226/hw32/) are NOT on the Mac; this prompt is the spec.
Confirmed on the Mac: Airflow answers at both http://localhost:8080 and http://airflow.localhost:6563
(the astro CLI proxy); both report version 3.4.0+astro.1.

## Spec summary

- Work in ~/Desktop/226/hw32. Due Wed Nov 25, 2026, 4:29 PM PT. Hard stop for the loop: 2:30 PM PT Nov 25.
- Never edit the Google Doc. Each round produces report_patch.md (copied to iter/<n>/report_patch.md);
  Scott or the 226 bot pastes it.
- Hard limits: no secrets printed/logged/pasted (airflow/.env, airflow/include/rsa_key.p8, passwords, keys,
  connection URIs; redact account, user, key path in captured logs). Real numbers only, each traceable to a
  file in evidence/ (writeup-comments.md and this prompt are NOT evidence). No Canvas submission, no git push
  or merge, no paid API spend. Leave teammates' and Scott's in-doc edits alone. GenAI disclosure: may only
  append a line that Claude Code assisted this pass.
- Snowflake: SELECT-only, except DAG/task runs Scott has approved. Ask Scott before any DAG trigger,
  `astro dev run dags test` / `tasks test` that writes, `dbt build`, backup, restore, prune, clone, COPY, DROP.
- Allowlist of proposable text: bodies of rubric items 1–11 and the appendix. Not the title or the
  group-member line unless Scott asks.
- Rubric items (doc labels them "(rubric item N)"): 1 Environment Setup; 2 Dataset; 3 Database, Schema,
  Staging (1 pt); 4 Airflow DAG: tasks, error handling, operators, code snippet (4); 5–6 dbt Models, SCD2,
  Tests (6); 7 dbt Operator in Airflow; 8 Run, Verify, Change and Re-run (3); 9 Zero-Copy Cloning, 9.1–9.4
  (4, 1 each); 10 Takeaways and Cost (2); 11 Extension Idea (1; how to extend the exercise to improve
  learning, not the pipeline). 25 pts total. GenAI disclosure required.

## Layout

```
evidence/            raw logs, query results (.txt/.csv), new screenshots
iter/<n>/doc.txt     exported doc text for the round
iter/<n>/comments.txt open comments
iter/<n>/score.md    scorer output (before and after the patch)
report_patch.md      the only writeup deliverable
```

## Loop

Round 0: Time Travel SELECT before ~1 AM Nov 25 → evidence/10_timetravel_before_run3.txt.
Rounds n = 1..4:
1. Re-read: export doc → iter/<n>/doc.txt, open comments → iter/<n>/comments.txt; diff vs iter/<n-1>.
   Text changed by someone else and not in the last patch is locked; flag it in score.md.
2. Score with the frozen checker (fresh subagent; iter/scorer_spec.md). Deterministic number-diff:
   every numeral in the export must string-match evidence/*.txt|csv; fail the item containing a miss.
3. Pick the worst failing gap by points: 5–6 (6), 4 (4), 9 (4), 8 (3), 10 (2), 3 and 11 (1).
4. Capture evidence for it (finished-run logs and SELECT-only first; writes need Scott's yes).
5. Write report_patch.md for ONE rubric item (item 9 may cover 9.1–9.4): heading; Action replace|insert;
   Locked-text check; exact text citing evidence files; screenshots with path + caption; comment
   reply/resolve (resolve only if the item just passed).
6. Re-score the would-be section against patch + evidence; log both scores.
Stop rules (first wins): all 11 pass with zero untraced numbers; 4 iterations; 2 consecutive iterations with
no score change; Wed Nov 25 2:30 PM PT.

score.md format:
```
# iter n — <time PT>
source: iter/<n>/doc.txt (export time …)
locked: …
item 1: pass|fail — <reason>
…
item 11: pass|fail — <reason>
genai_disclosure: present|missing
untraced_numbers: …
claims_missing_evidence: …
worst_gap: <item>
score_change_vs_prev: …
```

Human-only: byline "Group 9 | Author: Scott Nelson" (add names or state individual submission).
Gap → capture table: see evidence/README.md, which lists what was captured and how.
