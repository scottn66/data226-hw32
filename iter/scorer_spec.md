# Frozen scorer spec — HW 3.2 writeup loop (do not edit between rounds)

Written Nov 24, 2026, 7:05 PM PT, before round 1 was scored. Every round uses this file unchanged.

## What you grade

Only what the round's export shows, never memory and never intentions:
- `iter/<n>/doc.txt` — the Google Doc text export;
- `iter/<n>/doc.pdf` — the PDF export, used only to read the screenshots. Use the Read tool with `pages`;
  `pdfimages -list` / `pdftoppm` are available if you need a page at higher resolution;
- `iter/<n>/number_diff.txt` — the deterministic number check (`tools/number_diff.py`), already run;
- `iter/<n>/locked.txt` — text other people changed since the previous round;
- for a re-score after a patch: `iter/<n>/report_patch.md`, read as if pasted into the doc, plus the
  screenshot files it names.

You may open anything under `evidence/` and `screenshots/` to check that a claim is supported.
You must NOT use `writeup-comments.md`, `rerun-steps.md`, `loop_prompt.md`, any `README.md`, the doc's
comments, or anything you remember about the project. Those are not evidence.

## Rubric (official Canvas rubric, as pasted by Scott on Nov 23, 2026; 25 points)

Doc section → rubric item: §2 → 1, §3 → 2, §4 → 3, §5 → 4, §6 → 5 and 6, §7 → 7, §8 → 8, §9 → 9,
§10 → 10, §11 → 11. §12 is the GenAI disclosure. §1 (Overview), the header and the Appendix are not items.

| Item | Pts | Passes only if the section shows all of these |
|---|---|---|
| 1 Environment Setup | 1 | Python, Airflow, dbt, Snowflake account, Snowflake connector, each installed and named. |
| 2 Source a dataset | 1 | The dataset is named, and where it is stored (local filesystem or S3). |
| 3 Setup Snowflake | 1 | Creation of the database and schema(s) AND of the staging table is shown (DDL or screenshot); the staging table's load is evidenced. |
| 4 Airflow DAGs | 4 | Tasks for (a) extracting from the source, (b) loading into the staging table, (c) triggering the dbt run, (d) error handling that is shown working or shown in code; (e) the operators used are named and appropriate; a code snippet is present. |
| 5 dbt models | 6 | (a) dbt project structure; (b) a staging model that cleans/prepares; (c) a core model implementing SCD Type 2 for the product dimension, with its SQL logic shown; (d) tests for the models, with evidence that they pass. A screenshot that contradicts the text (for example a different test count) fails the item. |
| 6 dbt snapshots | 1 | A snapshot for the product dimension that tracks history, with its strategy stated. |
| 7 dbt & Airflow connection | 1 | dbt runs from Airflow through a dbt operator (or equivalent), shown in a readable screenshot or log of a finished run. |
| 8 Pipeline run | 3 | (a) the DAG was run; (b) the data was verified in Snowflake (counts or query results); (c) the source data was changed and the pipeline re-run, and the SCD2 history that resulted is shown. Every run the text describes must match the evidence, and the key numbers must be readable in the screenshots or present as text. |
| 9 Zero-copy cloning | 4 | Score 9.1–9.4 separately; item 9 passes only if all four pass. **9.1** prod cloned to a dev environment by a daily task; a new feature tested there with dbt; a DAG that runs dbt in dev; the change then applied in prod. **9.2** a point-in-time clone (AT/BEFORE), a dbt model doing year-over-year (or before/after) analysis on it, and an Airflow task that creates it at year end. **9.3** a schema cloned for prototyping, a new dbt model built in the clone, and an Airflow task that clones, runs and cleans up. **9.4** a daily backup by cloning, keeping only the last 7 days, and a restore procedure from the most recent backup. Each sub-item needs evidence of a run (log, screenshot or query result), and the text must describe the code as it is now (`evidence/src/`). |
| 10 Takeaways | 2 | Five takeaways, each from something tried or observed in this project (not generic statements), and any cost figures traceable to evidence. |
| 11 Misc | 1 | Answers "how would you extend this exercise to make the learning experience better" — changes to the assignment, not only to the pipeline. |

## Deciding checks (deterministic first)

1. **Number check:** if `number_diff.txt` marks a section FAIL, every rubric item in that section fails,
   whatever else it shows. List each untraced number under `untraced_numbers`.
2. **Screenshot files:** a `png ... MISSING` line in `number_diff.txt` fails that item.
3. **Claims:** a factual claim (what the code does, what a run produced, what a query returned) that no
   evidence file or readable screenshot supports, or that `evidence/src/` contradicts (the code
   changed since the text was written), goes under `claims_missing_evidence` and fails its item. Open the
   evidence files to check; a filename alone is not support.
4. **Judgment, last:** only after 1–3, decide whether the prose covers the rubric line above. Name the
   missing piece in the reason.

## Output: write `iter/<n>/score.md` in exactly this format

```
# iter n — <time PT>   (pre-patch | post-patch)
source: iter/<n>/doc.txt (export time from iter/EXPORTS.txt)
locked: <summary of iter/<n>/locked.txt: which sections, or none>
item 1: pass|fail — <reason, one or two sentences>
item 2: ...
item 3: ...
item 4: ...
item 5: ...
item 6: ...
item 7: ...
item 8: ...
item 9: pass|fail — 9.1 pass|fail (...); 9.2 ...; 9.3 ...; 9.4 ...
item 10: ...
item 11: ...
genai_disclosure: present|missing — <present-but-template counts as present; say so>
untraced_numbers: <section: number, ...> or none
claims_missing_evidence: <list, each with the section> or none
worst_gap: <failing item with the most points; ties → order 5, 4, 9, 8, 10, 3, 11, then 1, 2, 6, 7>
points_at_risk: <sum of the points of the failing items>
score_change_vs_prev: <items that flipped vs the previous score.md, or "first round">
```

When re-scoring a patch, append a second block with the same format marked `(post-patch, would-be)` to the
same file; grade the patched section as replaced/inserted per the patch and leave every other section as
the export shows it.
