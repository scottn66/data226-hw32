# report_patch.md — round 1: rubric item 5 (dbt models, 6 pts), section 6

Written Nov 24, 2026, about 7:15 PM PT, against `iter/1/doc.txt` (Drive export 6:50 PM; doc last edited 6:24 PM).
Paste by hand. This patch changes nothing outside section 6. Item 6 (snapshot) already passes; this patch
leaves its bullet alone.

Why this item: the round-1 scorer (`iter/1/score.md`) failed item 5 for two reasons. Screenshot 02 shows
"8 data tests" and "PASS=11 … TOTAL=11", which contradicts the text's "12 tests in the main build". And
the dbt project structure (rubric line 5a) is not shown anywhere.

## 6. dbt Models, SCD2, Tests (rubric items 5–6)

**Action:** insert (3 blocks), replace (screenshot 02 only).

**Locked-text check:** `tools/locked_diff.py iter/0/doc.txt iter/1/doc.txt` lists the four section 6 lines you
wrote this evening as locked:
- the window-function bullet;
- `models/marts/dim_products.sql`;
- the "12 tests in the main build" bullet;
- the `windowed as (` SQL excerpt.

This patch does not change them. It only inserts new bullets next to them, and replaces screenshot 02, which
has been in the doc since Nov 22 and wasn't edited tonight. The bullets "stg_products (view): type casting
and cleanup" and "products_snapshot: check strategy …" also stay as they are.

### Insert A: new first bullet of section 6, before "stg_products (view): type casting and cleanup"

> • Project structure. Airflow runs the dbt project in `airflow/dags/dbt/hw32_dbt`; `hw32/hw32_dbt` is an identical
> copy for local runs (evidence/06_dbt_nodes.txt, evidence/06_dbt_project_tree.txt):
>
> ```
> hw32_dbt/
>   dbt_project.yml        vars: enable_rating (9.1), build_yoy (9.2)
>   models/staging/        sources.yml, stg_products.sql (view), schema.yml
>   models/marts/          dim_products.sql (SCD2 table), schema.yml
>   models/history/        price_yoy.sql, built only when build_yoy is true (9.2)
>   models/prototype/      price_bands.sql, built only with the proto target (9.3)
>   snapshots/             products_snapshot.sql
>   tests/                 one_current_row_per_product, no_overlapping_validity,
>                          scd2_matches_snapshot, rating_in_range
> ```
>
> `dbt ls` on the main build lists 2 models, 1 snapshot and 12 tests (evidence/06_dbt_nodes.txt). The dbt targets
> are prod (HW32), dev (the HW32_DEV clone) and proto (HW32.PROTOTYPE).

### Insert B: new sub-bullet directly under "stg_products (view): type casting and cleanup"

> ◦ It trims titles, casts price to number(10,2), and strips the trailing ".0" that the CSV export left on sku and
> gtin13. Every column is read from the raw landing table through `source('raw', 'stg_products_raw')`
> (evidence/06_dbt_project_tree.txt).

### Insert C: new bullet directly after the "12 tests in the main build …" bullet (before the SQL excerpt)

> • All 12 tests pass in the Airflow run of Nov 24, 6:18 PM PT. Cosmos runs each model's tests as a separate
> task, so the results come from three task logs (evidence/06_cosmos_dbt_logs_rerun_0618pm.txt):
>
> ```
> dbt_transform.stg_products.test           Done. PASS=3 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=3
> dbt_transform.dim_products.test           Done. PASS=8 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=8
> dbt_transform.scd2_matches_snapshot_test  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
> ```
>
> In the same run, the snapshot task logged "OK snapshotted SNAPSHOTS.products_snapshot [SUCCESS 0]". The source
> file was the same as in run 3, so the snapshot recorded no new versions, and `verify` still reported
> history=254 (evidence/08_verify_logs.txt).

### Replace: screenshot 02 and the empty bullet after it

- Delete the image `02_dbt_build_pass11.png`, its "[Screenshot 02_dbt_build_pass11.png]" line, and the empty bullet
  that follows the image.
- Insert `evidence/06_airflow_dim_products_test_log.jpg` (800 × 537, captured from the Airflow UI at 7:00 PM).
  Caption:

  > Screenshot 02: Airflow, run of Nov 24 6:18 PM PT, task dbt_transform.dim_products.test (DbtTestLocalOperator):
  > 8 of 8 tests PASS.

- Sharper option, same evidence, no write. Open
  http://localhost:8080/dags/hw32_products_pipeline/runs/manual__2026-11-25T02%3A18%3A06.417949%2B00%3A00/tasks/dbt_transform.dim_products.test,
  scroll the log to the end, and take your own screenshot (Cmd+Ctrl+Shift+4). Keep the same caption.
- Alternative that writes, only if you choose to: run `dbt build` in `hw32/hw32_dbt`, which rebuilds the prod tables.
  It should end with `PASS=15 … TOTAL=15`. A terminal screenshot of that can replace 02 instead. If you use it,
  save the output as `evidence/06_dbt_build.txt` so the number check can trace it.

### Comment reply / resolve

- Comment #3, "[Claude] Out of date: 8 tests then, 12 now. …". Reply:

  > Replaced screenshot 02 with the Airflow log of dbt_transform.dim_products.test (Nov 24 6:18 PM run, 8/8 PASS).
  > Added the three test-task results (3 + 8 + 1 = 12 PASS) and the project structure. Evidence:
  > evidence/06_cosmos_dbt_logs_rerun_0618pm.txt, evidence/06_dbt_nodes.txt, evidence/06_dbt_project_tree.txt.

  Resolve it only after this item passes in the next round's scored export. The post-patch re-score in
  `iter/1/score.md` passes it, but that is a would-be score.

## Not in this patch (other items, for later rounds)

- **Item 1:** now has evidence for the locked SNOWFLAKE_JWT sentence. evidence/01_snowflake_jwt_failure.txt
  shows extract and backup_database each failing 3 times at midnight on Nov 24 with "Cannot specify basic
  authenticator together with private key parameters", then succeeding on try 4. No doc change is needed;
  the next round's scorer will see the file.
- **Item 7:** fails only because the section 7 screenshot (03) is the mid-run one. Swapping in the Task Instances
  half of screenshot 04 fixes it.
