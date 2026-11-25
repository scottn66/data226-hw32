# report_patch.md — round 2: rubric item 9 (zero-copy cloning, 4 pts), section 9

Written Nov 24, 2026, about 7:35 PM PT, against `iter/2/doc.txt` (Drive export 7:19 PM; doc last edited 7:17 PM).
Paste by hand. This patch changes nothing outside section 9.

v2 (7:45 PM): the first would-be re-score caught one wrong sentence. It said the Nov 24 backup came from the 12:38 AM run;
in fact the scheduled midnight run created it at 12:33 AM. That sentence is corrected in 9.4. The same re-score flagged
four wording points, and they are fixed too: the trigger rule of fail_if_prototype_failed, the 9.2 log lines (now
verbatim), the count of restore tasks, and which RAW tables get rebuilt.
The item 9 re-check of v2 (7:37 PM) passed and left three small wording points. They are fixed in this file: the prune
rule is stated exactly, the None backup name is described from the log timing instead of guessing a cause, and the
sentence says what dbt rebuilds (dim_products) instead of all of ANALYTICS.

Why this item: the round-2 scorer (`iter/2/score.md`) fails all four sub-items. 9.1 and 9.4 describe DAG versions that no longer exist (add_rating / verify_isolation, DIM_PRODUCTS_BKP_ table backups). 9.2 says AT(OFFSET => -1800) with a 30-minute offset, but the code uses AT(TIMESTAMP => …) and the log shows as_of 01:18. 9.3 has no run evidence. The number check also fails §9 on -1800. Item 9 is now the failing item with the most points (4).

## 9. Zero-Copy Cloning (rubric item 9)

**Action:** replace. The four sub-headings 9.1–9.4 stay. The bullets under each of them are replaced, and so are
the two old screenshots in 9.1 and 9.4. One bullet is inserted under the section heading.

**Locked-text check:** `tools/locked_diff.py iter/1/doc.txt iter/2/doc.txt iter/1/report_patch.md` shows tonight's
changes only in section 6. Section 9 is the same text as the 2:27 PM export (`iter/0/doc.txt`), so nothing in it is
locked. The sub-headings are not changed. The 9.4 "Lesson" bullet is kept, made precise, and cited.

### Insert: new bullet directly under the heading "9. Zero-Copy Cloning (rubric item 9)"

> • Five DAGs and one task of the daily pipeline cover 9.1–9.4, and every clone is zero-copy (`CREATE … CLONE`):
> 9.1 `clone_dev_database` (the last task of the daily pipeline) and `hw32_dev_clone_rating`; 9.2 `hw32_eoy_snapshot`; 9.3 `hw32_prototype`; 9.4
> `hw32_daily_backup` and `hw32_restore` (evidence/src/).

### 9.1 Dev environment: replace both bullets and the screenshot with

> • Daily clone: the pipeline's last task, `clone_dev_database`, runs `CREATE OR REPLACE DATABASE HW32_DEV CLONE HW32`
> every day at midnight Pacific. It did so in the scheduled run of Nov 24 and again in the 6:18 PM run
> (evidence/91_dev_clone_dbt_logs.txt).
>
> • New feature, tested in dev: `hw32_dev_clone_rating` (manual) clones prod to HW32_DEV and lands a ratings table
> only in `HW32_DEV.RAW.PRODUCT_RATINGS`. It then runs the dbt models with the dev target and `enable_rating=true`, so
> `dim_products` gains a `rating` column in the clone only. The Kaggle data has no ratings, so they are simulated:
> 1.0–5.0, derived from a hash of `product_id`, the same on every run (evidence/src/dags__hw32_dev_clone_rating.py.txt).
>
> • Checks before promotion, run of Nov 24 12:29 AM (evidence/91_dev_clone_dbt_logs.txt, evidence/91_dev_clone_logs.txt):
>
> ```
> dbt_dev.dim_products.test  (target='dev')  7 of 8 PASS rating_in_range; Done. PASS=8 ... TOTAL=8
> check_dev_feature          Record: (True, True, True)
> check_prod_untouched       prod DIM_PRODUCTS fingerprint before=2728168932473494028 after=2728168932473494028
> promote_ratings_to_prod    CREATE OR REPLACE TABLE HW32.RAW.PRODUCT_RATINGS CLONE HW32_DEV.RAW.PRODUCT_RATINGS
> ```
>
> • Applied in prod: after the promotion, `enable_rating` was set to true in `dbt_project.yml`, so the daily pipeline now
> builds the rating column in prod too. All 2,551 current products have a rating, and the average is 2.99
> (evidence/91_dev_clone_untouched.txt, evidence/src/dbt_dbt_project.yml.txt).

Delete the image under 9.1 (the old `clone_prod_to_dev → add_rating → verify_isolation` run; that DAG version no
longer exists) and its "[Screenshot: dev clone DAG green + verify_isolation log]" line.

### 9.2 Point-in-time clone: replace both bullets with

> • `hw32_eoy_snapshot` runs at midnight on Jan 1, Pacific time (schedule `0 0 1 1 *`). Its tasks:
> ◦ `freeze_snapshot` clones `HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT` `AT(TIMESTAMP => the year boundary)` into
> `PRODUCTS_SNAPSHOT_EOY_<year>`, and points the view `PRODUCTS_SNAPSHOT_FROZEN` at that clone.
> ◦ The dbt model `price_yoy` compares each product's frozen price and title with today's `dim_products`, and labels it
> added, removed, repriced, retitled or unchanged.
> ◦ `summarize_yoy` logs the counts.
>
> (evidence/src/dags__hw32_eoy_snapshot.py.txt, evidence/src/dbt_models__history__price_yoy.sql.txt)
>
> • Why one table and not the whole database: dbt rebuilds every ANALYTICS table with CREATE OR REPLACE on each run,
> which restarts its Time Travel history. The snapshot, by contrast, is merged in place. It is a TRANSIENT table (dbt's
> default), so Time Travel reaches back only 1 day (the log shows retention=1). That is why the clone must be taken at
> the year boundary: a year-end state cannot be cloned weeks later.
>
> • Demo before Jan 1. The DAG takes an `as_of` parameter. Run of Nov 24 1:21 AM with as_of 1:18 AM, just before
> run 3 (evidence/92_eoy_logs.txt, evidence/92_price_yoy.txt):
>
> ```
> freeze_snapshot  source HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT kind=TRANSIENT retention=1 day(s)
> freeze_snapshot  cloned HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT_PIT_20261124_0118 as of 2026-11-24 01:18:00 -08:00:
>                  2678 snapshot rows then, 2805 now
> summarize_yoy    Operator output is: [[(..., 'repriced', 83, Decimal('1.63')), (..., 'retitled', 44, ...),
>                  (..., 'unchanged', 2424, ...)]]
> ```
>
> The 83 repriced products (−19.45% to +19.67%) and the 44 retitled ones are exactly run 3's changes: 83 price
> and 44 title (evidence/08_source_diffs.txt).

### 9.3 Prototype schema: replace the bullet with

> • `hw32_prototype` (manual) runs these tasks:
> ◦ `clone_analytics_schema` runs `CREATE OR REPLACE SCHEMA HW32.PROTOTYPE CLONE HW32.ANALYTICS`.
> ◦ dbt builds the new model `price_bands` with the proto target. Its `ref('dim_products')` therefore reads the cloned
> table, and prod is never written.
> ◦ `show_results` prints the table.
> ◦ `drop_prototype_schema` drops the schema even if an earlier task failed (trigger rule all_done).
> ◦ `fail_if_prototype_failed` runs only if the clone, the dbt build or `show_results` failed (trigger rule one_failed),
> so such a run still ends red after the cleanup. On success it is skipped.
>
> (evidence/src/dags__hw32_prototype.py.txt)
>
> • The prototype model groups the current catalog into price bands. For each band it reports the median price, the
> share of the catalog (`RATIO_TO_REPORT`), and how many products were ever repriced, taken from the SCD2 history
> (evidence/src/dbt_models__prototype__price_bands.sql.txt).
>
> • Run of Nov 24 2:08 PM (evidence/93_prototype_dbt_logs.txt, evidence/93_prototype_logs.txt): `price_bands` was built in
> PROTOTYPE (target='proto') and its 3 tests passed. Then `DROP SCHEMA IF EXISTS HW32.PROTOTYPE` ran, and the schema no
> longer exists. Result:
>
> ```
> band        products  median   share  repriced
> under $25      532     15.98   20.9%  42 (7.9%)
> $25-99         930     46.98   36.5%  59 (6.3%)
> $100-499       834    187.075  32.7%  58 (7.0%)
> $500+          255    919.29   10.0%   9 (3.5%)
> ```

### 9.4 Backup and restore: replace the two bullets and the screenshot with (keep the "Lesson" bullet, edited as shown)

> • Daily backup: `hw32_daily_backup` runs at midnight Pacific. Its tasks:
> ◦ `backup_name` derives `HW32_BACKUP_<yyyymmdd>` from the run's Pacific date. Airflow 3 manual runs have no logical
> date, so the spec's `{{ ds_nodash }}` is undefined there.
> ◦ `backup_database` runs `CREATE DATABASE IF NOT EXISTS HW32_BACKUP_<yyyymmdd> CLONE HW32`. IF NOT EXISTS means a
> second run on the same day cannot overwrite a good backup.
> ◦ `prune_old_backups` drops every backup dated before today minus 6 days, so it keeps 7: today's and the 6 before it.
>
> (evidence/src/dags__hw32_daily_backup.py.txt)
>
> • Pruning test, Nov 24. Two fake old backups, `HW32_BACKUP_20261116` and `HW32_BACKUP_20261118`, were created by hand
> at 12:33 AM (evidence/94_prune_test_fixtures.txt). The DAG run at 12:36 AM then logged (evidence/94_backup_logs.txt):
>
> ```
> prune_old_backups  dropped HW32_BACKUP_20261116
> prune_old_backups  backups kept (>= 20261118): ['HW32_BACKUP_20261118', 'HW32_BACKUP_20261124']
> ```
>
> 20261118 is exactly on the 7-day boundary, so it was kept; it was dropped by hand afterwards. In that run,
> `backup_database` started before `backup_name` had returned, so the name rendered as None (its log shows
> `CREATE DATABASE IF NOT EXISTS None CLONE HW32`). The later runs render the name correctly. The real Nov 24
> backup, `HW32_BACKUP_20261124`, already existed: the scheduled midnight run created it at 12:33 AM, on its 4th try,
> after the connection fix described in §2 (evidence/01_snowflake_jwt_failure.txt, evidence/94_backups.txt).
>
> • Restore: `hw32_restore` (manual) has three tasks:
> ◦ `pick_backup` takes the newest `HW32_BACKUP_*`, or a date passed as a parameter.
> ◦ `restore_snapshots` clones that backup's SNAPSHOTS schema into `HW32.SNAPSHOTS_RESTORE` and checks that the clone
> isn't empty. It then swaps the clone with `HW32.SNAPSHOTS` in one `ALTER SCHEMA … SWAP WITH`, and keeps the old schema
> as `SNAPSHOTS_PRE_RESTORE_<time>`.
> ◦ `rerun_pipeline` triggers `hw32_products_pipeline` and waits for it to finish.
>
> Only SNAPSHOTS is restored. The landing table `RAW.STG_PRODUCTS_RAW` is reloaded from the source on every run,
> `RAW.PRODUCT_RATINGS` is re-created by the 9.1 DAG's promotion step, and the pipeline's dbt run rebuilds
> `dim_products` from the snapshot. The
> snapshot history is the only data nothing else can recreate (evidence/src/dags__hw32_restore.py.txt,
> evidence/src/dags__hw32_dev_clone_rating.py.txt).
>
> • Restore run of Nov 24 12:39 AM (evidence/94_restore_logs.txt):
>
> ```
> pick_backup        restoring from HW32_BACKUP_20261124 (available: ['HW32_BACKUP_20261124'])
> restore_snapshots  restored 2678 snapshot rows from HW32_BACKUP_20261124;
>                    previous schema kept as SNAPSHOTS_PRE_RESTORE_20261124_003926
> verify             (of the hw32_products_pipeline run that rerun_pipeline triggered, 12:39 AM)
>                    dim rows=2678 current=2551 products=2551 history=127
> ```
>
> The restore ran on intact data (before run 3), so it shows that the procedure works, not that lost rows came back.
>
> • Lesson: dbt creates its tables and snapshots on Snowflake as TRANSIENT, and a transient table cannot be cloned into a
> permanent one. Trying it fails with "Transient object cannot be cloned to a permanent object."
> (evidence/10_transient_clone_error.txt)

Delete the image under 9.4 (the old `backup → prune_old` run, which made DIM_PRODUCTS_BKP_ tables) and its
"[Screenshot: backup DAG green + log]" line.

### Screenshots (optional; the log excerpts above are the evidence)

If you want images as well, take them from the runs named above, as in rerun-steps.md (G1, H1, I, K): the dev DAG graph
(12:29 AM), the year-end DAG graph (1:21 AM), the prototype graph (2:08 PM) and the restore graph (12:39 AM).
No run needs to be triggered.

### Comment reply / resolve

Reply to each of these threads, quoting its opening words (the thread numbers shift as comments get resolved):

- "[Claude] A small table here helps the grader match the four sub-items …": *Added a first bullet that maps
  9.1–9.4 to the DAGs (evidence/src/).*
- "[Claude] That version of the DAG (clone_prod_to_dev → add_rating → verify_isolation) …": *Replaced 9.1 with the current
  DAG and its run log (dbt in dev, checks, promotion) and removed the old screenshot. Evidence:
  evidence/91_dev_clone_dbt_logs.txt, evidence/91_dev_clone_logs.txt, evidence/91_dev_clone_untouched.txt.*
- "[Claude] Replace with what hw32_eoy_snapshot does …" and "[Claude] Two corrections. The 1-day cap …": *Replaced 9.2
  with the Jan 1 DAG, the price_yoy model, the transient/1-day reason and the 1:21 AM demo run. Evidence:
  evidence/92_eoy_logs.txt, evidence/92_price_yoy.txt.*
- "[Claude] Right idea, but no evidence yet. hw32_prototype …": *Added the tasks, the model and the 2:08 PM run's results
  (evidence/93_prototype_dbt_logs.txt, evidence/93_prototype_logs.txt).*
- "[Claude] Out of date. Now backup_name → backup_database → prune_old_backups …", "[Claude] Replace with the hw32_restore
  DAG …" and "[Claude] Stale: this is the old backup → prune_old DAG …": *Replaced 9.4 with the database-clone backup,
  the pruning test and the 12:39 AM restore run, and removed the old screenshot (evidence/94_backup_logs.txt,
  evidence/94_prune_test_fixtures.txt, evidence/94_restore_logs.txt).*
- "[Claude] True, and confirmed: cloning the transient snapshot …": *Lesson now quotes the error
  (evidence/10_transient_clone_error.txt).*

Resolve them only once item 9 passes in the next round's scored export. The would-be re-score in `iter/2/score.md`
is not enough on its own.
