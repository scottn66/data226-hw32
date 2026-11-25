# Comments on the HW 3.2 writeup

For the Google Doc "DATA 226 Group HW 3.2" (last edited Nov 23, 2:47 PM PT). Prepared Nov 24, 2026.

Each comment quotes the line of the doc it belongs to, says what is wrong or missing, and gives the facts
to put there. The wording is yours. Every number comes from Snowflake or the Airflow logs on Nov 24,
and every query below was run before it was included. Times are Pacific unless marked UTC.

## Where the points are

| Rubric item (pts) | What holds it back now | Comments |
|---|---|---|
| 3. Snowflake setup (1) | The DDL is still a placeholder | 4a |
| 4. Airflow DAGs (4) | Code snippet is a placeholder; error handling is claimed but not shown | 5a–5f |
| 5. dbt models (6) | No SQL in the doc; test list and screenshot are out of date | 6a–6d |
| 8. Run, verify, re-run (3) | Screenshot 07 shows no visible change; 05 and 06 are unreadable | 8a–8d |
| 9. Cloning (4) | 9.1 and 9.4 describe DAGs that no longer exist; the 9.2 note is wrong; 9.3 has no evidence | 9a–9.4e |
| 10. Takeaways (2) | One-liners with nothing tried or observed behind them | 10a–10b |
| 11. Misc (1) | Answers a different question | 11a |
| Required | GenAI disclosure is the template text | 12a |

Items 1, 2, 6 and 7 are mostly fine and need small updates (2a, 3a, 6d, 7a).

## Suggested order

1. Run the Time Travel query in 10a (item 2) first. It stops working at about 1 AM tonight.
2. Take the screenshots in the checklist at the end, about 30–45 minutes. Re-run `hw32_prototype` before
   screenshot I so its counts include run 3.
3. Edit the doc section by section.
4. Do the GenAI disclosure and the appendix last, and keep the private key and `.env` out of anything you attach.

---

## Header

**H1.** "Group 9 | Author: Scott Nelson"
The title says Group HW, but only one name is listed. Add the other Group 9 members, or say the group
agreed you would submit alone.

## 1. Overview

**1a.** "...transforming data with Goal (1–2 sentences): ELT pipeline that loads..."
Two drafts got merged, and the template label "Goal (1–2 sentences):" is still in the middle. Keep one sentence.

**1b.** "Architecture diagram: CSV → Snowflake stage → RAW.STG_PRODUCTS_RAW → ..."
The chain is missing the steps added on Nov 23–24. The current flow:
CSV (local) → `extract` (validate, PUT) → `@HW32.RAW.PRODUCTS_STAGE` → `load_staging` (COPY in one transaction)
→ `RAW.STG_PRODUCTS_RAW` → `stg_products` (view) → `products_snapshot` (SNAPSHOTS) → `dim_products`
(ANALYTICS, joined with `RAW.PRODUCT_RATINGS`) → `verify` → `clone_dev_database` (HW32_DEV).

**1c.** The diagram image. These parts are out of date:
- The `extract` box says "validate CSV, count rows". It now also uploads the file, so the PUT arrow
  should start at `extract`, not `load_staging`.
- The `load_staging` box says "PUT + COPY INTO in one transaction, rollback on mismatch". It now runs only the
  COPY, and rolls back on any error, not only a row-count mismatch.
- Add a fifth task box, `clone_dev_database`, after `verify`, with an arrow to an HW32_DEV box.
- Add `RAW.PRODUCT_RATINGS` feeding `dim_products`. The `dim_products` box should list the new columns:
  `version`, `change_type`, `previous_price`, `rating`.
- Footer, tests: add `scd2_matches_snapshot`, `rating_in_range` and `accepted_values(change_type)`.
- Footer, clones: replace "point-in-time AT(OFFSET)" with a year-end `AT(TIMESTAMP)` clone of
  `PRODUCTS_SNAPSHOT` plus `price_yoy`. Replace "daily DIM_PRODUCTS_BKP_YYYYMMDD" with a daily whole-database
  clone `HW32_BACKUP_YYYYMMDD` and a restore by schema swap.
- The header could also give the schedule: `@daily`, midnight Pacific.

The five other DAGs don't need to be in this picture; the table in 9a covers them.

Done: the redrawn diagram with all of these changes is `hw32/diagram/architecture.png`. The script that
draws it is `hw32/diagram/architecture.py`; edit the text there and run `python3 architecture.py`.

## 2. Environment setup

**2a.** "dbt-core 1.10 / dbt-snowflake 1.10 (Python 3.12), key-pair auth" and "Airflow 3 via Astro CLI + Docker, Cosmos for dbt"
Exact versions make item 1 easy to grade:
- Airflow 3.4.0 (Astro Runtime 3.4-1, Python 3.14), Cosmos 1.16.0, `apache-airflow-providers-snowflake`
  6.21.0, `apache-airflow-providers-common-sql` 3.0.0, `snowflake-connector-python` 5.2.0.
- dbt-core 1.10.23 with dbt-snowflake 1.10.8, each in its own Python 3.12 virtualenv: one inside the Airflow image
  (see `airflow/Dockerfile`) and one on your Mac (`hw32/.venv`, the one screenshot 01 shows).
- One Airflow connection, `snowflake_conn`, defined in `airflow/.env`, with key-pair auth. With connector 5.x
  it also needs `"authenticator": "SNOWFLAKE_JWT"`. Without that setting, every task failed at midnight on Nov 24.

Screenshot 01 can stay.

## 3. Dataset

**3a.** "Home Depot product data (Kaggle), 2,551 rows, 14 columns"
Say where the file is stored; the spec asks for S3 or a local filesystem (rubric item 2). Yours is local:
`airflow/include/data/home_depot_data_1_2021_12.csv`, which the container sees under
`/usr/local/airflow/include/data/`. The `extract` task uploads it to Snowflake's internal stage.
Also worth one line: three versions of the file drive the runs. They are `original.csv` (run 1),
`run2.csv` (run 2) and the current file (run 3).

**3b.** "Natural key: product_id (unique); notes: sku has nulls and duplicates"
Correct, and numbers make it stronger: 144 rows have no SKU, and 373 more repeat a SKU that another row
already has. Two other facts about the data matter later (sections 6 and 10):
- `availability` is "InStock" in all 2,551 rows. The snapshot checks that column, but it can never trigger a new version.
- `scraped_at` differs for every row, but all values fall within 10 minutes on Dec 14, 2021, and
  `mutate_source.py` never changes it. So the `timestamp` snapshot strategy from the spec would have missed
  every change. That is why the snapshot uses `check`.

## 4. Database, schema, staging

**4a.** "DDL snippet + row count 2,551" (rubric item 3)
Replace it with the DDL in `hw32/setup.sql`: the warehouse, the database, the RAW and ANALYTICS schemas, the
file format, the stage, and the landing table. Note that `dbt snapshot` creates the SNAPSHOTS schema. Then add
the count: `SELECT COUNT(*) FROM HW32.RAW.STG_PRODUCTS_RAW;` returns 2,551.
One detail worth a sentence: the landing table in use is the one Snowsight's load wizard created on Nov 20,
with 14 columns and price as FLOAT. It replaced a hand-written version, so the file now loads column for
column, and all type conversion happens in `stg_products`.

## 5. Airflow DAG

**5a.** "Tasks: extract → load_staging → dbt_transform (task group) → verify"
The chain is now extract → load_staging → dbt_transform → verify → clone_dev_database. Add the schedule:
daily at midnight Pacific (`@daily`, catchup off).

**5b.** What each task does now (the current screenshots were taken before these changes):
- `extract` checks that the CSV exists and has rows, then PUTs it (gzipped) to `@HW32.RAW.PRODUCTS_STAGE`.
  Before Nov 23 it only counted rows, which made the "extract" point thin.
- `load_staging` runs one transaction: DELETE, then COPY with `ON_ERROR='ABORT_STATEMENT'`, then a row-count
  check. Any error rolls the whole transaction back.
- `verify` fails the run unless every product has exactly one current row. Run 3 logged
  `dim rows=2805 current=2551 products=2551 history=254`.
- `clone_dev_database` creates the daily dev clone for 9.1.

**5c.** "Error handling: retries=2, 1-min delay, on_failure_callback, transactional load with row-count check and rollback" (worth 1 point)
This is claimed but never shown, and you have real evidence: the scheduled run for Nov 23, the red bar in
your DAG list. `extract` failed three times, a minute apart, with "The conn_id `snowflake_conn` isn't
defined". The callback then logged "HW32 pipeline FAILED at task extract (try 3)". A screenshot of that
log (E in the checklist) proves that both the retries and the callback work.

**5d.** Operators (the spec's "Use appropriate Airflow operators")
Worth one sentence on why the code differs from the spec's snippets. Snowflake provider 6.x no longer has
`SnowflakeOperator`, so plain SQL steps use `SQLExecuteQueryOperator` and checks use `SQLCheckOperator`,
both from the common-sql provider. The other pieces:
- Python steps use TaskFlow `@task` with `SnowflakeHook`.
- The restore DAG uses `TriggerDagRunOperator`.
- dbt runs through Cosmos.

**5e.** "Code snippet: hw32_products_pipeline.py (key parts)" (rubric item 4)
Suggested excerpt, from the file as it is now:

```python
@dag(start_date=pendulum.datetime(2026, 11, 1, tz=TZ), schedule="@daily", catchup=False,
     tags=["hw32"],
     default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                   "on_failure_callback": notify_failure})
def hw32_products_pipeline():
    ...
    @task
    def load_staging(expected: int) -> int:
        """Replace the RAW table in one transaction; roll back on any error or row-count mismatch."""
        with SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn() as conn:
            cur = conn.cursor()
            cur.execute("BEGIN")
            try:
                cur.execute(f"DELETE FROM {RAW_TABLE}")
                cur.execute(f"COPY INTO {RAW_TABLE} FROM @{STAGE} FILES=('{STAGED_FILE}') "
                            f"FILE_FORMAT=(FORMAT_NAME={FILE_FORMAT}) "
                            "FORCE=TRUE ON_ERROR='ABORT_STATEMENT'")
                loaded = cur.execute(f"SELECT COUNT(*) FROM {RAW_TABLE}").fetchone()[0]
                if loaded != expected:
                    raise ValueError(f"loaded {loaded} rows, expected {expected}")
            except Exception:
                cur.execute("ROLLBACK")
                raise
            cur.execute("COMMIT")
        logging.info("staging rows: %s", loaded)
        return loaded

    dbt_transform = dbt_group("dbt_transform")   # Cosmos DbtTaskGroup, one task per dbt node
    ...
    load_staging(extract()) >> dbt_transform >> verify() >> clone_dev_database
```

**5f.** "[Screenshot 04_airflow_dbt_task_group_success.png]"
It shows run 1 with `verify` still running. Retake it from a finished run (B in the checklist).

## 6. dbt models, SCD2, tests

**6a.** "dim_products: product_sk, valid_from, valid_to, is_current" (the 2-point core model)
The model now does the SCD2 work itself. That is what keeps it from looking like a copy of the snapshot,
which was the main risk to these 2 points:
- `products_snapshot` records that something changed: dbt's MERGE writes `dbt_valid_from` and `dbt_valid_to`.
- `dim_products` builds the dimension from those rows with window functions:
  - `version` with ROW_NUMBER;
  - `previous_price` and `previous_title` with LAG;
  - `valid_to`, the start of the next version (LEAD), or 9999-12-31 for the current row;
  - `is_current`;
  - `change_type`: initial, price_change, title_change or other_change;
  - `rating`, the 9.1 feature.
- `scd2_matches_snapshot`, a singular test, checks that the windows it computes agree with dbt's
  `dbt_valid_to` for every row.

Suggested excerpt, the core of `models/marts/dim_products.sql`:

```sql
windowed as (
    select
        *,
        row_number()     over (partition by product_id order by valid_from) as version,
        lag(price)       over (partition by product_id order by valid_from) as previous_price,
        lag(title)       over (partition by product_id order by valid_from) as previous_title,
        lead(valid_from) over (partition by product_id order by valid_from) as next_valid_from
    from versions
)
...
    coalesce(w.next_valid_from, '9999-12-31'::timestamp_ntz)  as valid_to,
    w.next_valid_from is null                                 as is_current
```

**6b.** "Tests: unique/not_null + singular tests one_current_row_per_product, no_overlapping_validity" (the 2-point tests item)
There are 12 tests now:
- **Generic tests**, in the `schema.yml` files:
  - `stg_products.product_id`: unique and not_null;
  - `stg_products.price`: not_null;
  - `dim_products.product_sk`: unique and not_null;
  - `dim_products.product_id` and `dim_products.version`: not_null;
  - `dim_products.change_type`: accepted_values.
- **Singular tests**, in `tests/`: `one_current_row_per_product`, `no_overlapping_validity`,
  `scd2_matches_snapshot` and `rating_in_range`.

`price_yoy` and `price_bands` have their own tests, which run only inside their DAGs.

**6c.** "[Screenshot 02_dbt_build_pass11.png]"
This is out of date: it ran 8 tests, and there are 12 now. Retake it with `dbt build` (A in the checklist).
It should end with PASS=15 TOTAL=15 (2 models, 1 snapshot, 12 tests). Your local profile's `dev` target is
HW32, so this rebuilds the prod tables exactly as the pipeline does. That's harmless right now, because the
source hasn't changed since run 3.

**6d.** "products_snapshot: check strategy on title, price, availability, brand"
This is fine. Optionally, add why the strategy is `check` and not `timestamp` (see 3b).

## 7. dbt operator in Airflow

**7a.** "[Screenshot 03_airflow_cosmos_operators_task_list.png]"
Good evidence, but it was taken mid-run, with most tasks at "No Status". Retake it from run 3
(C in the checklist). The Operator column will show `DbtRunLocalOperator`, `DbtTestLocalOperator` and
`DbtSnapshotLocalOperator`, plus the new `scd2_matches_snapshot_test` task and `clone_dev_database`
(`SQLExecuteQueryOperator`).

Two sentences worth adding:
- Why Cosmos instead of the spec's `BashOperator` running `dbt run`: each model becomes its own Airflow task,
  so each one can retry and shows its own log.
- How test placement works: `scd2_matches_snapshot` reads two models. By default it would run right after the
  snapshot, against a stale `dim_products`, so the DAG sets `should_detach_multiple_parents_tests=True`,
  which runs it after both.

## 8. Run, verify, change and re-run

**8a.** "Run 1: dim rows 2551, current 2551 ... Run 2: dim rows 2678, current 2551, history 127" (rubric item 8)
There are three runs now:

| Run | Airflow run (PT) | Source file | Changes | Snapshot rows | Current |
|---|---|---|---|---|---|
| 1 | Nov 22, 7:37 PM | `original.csv` | none | 2,551 | 2,551 |
| 2 | Nov 22, 7:40 PM | `run2.csv` | 127: 91 price, 36 title | 2,678 | 2,551 |
| 3 | Nov 24, 1:20 AM | current file | 127: 83 price, 44 title (6 of these products also changed in run 2) | 2,805 | 2,551 |

After run 3, `dim_products` has 2,551 version-1 rows, 248 version-2 rows (168 price changes, 80 title
changes) and 6 version-3 rows.

Two clarifications to add:
- The version-1 rows start at 6:43 PM on Nov 22. That is when your local `dbt build` (screenshot 02)
  first created the snapshot.
- Snapshot timestamps are in UTC. For example, run 2 shows as 2026-11-23 03:40 UTC.

**8b.** "mutate_source.py changes 5% of rows (price ±20% or title edit)"
Accurate. Add that each run's file is kept (3a), and that run 3 used the same script again.

**8c.** Screenshots 05 and 06
The key number is unreadable at page size. Crop both down to the `verify` log line; 06 already highlights
`dim rows=2678 current=2551 products=2551 history=127`. Add the same crop for run 3
(D in the checklist): `dim rows=2805 current=2551 products=2551 history=254`.

**8d.** Screenshot 07
It shows product 204877843 twice with the same title and price, because the " (updated)" suffix is cut off,
so it reads like a duplicate row. Replace it with product 205235421, which now has three versions
(F in the checklist):

```sql
SELECT product_id, version, change_type, previous_price, price, valid_from, valid_to, is_current
FROM HW32.ANALYTICS.DIM_PRODUCTS
WHERE product_id = 205235421
ORDER BY version;
```

The result shows the price going $264.00 → $232.31 (run 2) → $269.35 (run 3). Each `valid_to` equals the next
row's `valid_from`, and only the last row is current.

## 9. Zero-copy cloning

**9a.** New, at the start of the section: a short table helps the grader match the four sub-items to DAGs.

| Sub-item | DAG | Schedule | What it clones |
|---|---|---|---|
| 9.1 | `clone_dev_database` task in `hw32_products_pipeline`; `hw32_dev_clone_rating` | daily; manual | HW32 → HW32_DEV |
| 9.2 | `hw32_eoy_snapshot` | midnight, Jan 1 | `PRODUCTS_SNAPSHOT`, as of a timestamp |
| 9.3 | `hw32_prototype` | manual | ANALYTICS → PROTOTYPE schema |
| 9.4 | `hw32_daily_backup`; `hw32_restore` | daily; manual | HW32 → `HW32_BACKUP_YYYYMMDD`; restore brings back the SNAPSHOTS schema |

### 9.1 Dev environment (1 pt)

**9.1a.** "HW32_DEV clone + RATING column; prod unchanged; DAG hw32_dev_clone_rating", and the screenshot of
`clone_prod_to_dev → add_rating → verify_isolation`
That version of the DAG no longer exists, so the screenshot is out of date. The spec asks for four things,
and all four are now in place:
1. **A daily clone.** `clone_dev_database` runs at the end of the main pipeline:
   `CREATE OR REPLACE DATABASE HW32_DEV CLONE HW32`.
2. **A new feature in dev.** `land_ratings_in_dev` creates `HW32_DEV.RAW.PRODUCT_RATINGS`.
   - The Kaggle data has no ratings, so they are simulated: values from 1.0 to 5.0, derived from a hash of
     `product_id`, so they come out the same every run. Say so in the doc.
   - `dim_products` gets a `rating` column only when the dbt variable `enable_rating` is true.
3. **A DAG that runs dbt in dev.** `hw32_dev_clone_rating` runs the dbt task group with the `dev` target
   (database HW32_DEV). The sources use `{{ target.database }}`, so a dev run reads the clone's RAW tables,
   not prod's.
4. **Test, then promote.**
   - `check_dev_feature` checks that rows exist, no rating is null, and all ratings are between 1 and 5.
     It returned `(True, True, True)`.
   - `check_prod_untouched` compares a fingerprint (`HASH_AGG`) of prod `DIM_PRODUCTS` from before and after
     the dev run. It was 2728168932473494028 both times.
   - `promote_ratings_to_prod` clones the ratings table into `HW32.RAW`.
   - Then `enable_rating: true` was set in `dbt_project.yml`, and the next prod run added the column:
     all 2,551 current products are rated, with an average of 2.99.

Screenshots G1 and G2 in the checklist cover this.

### 9.2 Point-in-time clone (1 pt)

**9.2a.** "AT(OFFSET => -1800) on PRODUCTS_SNAPSHOT; counts before/after"
Replace it with what `hw32_eoy_snapshot` does:
- **Schedule.** The DAG runs at midnight on Jan 1, Pacific time.
- **`freeze_snapshot`** clones `HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT` as it was at year end
  (`AT(TIMESTAMP => ...)`) into `PRODUCTS_SNAPSHOT_EOY_<year>`, and points the view
  `PRODUCTS_SNAPSHOT_FROZEN` at that clone.
- **`dbt_price_yoy`** builds `ANALYTICS.PRICE_YOY`. It compares each product's frozen price with today's and
  labels it added, removed, repriced, retitled or unchanged. This is the spec's "dbt model that performs
  year-over-year analysis".
- **The `as_of` parameter** allows a demo before Jan 1. The demo run froze the table as of Nov 24 at 1:18 AM,
  just before run 3:
  - the clone `PRODUCTS_SNAPSHOT_PIT_20261124_0118` has 2,678 rows, against 2,805 in the live table;
  - `price_yoy` found 83 repriced products (−19.45% to +19.67%, average +1.63%), 44 retitled and
    2,424 unchanged, which are exactly run 3's changes.

**9.2b.** "Note: trial Time Travel is 1 day max (transient tables), so a 1-year-back clone isn't possible"
Two corrections:
- **The 1-day cap comes from the table type, not the trial.** The table is TRANSIENT, which is dbt's default
  for tables and snapshots on Snowflake. Transient tables are capped at 1 day on every edition; the edition
  only matters for permanent tables.
- **Why one table and not the whole database, as in the spec.** dbt rebuilds every ANALYTICS table with
  CREATE OR REPLACE on each run, which starts its Time Travel history over. The snapshot is merged in place,
  so its history is continuous. A 2025 year-end clone is impossible either way, because these objects were
  created on Nov 20–22, 2026.

There is also a lesson worth keeping, which fits section 10: the window to freeze the state from before run 2
was missed, because after 24 hours it was gone. That's why the DAG runs at the year boundary instead of
reconstructing the state later.

Screenshots H1 and H2 in the checklist cover this.

### 9.3 Prototype schema (1 pt)

**9.3a.** "Clone ANALYTICS → PROTOTYPE, build PRICE_BANDS, drop schema"
The idea is right, but there is no evidence yet. What `hw32_prototype` does:
1. **`clone_analytics_schema`** runs `CREATE OR REPLACE SCHEMA HW32.PROTOTYPE CLONE HW32.ANALYTICS`.
2. **`dbt_prototype`** builds `price_bands` with the `proto` target, so it reads the cloned `dim_products` and
   never touches prod. This model is the spec's "complex transformation". It sorts the current catalog into
   price bands and reports, for each band:
   - the median price;
   - its share of the catalog (`RATIO_TO_REPORT`);
   - how many products were ever repriced, taken from the SCD2 history.
3. **`show_results`** prints the table.
4. **`drop_prototype_schema`** drops the schema even if something failed (trigger rule `all_done`).
5. **`fail_if_prototype_failed`** then marks the run failed if anything did. On a successful run it is
   skipped, which is the pink "skipped" task in your DAG list.

Result from your Nov 24, 2:08 PM rerun, which includes run 3 (use this run for screenshot I):

| Band | Products | Brands | Median price | Share of catalog | Changed | Repriced |
|---|---|---|---|---|---|---|
| under $25 | 532 | 211 | $15.98 | 20.9% | 59 | 42 (7.9%) |
| $25–99 | 930 | 351 | $46.98 | 36.5% | 91 | 59 (6.3%) |
| $100–499 | 834 | 304 | $187.08 | 32.7% | 81 | 58 (7.0%) |
| $500+ | 255 | 123 | $919.29 | 10.0% | 17 | 9 (3.5%) |

### 9.4 Backup and restore (1 pt)

**9.4a.** "DAG hw32_daily_backup: daily clone DIM_PRODUCTS_BKP_YYYYMMDD, prune older than 7 days", and its
screenshot (`backup → prune_old`)
Both are out of date. Now:
- **Tasks:** `backup_name` → `backup_database` → `prune_old_backups`, daily at midnight Pacific.
- **Each backup clones the whole database**, at no storage cost until the data diverges:
  `CREATE DATABASE IF NOT EXISTS HW32_BACKUP_<yyyymmdd> CLONE HW32`. The IF NOT EXISTS means a second run on
  the same day can't overwrite a good backup with later, possibly bad, data.
- **The name comes from the run's Pacific date**, not from `{{ ds_nodash }}` as in the spec, for two reasons:
  - Airflow 3 manual runs have no logical date, so `ds` is undefined. That is why the backup run at 12:33 AM
    on Nov 24 failed.
  - `ds` is a UTC date anyway.
- **Pruning keeps 7 days**: today and the 6 days before. The old version kept 8.

**9.4b.** Evidence for pruning
I tested it on Nov 24 with two fake backups. It dropped `HW32_BACKUP_20261116` and kept
`HW32_BACKUP_20261118`, which sits on the 7-day boundary. That run (12:36 AM) also hit a one-off Airflow race:
it started on the previous version of the DAG file. So it doesn't make a clean screenshot; repeat the test
yourself for J in the checklist.

**9.4c.** "Restore: CREATE OR REPLACE TRANSIENT TABLE HW32.ANALYTICS.DIM_PRODUCTS CLONE ..."
Replace it with the `hw32_restore` DAG (manual trigger only):
1. **`pick_backup`** picks the newest `HW32_BACKUP_*`, or a date passed as a parameter.
2. **`restore_snapshots`**:
   - clones that backup's SNAPSHOTS schema into `HW32.SNAPSHOTS_RESTORE` and checks it isn't empty;
   - swaps it with `HW32.SNAPSHOTS` in one atomic `ALTER SCHEMA ... SWAP WITH`;
   - keeps the old schema as `SNAPSHOTS_PRE_RESTORE_<time>`.
3. **`rerun_pipeline`** triggers `hw32_products_pipeline` and waits for it to finish.

Why only the SNAPSHOTS schema: RAW is reloaded from the source on every run, and dbt rebuilds ANALYTICS from
the snapshot. The snapshot's history is the only data nothing else can recreate.

Result on Nov 24 at 12:39 AM: "restored 2678 snapshot rows from HW32_BACKUP_20261124; previous schema kept
as SNAPSHOTS_PRE_RESTORE_20261124_003926". The triggered pipeline run then rebuilt `dim_products`
(K in the checklist).

**9.4d.** Optional, stronger demo (your call)
The restore ran on intact data. That proves the steps work, but nothing visibly comes back. To show a real
recovery:
1. Wait for tonight's backup, `HW32_BACKUP_20261125`. If your Mac is asleep at midnight, the scheduled run
   won't happen; trigger `hw32_daily_backup` by hand instead.
2. In Snowsight, run `DELETE FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT WHERE dbt_valid_to IS NOT NULL;`.
   This removes the 254 history rows.
3. Trigger `hw32_restore`.
4. Show that the count is back to 2,805.

Don't use today's `HW32_BACKUP_20261124`: it was taken before run 3 and before the 9.2 freeze, so restoring
from it would roll both back.

**9.4e.** "Lesson: dbt tables are transient by default, so clones must be TRANSIENT"
True, and I confirmed it. Cloning the transient snapshot into a permanent table fails with "Transient object
cannot be cloned to a permanent object." State it that precisely; it can also move to section 10.

## 10. Takeaways and cost (2 pts)

**10a.** The five one-line takeaways
Each needs two or three sentences: what you tried, what you saw, and what it means. You have more material
than you need, so pick five. Everything below was tested.

1. **Zero-copy clones are copy-on-write.** A clone shares its source's storage until one side changes, and
   only the changes cost anything. Evidence (L in the checklist):
   ```sql
   SELECT table_catalog, table_schema, active_bytes, time_travel_bytes
   FROM HW32.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
   WHERE table_name = 'PRODUCTS_SNAPSHOT'
     AND table_dropped IS NULL AND schema_dropped IS NULL AND catalog_dropped IS NULL
   ORDER BY 1, 2;
   ```
   On Nov 24 the backup and the dev clone owned 0 bytes. Prod owned 1.15 MB that run 3 wrote, plus 1.1 MB of
   older versions kept for Time Travel.
2. **Time Travel depends on the table type and on how dbt writes.**
   - Transient tables keep only 1 day of history.
   - CREATE OR REPLACE starts the history over.
   - The state from before run 2 was lost after a day.

   This query only works until about 1 AM tonight:
   ```sql
   SELECT (SELECT COUNT(*) FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT
           AT(TIMESTAMP => '2026-11-24 01:15:00 -08:00'::TIMESTAMP_TZ)) AS before_run3,
          (SELECT COUNT(*) FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT) AS now_rows;
   -- 2678 and 2805
   ```
3. **The data exposes SCD2 edge cases.**
   - The `check` strategy was required (see 3b).
   - `availability` never changes.
   - A product deleted from the source stays current forever, because the snapshot uses dbt's default for
     hard deletes (deleted rows are ignored). Since dbt 1.9, `hard_deletes='invalidate'` closes such rows.
4. **Airflow 3 breaks several of the spec's starter snippets.**
   - `SnowflakeOperator` is gone.
   - `schedule_interval` and `{{ execution_date }}` were removed.
   - Manual runs have no `ds`.
   - A run started right after the DAG file was edited ran on the old version of the DAG.
5. **Retries help with temporary errors, not configuration errors.** Two runs show this:
   - the Nov 23 run retried a missing connection twice before failing;
   - the Nov 24 midnight runs did the same for the missing key-pair setting.

   Retries only delay a real failure by about two minutes.
6. **Cosmos turns each dbt node into an Airflow task.** That gives each model its own retries and its own
   log. The catch: a test that reads two models needs `should_detach_multiple_parents_tests=True` to run
   after both of them.
7. **Idle warehouse time cost more than the pipeline.** See 10b.

**10b.** Item 5, "Cost: ~0.156 credits total, ~0.133 tonight; X-Small, 60s auto-suspend; per-run estimate ~0.03–0.05 credits"
These numbers are out of date. From `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` on Nov 24 (this view
lags by up to about 3 hours):
- **HW32_WH** (pipeline, dbt and clones; auto-suspend 60 s) used 0.600 credits from Nov 20 to 24. The
  biggest hour was midnight to 1 AM on Nov 24 (0.27), when all the cloning DAGs ran.
- **COMPUTE_WH** (Snowsight: worksheets, loading data, browsing; auto-suspend 300 s) used 1.076 credits,
  all by Nov 22.
- **What it means:** interactive use cost almost twice as much as the pipeline, even though its queries
  added up to only a few minutes. A warehouse is billed for every second it is running, with a one-minute
  minimum each time it starts, and it keeps running until auto-suspend stops it: 5 minutes on COMPUTE_WH
  versus 1 minute on HW32_WH.

Re-run the query when you write this section (M in the checklist). The current screenshot shows only four
hourly rows from Nov 20 and Nov 22. Keep the per-run estimate only if you can show where it comes from.

## 11. Extension idea (1 pt)

**11a.** "Incremental loads: external stage + Snowpipe or Stream/Task, dbt incremental model; backups as dbt post-hook"
The question asks how you would extend the exercise to make the learning experience better. That means
changes to the assignment, not to your pipeline. Ideas based on what actually happened; pick one or two:
- Have the mutation script also add and delete rows, so students must handle new products and hard deletes,
  which the default snapshot ignores.
- Make 9.2 time-boxed: freeze a point in time, change the data, then compare. That teaches why Time Travel
  limits matter; this project missed one window.
- Add a "break it" step to 9.4: delete the snapshot history, then restore it from backup.
- Update the starter snippets for Airflow 3. Several don't run as written: `SnowflakeOperator`,
  `schedule_interval` and `execution_date` were all removed.

Your incremental-load idea can stay as a second point if you frame it as a step in the assignment.

## 12. GenAI disclosure

**12a.** "Tools used, what they helped with (setup troubleshooting, code drafts, writeup structure), and what the author verified/ran personally."
This is still the template text, word for word. The spec says full points need "your own workflow and ...
your own code", so a specific disclosure is much safer than a general one. Facts from this Claude Code
session (Nov 23–24) that belong in it:
- Claude Code (Anthropic) reviewed this writeup against the spec and the code.
- It wrote or rewrote these files:
  - the six DAG files and `hw32_common.py`;
  - `dim_products.sql`, `price_yoy.sql` and `price_bands.sql`;
  - the tests `scd2_matches_snapshot` and `rating_in_range`;
  - `sources.yml` and the `schema.yml` files;
  - the targets in `include/profiles.yml` and the variables in `dbt_project.yml`;
  - the Airflow connection in `airflow/.env`;
  - the README files;
  - `setup.sql`, which it recovered from Snowflake's query history.
- On Nov 24 it ran the DAGs, the run-3 mutation and the verification queries against Snowflake, and fixed
  the bugs those runs exposed.
- It drafted these comments.

What only you can add:
- what you built or decided before Nov 23: for example runs 1 and 2, the screenshots and the diagram;
- any other AI tools you used;
- what you checked yourself.

One thing I noticed: Snowflake's query history has a query tagged `cortex_code_sandbox` on Nov 20 at
12:48 AM. The tag appears to come from Snowflake's AI coding tool, Cortex Code; if you used it, list it too. Whatever
the disclosure says, make sure you can explain each DAG and model in your own words.

## Appendix

**A1.** "Repo/file listing; full DAG and dbt code"
A trimmed listing, leaving out virtualenvs, logs and build output:

```
hw32/
  README.md                       maps each DAG to its rubric items
  setup.sql                       one-time Snowflake setup (section 4)
  airflow/                        Astro project
    Dockerfile                    Astro Runtime 3.4-1, plus dbt in its own virtualenv
    requirements.txt
    dags/
      hw32_products_pipeline.py   main pipeline (items 4, 7, 8; the daily 9.1 clone)
      hw32_dev_clone_rating.py    9.1
      hw32_eoy_snapshot.py        9.2
      hw32_prototype.py           9.3
      hw32_daily_backup.py        9.4 backup
      hw32_restore.py             9.4 restore
      hw32_common.py              shared settings and helpers
      dbt/hw32_dbt/               dbt project: models, snapshot, tests
    include/
      data/                       original.csv, run2.csv, current source file
      mutate_source.py            source changes for runs 2 and 3
      profiles.yml                dbt targets: prod, dev, proto
    tests/dags/test_dag_example.py
  hw32_dbt/                       copy of the dbt project for the local dbt CLI
```

- Pasting six DAGs and five models makes a very long appendix. A link to a repository, or a zip shared with
  the grader, is easier to read.
- Either way, leave out `airflow/.env` (it holds your account and login) and `airflow/include/rsa_key.p8`
  (your private key). Both are already in `airflow/.gitignore`, so a git repository skips them
  automatically; a zip does not.
- `airflow/dags/exampledag.py` is the Astro template file. Airflow ignores it, so delete it or leave it out
  of the listing.

---

## Screenshot checklist

Airflow is at http://localhost:8080, and the UI shows times in Pacific time. Runs are identified by the start
times shown in your DAG list.

| # | For | What to capture | Where |
|---|---|---|---|
| A | 6c | `dbt build` ending in PASS=15 TOTAL=15 | Terminal: `cd ~/Desktop/226/hw32/hw32_dbt && ../.venv/bin/dbt build` |
| B | 5f | Graph of a finished main run, all 10 tasks green | `hw32_products_pipeline` → run of Nov 24, 1:20 AM → Graph |
| C | 7a | Task list with the Operator column | Same run → Task Instances |
| D | 8c | `verify` log line ending `history=254` | Same run → `verify` → Logs (crop) |
| E | 5c | Retries and the failure callback | Nov 23 run (red bar) → `extract` → Logs, try 3: the error and "HW32 pipeline FAILED..." |
| F | 8d | Three versions of product 205235421 | Snowsight, query in 8d |
| G1 | 9.1 | Dev DAG graph (12 tasks) and the `check_prod_untouched` log | `hw32_dev_clone_rating` → run of Nov 24, 12:29 AM |
| G2 | 9.1 | Prod has the rating | `SELECT COUNT(*), COUNT(rating), ROUND(AVG(rating), 2) FROM HW32.ANALYTICS.DIM_PRODUCTS WHERE is_current;` returns 2551, 2551, 2.99 |
| H1 | 9.2 | Year-end DAG graph and the `freeze_snapshot` log ("2678 snapshot rows then, 2805 now") | `hw32_eoy_snapshot` → run of Nov 24, 1:21 AM |
| H2 | 9.2 | Year-over-year summary | `SELECT compared_to, status, COUNT(*) FROM HW32.ANALYTICS.PRICE_YOY GROUP BY ALL ORDER BY 2;` |
| I | 9.3 | Prototype graph and the `show_results` log | Trigger `hw32_prototype` again, then open its graph |
| J | 9.4 | Pruning drops an old backup | Run `CREATE DATABASE HW32_BACKUP_20261110 CLONE HW32;`, trigger `hw32_daily_backup`, then `prune_old_backups` → Logs ("dropped HW32_BACKUP_20261110") |
| K | 9.4 | Restore graph, the `restore_snapshots` log, and `SHOW SCHEMAS IN DATABASE HW32;` | `hw32_restore` → run of Nov 24, 12:39 AM |
| L | 10a | Zero-copy storage query | Query in 10a, item 1 |
| M | 10b | Credits by warehouse | `SELECT warehouse_name, ROUND(SUM(credits_used), 3) FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE start_time >= '2026-11-19' GROUP BY 1;` |
| — | 1c | Updated architecture diagram | Redraw |

Keep screenshot 01. Keep 05 and 06 too, cropped (8c).

## Things I could not check

- Who is in Group 9, what was built before Nov 23, and which AI tools were used before then.
- Whether tonight's midnight backup runs. It needs your Mac awake and Docker running.
