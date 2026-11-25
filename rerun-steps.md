# Reruns and screenshots for HW 3.2

Written Nov 24, 2026, 6:40 PM PT. Every query result and run below was checked at that time.

Most screenshots don't need a rerun: the runs already exist in Airflow, and you only need to open
them and capture the screen. Only three steps actually run something: A (`dbt build`), J (the prune
test) and the optional restore demo (K2).

## 0. Before you start

**Airflow.** Docker Desktop must be running. Check in a terminal:

```bash
docker ps --format '{{.Names}} {{.Status}}' | grep airflow
```

You should see five `airflow_6c8d47-...` containers marked "Up". If not, open Docker Desktop and wait
a minute; if they still don't appear:

```bash
cd ~/Desktop/226/hw32/airflow && astro dev start
```

Then open http://localhost:8080. Local Astro has no login.

**Finding a run.** Dags → click the DAG name → **Runs** tab → click the run. The UI shows times in
Pacific (the clock icon in the sidebar says -08:00), but run IDs are in UTC. This table maps them:

| DAG | Run (Pacific) | Run ID (UTC) | Use for |
|---|---|---|---|
| `hw32_products_pipeline` | Nov 23, 3:06 PM, failed | `scheduled__2026-11-23T08:00:00+00:00` | E |
| `hw32_products_pipeline` | Nov 24, 1:20 AM (run 3) | `manual__2026-11-24T09:20:07...` | D (run 3) |
| `hw32_products_pipeline` | Nov 24, 6:18 PM (rerun, same file) | `manual__2026-11-25T02:18:06...` | B, C, D (already in the doc as 04) |
| `hw32_dev_clone_rating` | Nov 24, 12:29 AM | `manual__2026-11-24T08:29:21...` | G1 |
| `hw32_eoy_snapshot` | Nov 24, 1:21 AM | `manual__2026-11-24T09:21:09...` | H1 |
| `hw32_prototype` | Nov 24, 2:08 PM | `manual__2026-11-24T22:08:29...` | I |
| `hw32_restore` | Nov 24, 12:39 AM | `manual__2026-11-24T08:39:24...` | K |

**Reading a log.** In a run, click a task box in the Graph (or its name under Task Instances), then
the **Logs** tab. A task that retried has one log per try; pick the try in the log view's attempt
selector.

**Triggering a DAG.** Open the DAG and click **Trigger** (top right). A dialog opens. For DAGs with
parameters (`hw32_restore`, `hw32_eoy_snapshot`) it shows a form; leave the fields as described below.
Click **Trigger** in the dialog. The run appears in the Grid within a few seconds.

**Snowsight.** Use role ACCOUNTADMIN. In each worksheet, set the warehouse (top right) to **HW32_WH**
instead of COMPUTE_WH: it suspends after 60 seconds instead of 300, which is the point of takeaway 7.

**Capturing and replacing images.** Cmd+Ctrl+Shift+4 and drag a box: the crop goes to the clipboard
and you can paste it straight into the doc. (Cmd+Shift+4 saves a file to the Desktop instead.) To swap an
old screenshot: right-click it in the doc → **Replace image** → **Upload from computer** (or paste).

## 1. Do first: the Time Travel query (before 1 AM)

Section 10, takeaway 2. It stops working at about 1:15 AM tonight, when the 1-day window passes run 3.

```sql
SELECT (SELECT COUNT(*) FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT
        AT(TIMESTAMP => '2026-11-24 01:15:00 -08:00'::TIMESTAMP_TZ)) AS before_run3,
       (SELECT COUNT(*) FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT) AS now_rows;
```

Expected: `2678 | 2805`. Screenshot the query and result together.

## 2. Screenshots that need a run

### A. `dbt build` (section 6, replaces 02)

```bash
cd ~/Desktop/226/hw32/hw32_dbt
source ../.venv/bin/activate
dbt build
```

- Takes under a minute (the old screenshot shows 18 seconds for 11 nodes). The last lines should read `Completed successfully` and
  `Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15` (2 models, 1 snapshot, 12 tests).
- Screenshot from the first `START` line to the `Done.` line. If it doesn't fit, take the last 20 lines;
  the PASS/TOTAL line is what matters.
- What it touches: your local profile writes to HW32, the same tables the pipeline uses. It is safe now
  because the source file hasn't changed since run 3: the snapshot finds nothing new and stays at 2,805 rows.
- `deactivate` when done.

### J. Pruning (section 9.4)

The 2:10 PM backup run dropped nothing, because no backup was older than 7 days. To show pruning work:

1. In Snowsight, create a fake old backup (a zero-copy clone, so it costs nothing):
   ```sql
   CREATE DATABASE HW32_BACKUP_20261110 CLONE HW32;
   ```
2. In Airflow, trigger `hw32_daily_backup`. It finishes in under a minute.
3. Open the run → `prune_old_backups` → Logs. Screenshot these two lines:
   - `dropped HW32_BACKUP_20261110`
   - `backups kept (>= 20261118): ['HW32_BACKUP_20261124']`

   (If you do this after midnight, the run also creates `HW32_BACKUP_20261125`, and the second line
   says `>= 20261119` and lists both backups.)
4. Also screenshot the run's Graph (`backup_name → backup_database → prune_old_backups`, all green). It
   replaces the old `backup → prune_old` screenshot in 9.4.
5. Optional proof in Snowsight: `SHOW DATABASES LIKE 'HW32_BACKUP%';` no longer lists 20261110.

## 3. Screenshots from runs that already exist

| # | Section | Open | Capture |
|---|---|---|---|
| B | 5 | Done: 04 in the doc is the 6:18 PM run, all 10 tasks green | Nothing to do |
| C | 7 | Same 6:18 PM run, Task Instances tab | Crop the task list with the Operator column. It replaces 03, the mid-run screenshot |
| D | 8 | 1:20 AM run → `verify` → Logs | Crop to `dim rows=2805 current=2551 products=2551 history=254` |
| E | 5 | Nov 23 failed run → `extract` → Logs, try 3 | The error "The conn_id `snowflake_conn` isn't defined" and the line "HW32 pipeline FAILED at task extract (try 3)" |
| G1 | 9.1 | `hw32_dev_clone_rating`, 12:29 AM run | Graph (12 tasks, green), then `check_prod_untouched` → Logs: the line with the same fingerprint before and after |
| H1 | 9.2 | `hw32_eoy_snapshot`, 1:21 AM run | Graph, then `freeze_snapshot` → Logs: "2678 snapshot rows then, 2805 now" |
| I | 9.3 | `hw32_prototype`, 2:08 PM run | Graph (the pink `fail_if_prototype_failed` is skipped on success; that is expected), then `show_results` → Logs: the "Operator output is:" line |
| K | 9.4 | `hw32_restore`, 12:39 AM run | Graph, then `restore_snapshots` → Logs: "restored 2678 snapshot rows from HW32_BACKUP_20261124; previous schema kept as SNAPSHOTS_PRE_RESTORE_20261124_003926" |

The 2:08 PM prototype run includes run 3, so the 9.3 table changes to:

| Band | Products | Brands | Median price | Share of catalog | Changed | Repriced |
|---|---|---|---|---|---|---|
| under $25 | 532 | 211 | $15.98 | 20.9% | 59 | 42 (7.9%) |
| $25–99 | 930 | 351 | $46.98 | 36.5% | 91 | 59 (6.3%) |
| $100–499 | 834 | 304 | $187.08 | 32.7% | 81 | 58 (7.0%) |
| $500+ | 255 | 123 | $919.29 | 10.0% | 17 | 9 (3.5%) |

The Changed column adds up to 248 and Repriced to 168, which match `dim_products`.

## 4. Snowsight screenshots (queries only, nothing runs)

Each was run at 6:33 PM and returned the result shown.

**F (section 8, replaces 07):**
```sql
SELECT product_id, version, change_type, previous_price, price, valid_from, valid_to, is_current
FROM HW32.ANALYTICS.DIM_PRODUCTS WHERE product_id = 205235421 ORDER BY version;
```
Three rows: $264.00 → $232.31 → $269.35. Each `valid_to` equals the next `valid_from`, and only version 3 is current.

**G2 (9.1):**
```sql
SELECT COUNT(*), COUNT(rating), ROUND(AVG(rating), 2)
FROM HW32.ANALYTICS.DIM_PRODUCTS WHERE is_current;
```
`2551 | 2551 | 2.99`

**H2 (9.2):**
```sql
SELECT compared_to, status, COUNT(*) FROM HW32.ANALYTICS.PRICE_YOY GROUP BY ALL ORDER BY 2;
```
`PIT_20261124_0118`: repriced 83, retitled 44, unchanged 2424.

**K, second half (9.4):** `SHOW SCHEMAS IN DATABASE HW32;` lists `SNAPSHOTS` (created 12:39 AM by the
swap) and `SNAPSHOTS_PRE_RESTORE_20261124_003926`.

**L (takeaway 1):**
```sql
SELECT table_catalog, table_schema, active_bytes, time_travel_bytes
FROM HW32.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE table_name = 'PRODUCTS_SNAPSHOT'
  AND table_dropped IS NULL AND schema_dropped IS NULL AND catalog_dropped IS NULL
ORDER BY 1, 2;
```
The backup and the dev clone show 0 active bytes; prod shows about 1.15 MB.

**M (section 10, replaces the metering screenshot):**
```sql
SELECT warehouse_name, ROUND(SUM(credits_used), 3) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= '2026-11-19' GROUP BY 1 ORDER BY 2 DESC;
```
At 6:33 PM: COMPUTE_WH 1.076, HW32_WH 0.742, CLOUD_SERVICES_ONLY 0.002. The view lags by up to
about 3 hours, so a later run will show a little more for HW32_WH. Quote whatever your screenshot shows.

## 5. Optional: a real restore (K2), after midnight

Only if you want 9.4 to show data coming back rather than a restore of intact data.

1. **Wait for a backup taken after run 3.** At midnight the scheduler runs `hw32_products_pipeline` and
   `hw32_daily_backup`, as long as the Mac is awake and Docker is running. Check:
   `SHOW DATABASES LIKE 'HW32_BACKUP%';` must list `HW32_BACKUP_20261125`. If it doesn't, trigger
   `hw32_daily_backup` by hand after midnight.
2. **Break it.** In Snowsight:
   ```sql
   DELETE FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT WHERE dbt_valid_to IS NOT NULL;   -- removes 254 rows
   SELECT COUNT(*) FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT;                         -- 2551
   ```
   Screenshot the count.
3. **Restore.** Trigger `hw32_restore`, setting `backup_date` to `20261125` in the dialog. The run restores the
   schema, then starts `hw32_products_pipeline` and waits for it (about a minute).
4. **Show it's back.** `SELECT COUNT(*) FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT;` returns 2805, and the
   pipeline's `verify` log again says `history=254`. Screenshot the restore Graph, the
   `restore_snapshots` log and the count.

Do steps 2 and 3 back to back, so the midnight pipeline doesn't run against the damaged table in
between.

## Don't rerun these

- **`mutate_source.py`.** It would create run 4 with new changes, and the numbers in sections 5, 8, 9.2
  and 9.3 would all go stale.
- **`hw32_restore` before `HW32_BACKUP_20261125` exists.** The only backup now, `HW32_BACKUP_20261124`,
  was taken at 12:33 AM, before run 3. Restoring it rolls the snapshot back to 2,678 rows, and the
  pipeline run that follows would record run 3's changes again with tonight's timestamps. Use the existing
  12:39 AM run for K.
- **`hw32_eoy_snapshot`.** With `as_of` blank it fails (it tries a Jan 1, 2026 clone, far outside the 1-day
  window). With a recent `as_of`, it replaces `PRICE_YOY` with a comparison where nearly everything is
  unchanged, which loses the 83/44 result. Use the 1:21 AM run for H1.

Safe to rerun at any time: `hw32_products_pipeline` (the 6:18 PM run proves it: the same file produced
no new versions), `hw32_prototype`, `hw32_dev_clone_rating` and `hw32_daily_backup`.

## Normal things you may see

- Red marks in the DAG list for `hw32_eoy_snapshot` (12:25 AM) and `hw32_daily_backup` (12:33 AM).
  Those are the Nov 24 debugging failures described in 9.2 and 9.4. Crop them out of screenshots or
  mention them.
- The pink "skipped" task in `hw32_prototype` (see I).
- `HW32_DEV` shows a creation time of 6:18 PM: every pipeline run recreates it.

## Cleanup, after all screenshots (your call)

These are leftovers from Nov 22–24 and aren't used by any DAG:

```sql
DROP TABLE  HW32.ANALYTICS.DIM_PRODUCTS_BKP_20261122;         -- old per-table backups
DROP TABLE  HW32.ANALYTICS.DIM_PRODUCTS_BKP_20261123;
DROP TABLE  HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT_PIT;              -- first 9.2 attempt
DROP TABLE  HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT_PIT_20261124_0000;
DROP SCHEMA HW32.SNAPSHOTS_PRE_RESTORE_20261124_003926;        -- only after screenshot K
```

Keep `PRODUCTS_SNAPSHOT_PIT_20261124_0118`: the `PRODUCTS_SNAPSHOT_FROZEN` view and `PRICE_YOY` depend on it.
