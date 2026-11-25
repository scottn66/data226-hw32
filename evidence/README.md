# evidence/ — what was captured, how, and when

All captures are read-only: SELECT/SHOW queries through the Airflow connection (`tools/sfq.sh`, which
refuses anything else), logs of Airflow runs that had already finished (`tools/aflog.py`, REST GET only),
local version checks, and copies of the current source files. Every text file passes through
`tools/redact.py` (account, user, key path, emails, keys, URIs → [REDACTED]). Not evidence: this README,
writeup-comments.md, rerun-steps.md, loop_prompt.md.

Round 0 (Nov 24, 6:45 PM PT): `10_timetravel_before_run3.txt`.
Batch 1 (Nov 24, ~6:48–6:50 PM PT): everything else, by `tools/capture_batch1.sh`.

| File | Rubric | What it is |
|---|---|---|
| 00_assignment_snapshot_excerpt.txt | 10 | (8:08 PM) the assignment's example snapshot config (timestamp strategy on updated_at), from Scott's Nov 23 paste of the spec |
| 00_airflow_runs.txt | all | Last 20 runs per DAG with Pacific start/end times (run IDs are UTC) |
| 01_versions.txt | 1 | Airflow image packages, dbt in the image and on the Mac, Snowflake version, HW32_WH settings |
| 01_snowflake_jwt_failure.txt | 1, 4 | (7:00 PM) Nov 24 midnight scheduled runs: extract and backup_database failed 3 times ("Cannot specify basic authenticator together with private key parameters"), callback fired, try 4 succeeded after the fix |
| 06_dbt_project_tree.txt | 5, 6 | (7:00 PM) dbt project file tree, dbt_project.yml, stg_products.sql, products_snapshot.sql, target names |
| 06_airflow_dim_products_test_log.jpg | 5 | (7:00 PM) Airflow UI screenshot, 6:18 PM run, dbt_transform.dim_products.test: 8 of 8 PASS (800 × 537, built-in browser) |
| 02_dataset.txt | 2 | Row count, distinct product_id, null and repeated SKUs, column count, availability values, scraped_at range |
| 03_stg_count.txt | 3 | STG_PRODUCTS_RAW count, schemas, stage, file format, landing table |
| 04_retry_callback_extract.txt | 4 | Nov 23 scheduled run: extract tries 1–3 (one minute apart, conn_id not defined) and the failure callback line |
| 04_operators_rerun_0618pm.txt | 4, 7 | Task list with operator classes for the Nov 24, 6:18 PM pipeline run |
| 06_dbt_nodes.txt | 5, 6 | `dbt ls`: 2 models, 1 snapshot, 12 tests; the two dbt project copies are identical |
| 06_cosmos_dbt_logs_rerun_0618pm.txt | 5, 6, 7 | dbt output of every Cosmos task in the 6:18 PM run (3 + 8 + 1 tests PASS, snapshot, models) |
| 08_run3_verify.txt | 8 | dim_products counts now |
| 08_verify_logs.txt | 8 | `verify` and `load_staging` log lines of run 3 (1:20 AM) and the 6:18 PM rerun |
| 08_versions.txt | 8 | dim_products rows by version and change_type |
| 08_product_205235421.txt | 8 | A product with three versions |
| 08_source_diffs.txt | 8, 2 | (7:10 PM, local read-only) The three source CSVs compared: 127 changes per run (91 price + 36 title; 83 + 44), no rows added or removed, scraped_at never changed, 6 products changed in both |
| 08_snapshot_by_run.txt | 8 | Snapshot rows by dbt_valid_from (one timestamp per run that added rows) |
| 91_dev_clone_logs.txt, 91_dev_clone_untouched.txt | 9.1 | Dev DAG run (12:29 AM): tasks, checks, fingerprint; prod rating coverage now |
| 91_dev_clone_dbt_logs.txt | 9.1 | (batch 2, 7:20 PM) dbt in the dev target (tests incl. rating_in_range PASS), promotion clone into HW32.RAW, and the daily clone_dev_database statement in the scheduled midnight run and the 6:18 PM run |
| 93_prototype_dbt_logs.txt | 9.3 | (batch 2) price_bands built with the proto target in PROTOTYPE, its 3 tests PASS |
| 94_prune_test_fixtures.txt | 9.4 | (batch 2) QUERY_HISTORY: the two fake old backups created by hand at 12:33 AM, the DAG's DROP of HW32_BACKUP_20261116 at 12:37 AM, and the hand cleanup of HW32_BACKUP_20261118 |
| 92_eoy_logs.txt, 92_price_yoy.txt | 9.2 | Point-in-time run (1:21 AM): freeze log and summary; PRICE_YOY now; snapshot tables |
| 93_prototype_logs.txt | 9.3 | Prototype run (2:08 PM): clone, results, drop; PROTOTYPE schema no longer exists |
| 94_backup_logs.txt, 94_backups.txt | 9.4 | Backup runs (12:36 AM, 12:38 AM, 2:10 PM) and current backups |
| 94_restore_logs.txt, 94_restore.txt | 9.4 | Restore run (12:39 AM) and the pipeline run it triggered; schemas now |
| 10_credits_r3.txt | 10 | (batch 3, 7:55 PM) credits by warehouse and day, query count and execution minutes per warehouse, auto-suspend settings |
| 10_platform_limit_failures.txt | 10 | (batch 3) failed manual backup run: `'ds_nodash' is undefined` (3 tries); failed first year-end run (KeyError 'n', a code bug, plus "kind=TRANSIENT retention=1") |
| 10_airflow3_checks.txt | 10, 4 | (batch 3) inside the image: SnowflakeOperator does not exist; DAG() rejects schedule_interval |
| 10_timetravel_before_run3.txt | 10 | Snapshot count at 1:15 AM Nov 24 (Time Travel) vs now |
| 10_storage_metrics.txt | 10 | Bytes owned by the snapshot table and its clones |
| 10_credits.txt | 10 | Credits by warehouse and by day since Nov 19; warehouse settings |
| 10_transient_clone_error.txt | 10 | The "Transient object cannot be cloned to a permanent object" error from query history |
| src/*.txt | all | Copies of the current DAGs, dbt models/tests/snapshot/yml, setup.sql, mutate_source.py, Dockerfile, requirements.txt (never .env, rsa_key.p8 or profiles.yml) |

Not yet captured (each needs a write, so Scott decides): a fresh `dbt build` screenshot (PASS=15), a prune
demo that drops an old backup, a restore from a post-run-3 backup.
