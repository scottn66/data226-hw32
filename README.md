# DATA 226 HW 3.2: dbt + Airflow + Snowflake

An e-commerce product catalog (Kaggle Home Depot data, 2,551 products) loaded into Snowflake by Airflow 3,
historized as a slowly changing dimension (SCD2) with a dbt snapshot, and used to practice Snowflake
zero-copy cloning: a dev clone, a point-in-time clone, a prototype schema, and daily backups with restore.
SJSU DATA 226, Fall 2026, due Nov 25, 2026.

Stack: Astro Runtime 3.4 (Airflow 3.4, Python 3.14) in Docker, Astronomer Cosmos 1.16 (dbt inside Airflow),
dbt-core 1.10 with dbt-snowflake 1.10, Snowflake key-pair (JWT) auth.

## Layout

| Path | What it is |
|---|---|
| `airflow/` | Astro project. `dags/` holds the six DAGs, shared helpers in `hw32_common.py`, and the dbt project Airflow runs (`dags/dbt/hw32_dbt/`) |
| `airflow/include/data/` | Source CSVs: `original.csv` (run 1), `run2.csv` (run 2), `home_depot_data_1_2021_12.csv` (current, run 3) |
| `airflow/include/mutate_source.py` | Changes a random 5% of rows (price or title) to simulate source updates |
| `hw32_dbt/` | Copy of the dbt project for running the dbt CLI on the Mac (identical to `airflow/dags/dbt/hw32_dbt/` at the first commit) |
| `setup.sql` | One-time Snowflake setup: warehouse, database, schemas, stage, file format, landing table |
| `diagram/` | Architecture diagram and the matplotlib script that draws it |
| `screenshots/` | Screenshots used in the writeup (01 and 06 are redacted copies) |
| `rerun-steps.md` | Step-by-step rerun and screenshot guide, with run IDs mapped to Pacific times |
| `evidence/` | Read-only captures (query results, Airflow logs, version checks) behind every number in the writeup; index in `evidence/README.md` |
| `tools/` | Helper scripts: read-only Snowflake queries, Airflow log fetcher, redaction, secret check, writeup checkers |
| `iter/`, `writeup-comments.md`, `loop_prompt.md`, `report_patch.md` | Record of the writeup review loop (Google Doc text exports, scores, patches). Not needed to run anything |

## Set up from scratch

1. Snowflake: create a key pair for your user (`openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out airflow/include/rsa_key.p8`,
   then `ALTER USER ... SET RSA_PUBLIC_KEY='...'` with the public key), and run `setup.sql`.
2. Copy `airflow/.env.example` to `airflow/.env` and `airflow/include/profiles.yml.example` to
   `airflow/include/profiles.yml`, and fill in the account identifier and user. Both copies are gitignored.
3. `cd airflow && astro dev start`, open http://localhost:8080, and trigger `hw32_products_pipeline`.
4. To see SCD2 history: `python airflow/include/mutate_source.py`, then run the pipeline again.

## DAGs

| DAG | Schedule | Rubric | Tasks |
|---|---|---|---|
| `hw32_products_pipeline` | daily | 4, 5, 7, 8, 9.1 | extract (validate + PUT to stage) → load_staging (transactional COPY, rollback on mismatch) → dbt_transform (Cosmos) → verify → clone_dev_database |
| `hw32_dev_clone_rating` | manual | 9.1 | clone prod → land ratings in dev → dbt in dev with `enable_rating` → checks (dev feature, prod fingerprint unchanged) → promote ratings to prod |
| `hw32_eoy_snapshot` | Jan 1 | 9.2 | clone snapshot `AT(TIMESTAMP)` → dbt `price_yoy` → summary (`as_of` param for a demo) |
| `hw32_prototype` | manual | 9.3 | clone ANALYTICS → PROTOTYPE → dbt `price_bands` (target `proto`) → show → drop schema (always) |
| `hw32_daily_backup` | daily | 9.4 | name from the run's Pacific date → `CREATE DATABASE IF NOT EXISTS HW32_BACKUP_<yyyymmdd> CLONE HW32` → keep last 7 |
| `hw32_restore` | manual | 9.4 | newest backup → clone its SNAPSHOTS schema → swap → re-run pipeline |

Snowflake connection: `snowflake_conn`, defined in `airflow/.env` (key-pair auth with
`"authenticator": "SNOWFLAKE_JWT"` and `include/rsa_key.p8`). Reload it with `astro dev restart`
after editing `.env`.

9.1 promotion: after `hw32_dev_clone_rating` succeeds, set `enable_rating: true` in
`dbt_project.yml` and run `hw32_products_pipeline`; prod `DIM_PRODUCTS` then has `RATING`.
Done on Nov 24, 2026 (the flag is now `true`).

9.2 and restores: the `as_of` clone uses Time Travel on `SNAPSHOTS.PRODUCTS_SNAPSHOT`, which is
transient (1 day). A restore replaces that table with a clone of the backup, so `as_of` can't
reach back past the most recent restore.

Source changes for the SCD test: `python airflow/include/mutate_source.py` (changes a random 5% of
rows). Run 1 = `data/original.csv`, run 2 = `data/run2.csv` (127 changes: 91 price, 36 title),
run 3 = the current `home_depot_data_1_2021_12.csv` (127 more: 83 price, 44 title; 6 products
reach version 3). Snapshot timestamps (`valid_from`/`valid_to`) are UTC.

## Things that tripped us up (reference)

Airflow 3 (Astro Runtime 3.4):
- `SnowflakeOperator` is gone from the Snowflake provider (6.21). Use `SQLExecuteQueryOperator` from
  `airflow.providers.common.sql` with `conn_id="snowflake_conn"`, or `SnowflakeHook` inside a `@task`.
- `DAG(schedule_interval=...)` raises a TypeError; the argument is `schedule`.
- Manual runs have no `ds` / `ds_nodash` (a template using them fails with "'ds_nodash' is undefined").
  Derive dates from `dag_run.run_after` instead (see `hw32_daily_backup.py`).
- The REST API v2 returns task logs as JSON events; the exception text is in `error_detail` (`tools/aflog.py`).

Snowflake connection and connector:
- Key-pair auth in an Airflow connection needs `"authenticator": "SNOWFLAKE_JWT"` in the extras. Without it
  the connector fails with "Cannot specify basic authenticator together with private key parameters".
- With a `DictCursor`, keys for `SELECT` column aliases come back UPPERCASE; `SHOW` output keys stay lowercase.

dbt and Cosmos:
- dbt-snowflake builds tables as transient by default: 1-day Time Travel, and a transient table can't be
  cloned into a permanent one ("Transient object cannot be cloned to a permanent object").
- Cosmos `DbtTaskGroup` needed `RenderConfig(should_detach_multiple_parents_tests=True)` for a test that
  depends on two nodes (here `scd2_matches_snapshot`, which reads the snapshot and `dim_products`).
- One `profiles.yml` with three targets (`prod`, `dev` → `HW32_DEV`, `proto` → `PROTOTYPE` schema) lets
  the same Cosmos helper (`hw32_common.dbt_group`) build into prod, a dev clone, or a prototype schema.

Cloning:
- Clones are zero-copy: a clone owns no storage until rows change (`evidence/10_storage_metrics.txt`).
- `CREATE DATABASE ... CLONE` is the simplest backup; prune by parsing the `yyyymmdd` suffix.

## Tools

- `tools/sfq.sh "SELECT ..."`: runs a read-only query through the Airflow connection inside the scheduler
  container (refuses anything but SELECT/WITH/SHOW/DESC), output redacted.
- `tools/aflog.py runs|states|log ...`: reads DAG runs and task logs from the local Airflow REST API.
- `tools/check_secrets.py`: checks every file git would commit for the Snowflake account, user and key text
  (read from the gitignored config files) and for key blocks, credential URIs and email addresses. The
  repo's pre-commit hook runs it. Images can't be scanned, so look at new screenshots before adding them.

Never commit `airflow/.env`, `airflow/include/rsa_key.p8` or `airflow/include/profiles.yml` (all gitignored).
The source data comes from a Kaggle dataset; check its license before making this repository public.
