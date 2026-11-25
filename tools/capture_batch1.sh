#!/bin/bash
# Batch 1 evidence capture (Nov 24 evening). Read-only: SELECT/SHOW, finished-run logs, local files.
set -u
cd "$(dirname "$0")/.."
E=evidence; T=tools
P=hw32_products_pipeline
R_FAIL='scheduled__2026-11-23T08:00:00+00:00'
R_RUN3='manual__2026-11-24T09:20:07.443146+00:00'
R_RERUN='manual__2026-11-25T02:18:06.417949+00:00'
R_DEV='manual__2026-11-24T08:29:21.242374+00:00'
R_EOY='manual__2026-11-24T09:21:09.363737+00:00'
R_PROTO='manual__2026-11-24T22:08:29.063686+00:00'
R_BKP_PRUNE='manual__2026-11-24T08:36:33.309227+00:00'
R_BKP_1238='manual__2026-11-24T08:38:39.118968+00:00'
R_BKP_PM='manual__2026-11-24T22:10:23.303919+00:00'
R_RESTORE='manual__2026-11-24T08:39:24.346007+00:00'
R_RESTORE_PIPE='manual__2026-11-24T08:39:34.052055+00:00'
AF="python3 $T/aflog.py"

# --- run lists (Pacific times) ---
for d in $P hw32_dev_clone_rating hw32_eoy_snapshot hw32_prototype hw32_daily_backup hw32_restore; do
  $AF runs $d; echo; done > $E/00_airflow_runs.txt

# --- item 1: versions ---
{
  echo "# captured $(TZ=America/Los_Angeles date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "## Airflow image (docker exec airflow_6c8d47-scheduler-1)"
  docker exec airflow_6c8d47-scheduler-1 bash -c 'airflow version; python --version; pip list 2>/dev/null | grep -i -E "^(apache-airflow|apache-airflow-providers-snowflake|apache-airflow-providers-common-sql|astronomer-cosmos|snowflake-connector-python) "; echo "-- dbt_venv:"; /usr/local/airflow/dbt_venv/bin/python --version; /usr/local/airflow/dbt_venv/bin/dbt --version' 2>&1 | grep -v -i warning
  echo "## Airflow API version (http://localhost:8080 and http://airflow.localhost:6563)"
  curl -s http://localhost:8080/api/v2/version; echo; curl -s http://airflow.localhost:6563/api/v2/version; echo
  echo "## Mac dbt (hw32/.venv)"
  .venv/bin/python --version; .venv/bin/dbt --version 2>&1
} | python3 $T/redact.py > $E/01_versions.txt
$T/sfq.sh "SELECT CURRENT_VERSION()" "SHOW WAREHOUSES LIKE 'HW32_WH'" >> $E/01_versions.txt

# --- item 2: dataset ---
$T/sfq.sh \
 "SELECT COUNT(*) AS rows_, COUNT(DISTINCT product_id) AS distinct_product_id, COUNT_IF(sku IS NULL) AS null_sku, COUNT(sku) - COUNT(DISTINCT sku) AS repeat_sku FROM HW32.RAW.STG_PRODUCTS_RAW" \
 "SELECT COUNT(*) AS n_columns FROM HW32.INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'RAW' AND table_name = 'STG_PRODUCTS_RAW'" \
 "SELECT availability, COUNT(*) FROM HW32.RAW.STG_PRODUCTS_RAW GROUP BY 1" \
 "SELECT MIN(scraped_at), MAX(scraped_at), COUNT(DISTINCT scraped_at) FROM HW32.RAW.STG_PRODUCTS_RAW" \
 > $E/02_dataset.txt

# --- item 3: staging objects + count ---
$T/sfq.sh "SELECT COUNT(*) FROM HW32.RAW.STG_PRODUCTS_RAW" "SHOW SCHEMAS IN DATABASE HW32" \
 "SHOW STAGES IN SCHEMA HW32.RAW" "SHOW FILE FORMATS IN SCHEMA HW32.RAW" \
 "SHOW TABLES LIKE 'STG_PRODUCTS_RAW' IN SCHEMA HW32.RAW" > $E/03_stg_count.txt

# --- item 4: retries + callback (Nov 23 failed scheduled run) ---
{ $AF states $P $R_FAIL
  for t in 1 2 3; do $AF log $P $R_FAIL extract $t "ERROR|isn't defined|FAILED|Marking|retry|UP_FOR_RETRY|Task failed"; done
} > $E/04_retry_callback_extract.txt
# operators in a finished run
$AF states $P $R_RERUN > $E/04_operators_rerun_0618pm.txt

# --- items 5-6 / 7: dbt nodes + Cosmos logs of the 6:18 PM run ---
{ echo "# dbt ls (local project hw32_dbt, no warehouse connection), $(TZ=America/Los_Angeles date '+%Y-%m-%d %H:%M %Z')"
  (cd hw32_dbt && ../.venv/bin/dbt ls --quiet --resource-types model snapshot test --output json --output-keys name resource_type 2>&1)
} > $E/06_dbt_ls.raw
python3 - <<'PY' > $E/06_dbt_nodes.txt
import json, collections
lines=[l for l in open("evidence/06_dbt_ls.raw") if l.startswith("{")]
nodes=[json.loads(l) for l in lines]
c=collections.Counter(n["resource_type"] for n in nodes)
print("# dbt ls on hw32_dbt (same files as airflow/dags/dbt/hw32_dbt; diff -rq showed no differences)")
for n in sorted(nodes, key=lambda n:(n["resource_type"], n["name"])): print(f"{n['resource_type']:<9} {n['name']}")
print(f"\ncounts: models={c['model']} snapshots={c['snapshot']} tests={c['test']} total={sum(c.values())}")
PY
rm -f $E/06_dbt_ls.raw
diff -rq hw32_dbt airflow/dags/dbt/hw32_dbt -x target -x logs -x dbt_packages -x .user.yml >> $E/06_dbt_nodes.txt 2>&1 && echo "diff -rq hw32_dbt airflow/dags/dbt/hw32_dbt: identical" >> $E/06_dbt_nodes.txt
{ for t in dbt_transform.stg_products.run dbt_transform.stg_products.test dbt_transform.products_snapshot_snapshot dbt_transform.dim_products.run dbt_transform.dim_products.test dbt_transform.scd2_matches_snapshot_test; do
    $AF log $P $R_RERUN $t 1 "START|PASS|FAIL|ERROR=|Done\.|OK created|OK snapshotted|SUCCESS|Finished running"; done
} > $E/06_cosmos_dbt_logs_rerun_0618pm.txt

# --- item 8: runs ---
$T/sfq.sh \
 "SELECT COUNT(*) AS dim_rows, COUNT_IF(is_current) AS current_rows, COUNT(DISTINCT product_id) AS products, COUNT(*) - COUNT_IF(is_current) AS history FROM HW32.ANALYTICS.DIM_PRODUCTS" \
 > $E/08_run3_verify.txt
{ $AF log $P $R_RUN3 verify 1 "dim rows"; $AF log $P $R_RUN3 load_staging 1 "staging rows"
  $AF log $P $R_RERUN verify 1 "dim rows"; $AF log $P $R_RERUN load_staging 1 "staging rows"; } > $E/08_verify_logs.txt
$T/sfq.sh "SELECT version, change_type, COUNT(*) FROM HW32.ANALYTICS.DIM_PRODUCTS GROUP BY 1, 2 ORDER BY 1, 2" \
          "SELECT version, COUNT(*) FROM HW32.ANALYTICS.DIM_PRODUCTS GROUP BY 1 ORDER BY 1" > $E/08_versions.txt
$T/sfq.sh "SELECT product_id, version, change_type, previous_price, price, valid_from, valid_to, is_current FROM HW32.ANALYTICS.DIM_PRODUCTS WHERE product_id = 205235421 ORDER BY version" > $E/08_product_205235421.txt
$T/sfq.sh "SELECT dbt_valid_from, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', dbt_valid_from) AS valid_from_pt, COUNT(*) AS rows_ FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT GROUP BY 1, 2 ORDER BY 1" \
          "SELECT COUNT(*) AS snapshot_rows, COUNT_IF(dbt_valid_to IS NULL) AS current_rows FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT" > $E/08_snapshot_by_run.txt

# --- item 9 ---
{ $AF states hw32_dev_clone_rating $R_DEV
  for t in clone_prod_to_dev land_ratings_in_dev check_dev_feature check_prod_untouched promote_ratings_to_prod; do
    $AF log hw32_dev_clone_rating $R_DEV $t 1 "Executing|fingerprint|Record|Success|True|ERROR"; done
} > $E/91_dev_clone_logs.txt
$T/sfq.sh "SELECT COUNT(*), COUNT(rating), ROUND(AVG(rating), 2), MIN(rating), MAX(rating) FROM HW32.ANALYTICS.DIM_PRODUCTS WHERE is_current" \
          "SHOW DATABASES LIKE 'HW32_DEV'" "SELECT COUNT(*) FROM HW32.RAW.PRODUCT_RATINGS" > $E/91_dev_clone_untouched.txt
{ $AF states hw32_eoy_snapshot $R_EOY
  $AF log hw32_eoy_snapshot $R_EOY freeze_snapshot 1 "source|cloned|ERROR"
  $AF log hw32_eoy_snapshot $R_EOY summarize_yoy 1 "Operator output|Executing"; } > $E/92_eoy_logs.txt
$T/sfq.sh "SELECT compared_to, status, COUNT(*) AS products, ROUND(AVG(pct_change), 2) AS avg_pct_change, MIN(pct_change), MAX(pct_change) FROM HW32.ANALYTICS.PRICE_YOY GROUP BY 1, 2 ORDER BY 2" \
          "SELECT COUNT(*) FROM HW32.SNAPSHOTS.PRODUCTS_SNAPSHOT_PIT_20261124_0118" \
          "SHOW TABLES IN SCHEMA HW32.SNAPSHOTS" "SHOW TABLES IN SCHEMA HW32.ANALYTICS" > $E/92_price_yoy.txt
{ $AF states hw32_prototype $R_PROTO
  $AF log hw32_prototype $R_PROTO clone_analytics_schema 1 "Executing"
  $AF log hw32_prototype $R_PROTO show_results 1 "Operator output|Executing"
  $AF log hw32_prototype $R_PROTO drop_prototype_schema 1 "Executing"; } > $E/93_prototype_logs.txt
$T/sfq.sh "SHOW SCHEMAS LIKE 'PROTOTYPE' IN DATABASE HW32" >> $E/93_prototype_logs.txt
{ for r in $R_BKP_PRUNE $R_BKP_1238 $R_BKP_PM; do $AF states hw32_daily_backup $r
    $AF log hw32_daily_backup $r backup_name 1 "Returned"; $AF log hw32_daily_backup $r backup_database 1 "Executing"
    $AF log hw32_daily_backup $r prune_old_backups 1 "dropped|kept|ERROR"; done; } > $E/94_backup_logs.txt
$T/sfq.sh "SHOW DATABASES LIKE 'HW32_BACKUP_%'" > $E/94_backups.txt
{ $AF states hw32_restore $R_RESTORE
  $AF log hw32_restore $R_RESTORE pick_backup 1 "restoring"
  $AF log hw32_restore $R_RESTORE restore_snapshots 1 "restored|ERROR"
  $AF states $P $R_RESTORE_PIPE; $AF log $P $R_RESTORE_PIPE verify 1 "dim rows"; } > $E/94_restore_logs.txt
$T/sfq.sh "SHOW SCHEMAS IN DATABASE HW32" > $E/94_restore.txt

# --- item 10 ---
$T/sfq.sh "SELECT table_catalog, table_schema, active_bytes, time_travel_bytes FROM HW32.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS WHERE table_name = 'PRODUCTS_SNAPSHOT' AND table_dropped IS NULL AND schema_dropped IS NULL AND catalog_dropped IS NULL ORDER BY 1, 2" > $E/10_storage_metrics.txt
$T/sfq.sh "SELECT warehouse_name, ROUND(SUM(credits_used), 3) AS credits FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE start_time >= '2026-11-19' GROUP BY 1" \
          "SELECT warehouse_name, DATE(CONVERT_TIMEZONE('America/Los_Angeles', start_time)) AS day_pt, ROUND(SUM(credits_used), 3) AS credits FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE start_time >= '2026-11-19' GROUP BY 1, 2 ORDER BY 1, 2" \
          "SHOW WAREHOUSES" > $E/10_credits.txt
$T/sfq.sh "SELECT start_time, query_type, error_code, error_message FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE start_time >= '2026-11-23' AND error_message ILIKE '%Transient object cannot be cloned%' ORDER BY start_time" > $E/10_transient_clone_error.txt

# --- source copies (no secrets; .env, rsa_key.p8 and profiles.yml are never copied) ---
mkdir -p $E/src
for f in airflow/dags/hw32_*.py airflow/include/mutate_source.py airflow/Dockerfile airflow/requirements.txt setup.sql \
         airflow/dags/dbt/hw32_dbt/dbt_project.yml airflow/dags/dbt/hw32_dbt/snapshots/*.sql airflow/dags/dbt/hw32_dbt/tests/*.sql \
         airflow/dags/dbt/hw32_dbt/models/*/*.sql airflow/dags/dbt/hw32_dbt/models/*/*.yml; do
  cp "$f" "$E/src/$(echo "$f" | sed 's#^airflow/dags/dbt/hw32_dbt/#dbt_#; s#^airflow/##; s#/#__#g').txt"
done
echo done
