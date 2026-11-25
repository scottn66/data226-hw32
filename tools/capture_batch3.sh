#!/bin/bash
# Batch 3 evidence (round 3, item 10). Read-only: finished-run logs, SELECT/SHOW, import checks in the container.
set -u
cd "$(dirname "$0")/.."
AF="python3 tools/aflog.py"
STAMP="$(TZ=America/Los_Angeles date '+%Y-%m-%d %H:%M %Z')"
{ echo "# batch 3, $STAMP: failed runs that show platform limits (read-only log capture)"
  R='manual__2026-11-24T08:25:44.644902+00:00'
  $AF states hw32_eoy_snapshot $R
  for t in 1 2 3; do $AF log hw32_eoy_snapshot $R freeze_snapshot $t "Task failed|outside|retention|source"; done
  R='manual__2026-11-24T08:33:42.190113+00:00'
  $AF states hw32_daily_backup $R
  for t in 1 2 3; do $AF log hw32_daily_backup $R backup_database $t "Task failed|ds_nodash|undefined|Executing"; done
} > evidence/10_platform_limit_failures.txt
{ echo "# batch 3, $STAMP: import/signature checks inside the Airflow image (read-only python, no DAG run)"
  docker exec -i airflow_6c8d47-scheduler-1 python - <<'PY' 2>&1 | grep -v -i "warning"
import importlib
try:
    importlib.import_module("airflow.providers.snowflake.operators.snowflake").SnowflakeOperator
    print("SnowflakeOperator: importable")
except Exception as e:
    print("SnowflakeOperator:", type(e).__name__, str(e)[:160])
from airflow.sdk import DAG
try:
    DAG(dag_id="probe", schedule_interval="@daily")
    print("DAG(schedule_interval=...): accepted")
except Exception as e:
    print("DAG(schedule_interval=...):", type(e).__name__, str(e)[:160])
PY
} | python3 tools/redact.py > evidence/10_airflow3_checks.txt
tools/sfq.sh "SELECT warehouse_name, ROUND(SUM(credits_used), 3) AS credits FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE start_time >= '2026-11-19' GROUP BY 1 ORDER BY 2 DESC" \
  "SELECT warehouse_name, DATE(CONVERT_TIMEZONE('America/Los_Angeles', start_time)) AS day_pt, ROUND(SUM(credits_used), 3) AS credits FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE start_time >= '2026-11-19' AND warehouse_name IN ('HW32_WH', 'COMPUTE_WH') GROUP BY 1, 2 ORDER BY 1, 2" \
  "SELECT warehouse_name, COUNT(*) AS queries, ROUND(SUM(execution_time) / 1000 / 60, 1) AS exec_minutes FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE start_time >= '2026-11-19' AND warehouse_name IN ('HW32_WH', 'COMPUTE_WH') GROUP BY 1 ORDER BY 1" \
  "SHOW WAREHOUSES LIKE '%WH'" > evidence/10_credits_r3.txt
echo done
