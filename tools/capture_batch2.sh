#!/bin/bash
# Batch 2 evidence (round 2, item 9). Read-only: finished-run logs and a QUERY_HISTORY SELECT.
set -u
cd "$(dirname "$0")/.."
AF="python3 tools/aflog.py"
R_DEV='manual__2026-11-24T08:29:21.242374+00:00'
R_PROTO='manual__2026-11-24T22:08:29.063686+00:00'
R_MID='scheduled__2026-11-24T08:00:00+00:00'
R_RERUN='manual__2026-11-25T02:18:06.417949+00:00'
STAMP="$(TZ=America/Los_Angeles date '+%Y-%m-%d %H:%M %Z')"
{ echo "# batch 2, $STAMP: read-only log capture"
  for t in dbt_dev.stg_products.test dbt_dev.dim_products.run dbt_dev.dim_products.test dbt_dev.scd2_matches_snapshot_test; do
    $AF log hw32_dev_clone_rating $R_DEV $t 1 "OK created|Done\.|PASS rating|target="; done
  $AF log hw32_dev_clone_rating $R_DEV promote_ratings_to_prod 1 "Executing|CLONE"
  echo "## daily clone task in the main pipeline (scheduled midnight run, and the 6:18 PM run)"
  $AF log hw32_products_pipeline $R_MID clone_dev_database 1 "Executing:"
  $AF log hw32_products_pipeline $R_RERUN clone_dev_database 1 "Executing:"
} > evidence/91_dev_clone_dbt_logs.txt
{ echo "# batch 2, $STAMP: read-only log capture"
  for t in dbt_prototype.price_bands.run dbt_prototype.price_bands.test; do
    $AF log hw32_prototype $R_PROTO $t 1 "OK created|Done\.|PASS|target="; done
} > evidence/93_prototype_dbt_logs.txt
tools/sfq.sh "SELECT start_time, query_type, execution_status, LEFT(query_text, 120) AS query_text FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE start_time >= '2026-11-23' AND query_text ILIKE '%HW32_BACKUP_2026111%' ORDER BY start_time" > evidence/94_prune_test_fixtures.txt
echo done
