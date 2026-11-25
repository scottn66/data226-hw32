#!/bin/bash
# Read-only Snowflake queries through the Airflow connection (snowflake_conn), redacted.
# Usage: tools/sfq.sh "SELECT ..." "SHOW ..."   -> prints capture header, full SQL, all rows
# Refuses anything that is not SELECT / WITH / SHOW / DESCRIBE.
HERE="$(cd "$(dirname "$0")" && pwd)"
args=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' "$@")
echo "# captured $(TZ=America/Los_Angeles date '+%Y-%m-%d %H:%M:%S %Z') via Airflow connection snowflake_conn (read-only)"
docker exec -i -e SQLS="$args" airflow_6c8d47-scheduler-1 python - 2>/dev/null <<'PY' | python3 "$HERE/redact.py"
import json, os, logging, re
logging.disable(logging.CRITICAL)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
sqls = json.loads(os.environ["SQLS"])
for s in sqls:
    if not re.match(r"\s*(SELECT|WITH|SHOW|DESCRIBE|DESC)\b", s, re.I):
        raise SystemExit("refused (not read-only): " + s[:80])
with SnowflakeHook(snowflake_conn_id="snowflake_conn").get_conn() as conn:
    cur = conn.cursor()
    for sql in sqls:
        print("\n>> " + " ".join(sql.split()))
        try:
            cur.execute(sql)
        except Exception as e:
            print("   ERROR:", str(e)[:500]); continue
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall() if cols else []
        print("   " + " | ".join(cols))
        for r in rows[:500]:
            print("   " + " | ".join(str(v) for v in r))
        print(f"   ({len(rows)} rows)")
PY
