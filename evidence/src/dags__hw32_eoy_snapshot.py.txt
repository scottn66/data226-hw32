"""9.2 Point-in-time clone + year-over-year dbt model.

Runs at local midnight on Jan 1: clones PRODUCTS_SNAPSHOT AT(TIMESTAMP => year end) into
PRODUCTS_SNAPSHOT_EOY_<year>, points the PRODUCTS_SNAPSHOT_FROZEN view at it, and builds the
price_yoy dbt model (frozen catalog vs. today).

Why the snapshot table and not the whole database: dbt recreates every ANALYTICS table on each run,
so their Time Travel only reaches back to the last run. The snapshot is merged in place, so its
history is continuous. It is TRANSIENT (dbt's default), which caps Time Travel at 1 day, so the
clone has to be taken at the year boundary, not weeks later.

For a demo before Jan 1, trigger with the `as_of` param set to a time within the last day.
"""
import logging
from datetime import timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Param, dag, get_current_context, task
from snowflake.connector import DictCursor

from hw32_common import DEFAULT_ARGS, PROD_DB, SNOWFLAKE_CONN, TZ, dbt_group

SNAPSHOT = f"{PROD_DB}.SNAPSHOTS.PRODUCTS_SNAPSHOT"


@dag(schedule="0 0 1 1 *", start_date=pendulum.datetime(2026, 11, 1, tz=TZ), catchup=False,
     tags=["hw32", "clone"], default_args=DEFAULT_ARGS,
     params={"as_of": Param("", type="string",
                            description="Blank = year-end clone. Demo: a time within the last "
                                        "day, e.g. 2026-11-22T19:39:00-08:00")})
def hw32_eoy_snapshot():

    @task
    def freeze_snapshot() -> str:
        ctx = get_current_context()
        as_of_param = ctx["params"]["as_of"].strip()
        if as_of_param:
            as_of = pendulum.parse(as_of_param, tz=TZ, strict=False)
            label = "PIT_" + as_of.in_timezone(TZ).format("YYYYMMDD_HHmm")
        else:
            as_of = pendulum.instance(ctx["dag_run"].run_after).in_timezone(TZ).start_of("year")
            label = f"EOY_{as_of.year - 1}"
        clone = f"{SNAPSHOT}_{label}"
        ts = as_of.in_timezone(TZ).format("YYYY-MM-DD HH:mm:ss Z")

        with SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn() as conn:
            cur = conn.cursor(DictCursor)
            info = cur.execute(
                f"SHOW TABLES LIKE 'PRODUCTS_SNAPSHOT' IN SCHEMA {PROD_DB}.SNAPSHOTS").fetchone()
            retention_days = int(info["retention_time"])
            logging.info("source %s kind=%s retention=%s day(s)", SNAPSHOT, info["kind"], retention_days)
            if pendulum.now("UTC") - as_of > timedelta(days=retention_days):
                raise ValueError(f"{ts} is outside the {retention_days}-day Time Travel window of "
                                 f"{SNAPSHOT}; a year-end clone must run at the year boundary")

            kind = "TRANSIENT TABLE" if info["kind"] == "TRANSIENT" else "TABLE"
            cur.execute(f"CREATE OR REPLACE {kind} {clone} CLONE {SNAPSHOT} "
                        f"AT(TIMESTAMP => TO_TIMESTAMP_TZ('{ts}', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'))")
            cur.execute(f"CREATE OR REPLACE VIEW {SNAPSHOT}_FROZEN AS "
                        f"SELECT '{label}' AS frozen_label, * FROM {clone}")
            then_rows = cur.execute(f"SELECT COUNT(*) AS N FROM {clone}").fetchone()["N"]
            now_rows = cur.execute(f"SELECT COUNT(*) AS N FROM {SNAPSHOT}").fetchone()["N"]
        logging.info("cloned %s as of %s: %s snapshot rows then, %s now", clone, ts, then_rows, now_rows)
        return label

    price_yoy = dbt_group("dbt_price_yoy", select=["price_yoy"], dbt_vars={"build_yoy": True})

    summarize = SQLExecuteQueryOperator(
        task_id="summarize_yoy",
        conn_id=SNOWFLAKE_CONN,
        sql=f"""
            SELECT compared_to, status, COUNT(*) AS products,
                   ROUND(AVG(pct_change), 2) AS avg_pct_change
            FROM {PROD_DB}.ANALYTICS.PRICE_YOY
            GROUP BY 1, 2 ORDER BY 2
        """,
        show_return_value_in_logs=True,
    )

    freeze_snapshot() >> price_yoy >> summarize


hw32_eoy_snapshot()
