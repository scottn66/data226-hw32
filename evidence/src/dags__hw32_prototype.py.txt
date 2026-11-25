"""9.3 Rapid prototyping with a cloned schema.

clone_analytics_schema (HW32.ANALYTICS -> HW32.PROTOTYPE, zero-copy)
-> dbt_prototype (price_bands, built with the `proto` target, so it reads and writes only PROTOTYPE)
-> show_results -> drop_prototype_schema (always runs, even if the prototype fails)

fail_if_prototype_failed keeps the DAG run red when something upstream failed; otherwise the
always-run cleanup task would make the run look successful.
"""
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import TriggerRule, dag, task

from hw32_common import DEFAULT_ARGS, PROD_DB, SNOWFLAKE_CONN, TZ, dbt_group

PROTO = f"{PROD_DB}.PROTOTYPE"


@dag(schedule=None, start_date=pendulum.datetime(2026, 11, 1, tz=TZ), catchup=False,
     tags=["hw32", "clone"], default_args=DEFAULT_ARGS)
def hw32_prototype():
    clone_schema = SQLExecuteQueryOperator(
        task_id="clone_analytics_schema",
        conn_id=SNOWFLAKE_CONN,
        sql=f"CREATE OR REPLACE SCHEMA {PROTO} CLONE {PROD_DB}.ANALYTICS",
    )

    prototype = dbt_group("dbt_prototype", target="proto", select=["price_bands"])

    show_results = SQLExecuteQueryOperator(
        task_id="show_results",
        conn_id=SNOWFLAKE_CONN,
        sql=f"SELECT * FROM {PROTO}.PRICE_BANDS ORDER BY price_band",
        show_return_value_in_logs=True,
    )

    drop_schema = SQLExecuteQueryOperator(
        task_id="drop_prototype_schema",
        conn_id=SNOWFLAKE_CONN,
        sql=f"DROP SCHEMA IF EXISTS {PROTO}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def fail_if_prototype_failed():
        raise RuntimeError("prototype failed (PROTOTYPE schema was still dropped)")

    clone_schema >> prototype >> show_results >> drop_schema
    [clone_schema, prototype, show_results] >> fail_if_prototype_failed()


hw32_prototype()
