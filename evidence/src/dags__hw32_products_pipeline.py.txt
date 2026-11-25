"""HW 3.2 main pipeline.

extract (validate CSV, PUT to internal stage) -> load_staging (transactional COPY INTO RAW)
-> dbt_transform (Cosmos: stg_products -> products_snapshot (SCD2) -> dim_products, tests after each)
-> verify (one current row per product) -> clone_dev_database (9.1: fresh zero-copy dev clone daily)
"""
import csv
import logging
from datetime import timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from hw32_common import DEV_DB, HOME, PROD_DB, SNOWFLAKE_CONN, TZ, dbt_group

CSV_FILE = HOME / "include/data/home_depot_data_1_2021_12.csv"
STAGE = f"{PROD_DB}.RAW.PRODUCTS_STAGE"
RAW_TABLE = f"{PROD_DB}.RAW.STG_PRODUCTS_RAW"
FILE_FORMAT = f"{PROD_DB}.RAW.CSV_FMT"
STAGED_FILE = CSV_FILE.name + ".gz"  # PUT gzips the file


def notify_failure(context):
    ti = context["task_instance"]
    logging.error("HW32 pipeline FAILED at task %s (try %s)", ti.task_id, ti.try_number)


@dag(start_date=pendulum.datetime(2026, 11, 1, tz=TZ), schedule="@daily", catchup=False,
     tags=["hw32"],
     default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                   "on_failure_callback": notify_failure})
def hw32_products_pipeline():

    @task
    def extract() -> int:
        """Validate the source file, then upload it to the Snowflake internal stage."""
        if not CSV_FILE.exists():
            raise FileNotFoundError(CSV_FILE)
        with CSV_FILE.open(newline="") as f:
            n = sum(1 for _ in csv.reader(f)) - 1
        if n <= 0:
            raise ValueError("source file has no rows")
        with SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn() as conn:
            conn.cursor().execute(
                f"PUT file://{CSV_FILE} @{STAGE} OVERWRITE=TRUE AUTO_COMPRESS=TRUE")
        logging.info("source rows: %s, staged as @%s/%s", n, STAGE, STAGED_FILE)
        return n

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

    dbt_transform = dbt_group("dbt_transform")

    @task
    def verify():
        total, current, products = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_first(
            "SELECT COUNT(*), COUNT_IF(is_current), COUNT(DISTINCT product_id) "
            f"FROM {PROD_DB}.ANALYTICS.DIM_PRODUCTS")
        logging.info("dim rows=%s current=%s products=%s history=%s",
                     total, current, products, total - current)
        if current != products:
            raise ValueError("each product must have exactly one current row")

    # 9.1: rebuild the dev environment from verified prod data every day (zero-copy, no storage cost
    # until dev diverges).
    clone_dev_database = SQLExecuteQueryOperator(
        task_id="clone_dev_database",
        conn_id=SNOWFLAKE_CONN,
        sql=f"CREATE OR REPLACE DATABASE {DEV_DB} CLONE {PROD_DB}",
    )

    load_staging(extract()) >> dbt_transform >> verify() >> clone_dev_database


hw32_products_pipeline()
