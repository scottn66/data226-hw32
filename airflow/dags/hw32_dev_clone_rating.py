"""9.1 Development environment via zero-copy clone: build and test a new feature (product rating)
with dbt in HW32_DEV, prove prod was untouched, then promote the tested data to prod.

clone_prod_to_dev -> land_ratings_in_dev -> fingerprint_prod -> dbt_dev (enable_rating=true, target dev)
-> check_dev_feature -> check_prod_untouched -> promote_ratings_to_prod

After promotion, flip `enable_rating: true` in dbt_project.yml so the daily prod pipeline
builds dim_products with the rating column. The ratings are simulated (the Kaggle data has none):
a deterministic 1.0-5.0 value per product.
"""
import logging

import pendulum
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from hw32_common import DEFAULT_ARGS, DEV_DB, PROD_DB, SNOWFLAKE_CONN, TZ, dbt_group

PROD_FINGERPRINT_SQL = f"SELECT HASH_AGG(*) FROM {PROD_DB}.ANALYTICS.DIM_PRODUCTS"


@dag(schedule=None, start_date=pendulum.datetime(2026, 11, 1, tz=TZ), catchup=False,
     tags=["hw32", "clone"], default_args=DEFAULT_ARGS)
def hw32_dev_clone_rating():
    clone_prod_to_dev = SQLExecuteQueryOperator(
        task_id="clone_prod_to_dev",
        conn_id=SNOWFLAKE_CONN,
        sql=f"CREATE OR REPLACE DATABASE {DEV_DB} CLONE {PROD_DB}",
    )

    # The new upstream feed lands in dev only.
    land_ratings_in_dev = SQLExecuteQueryOperator(
        task_id="land_ratings_in_dev",
        conn_id=SNOWFLAKE_CONN,
        sql=f"""
            CREATE OR REPLACE TABLE {DEV_DB}.RAW.PRODUCT_RATINGS AS
            SELECT product_id,
                   ROUND(1 + MOD(ABS(HASH(product_id)), 41) / 10, 1)::NUMBER(2, 1) AS rating,
                   CURRENT_TIMESTAMP() AS loaded_at
            FROM {DEV_DB}.RAW.STG_PRODUCTS_RAW
        """,
    )

    @task
    def fingerprint_prod() -> str:
        """Hash of every row in prod DIM_PRODUCTS before the dev run."""
        return str(SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_first(PROD_FINGERPRINT_SQL)[0])

    dbt_dev = dbt_group("dbt_dev", target="dev", dbt_vars={"enable_rating": True})

    check_dev_feature = SQLCheckOperator(
        task_id="check_dev_feature",
        conn_id=SNOWFLAKE_CONN,
        sql=f"""
            SELECT COUNT(*) > 0,                       -- dev dimension built
                   COUNT_IF(rating IS NULL) = 0,       -- every version has a rating
                   MIN(rating) >= 1 AND MAX(rating) <= 5
            FROM {DEV_DB}.ANALYTICS.DIM_PRODUCTS
        """,
    )

    @task
    def check_prod_untouched(before: str):
        after = str(SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_first(PROD_FINGERPRINT_SQL)[0])
        logging.info("prod DIM_PRODUCTS fingerprint before=%s after=%s", before, after)
        if after != before:
            raise ValueError("prod DIM_PRODUCTS changed during the dev run")

    # Runs only if every dev dbt test and check above passed. Zero-copy again: prod gets the exact
    # table that was tested.
    promote_ratings_to_prod = SQLExecuteQueryOperator(
        task_id="promote_ratings_to_prod",
        conn_id=SNOWFLAKE_CONN,
        sql=f"CREATE OR REPLACE TABLE {PROD_DB}.RAW.PRODUCT_RATINGS CLONE {DEV_DB}.RAW.PRODUCT_RATINGS",
    )

    before = fingerprint_prod()
    clone_prod_to_dev >> land_ratings_in_dev >> before >> dbt_dev >> check_dev_feature
    check_dev_feature >> check_prod_untouched(before) >> promote_ratings_to_prod


hw32_dev_clone_rating()
