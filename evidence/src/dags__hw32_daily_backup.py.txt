"""9.4 Daily zero-copy backup of the whole HW32 database, keeping the last 7 days.

backup_name: HW32_BACKUP_<yyyymmdd>, from the run's local (Pacific) date. Airflow 3 manual runs
  have no logical date, so `ds` can't be used, and it would be the UTC date anyway.
backup_database: CREATE DATABASE IF NOT EXISTS <name> CLONE HW32
  (IF NOT EXISTS so a same-day rerun can't overwrite a good morning backup with later bad data)
prune_old_backups: drop HW32_BACKUP_* dated before the 7-day window (today and the 6 days before).
Restore lives in hw32_restore (manual only).
"""
import logging
from datetime import timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task
from snowflake.connector import DictCursor

from hw32_common import BACKUP_PREFIX, DEFAULT_ARGS, PROD_DB, SNOWFLAKE_CONN, TZ, list_backups

KEEP_DAYS = 7


@dag(schedule="@daily", start_date=pendulum.datetime(2026, 11, 22, tz=TZ), catchup=False,
     tags=["hw32", "backup"], default_args=DEFAULT_ARGS)
def hw32_daily_backup():

    @task
    def backup_name() -> str:
        run_after = pendulum.instance(get_current_context()["dag_run"].run_after)
        return BACKUP_PREFIX + run_after.in_timezone(TZ).format("YYYYMMDD")

    name = backup_name()

    backup_database = SQLExecuteQueryOperator(
        task_id="backup_database",
        conn_id=SNOWFLAKE_CONN,
        sql=("CREATE DATABASE IF NOT EXISTS {{ ti.xcom_pull(task_ids='backup_name') }} "
             f"CLONE {PROD_DB}"),
    )

    @task
    def prune_old_backups(newest: str):
        today = pendulum.from_format(newest[-8:], "YYYYMMDD")
        keep_from = (today - timedelta(days=KEEP_DAYS - 1)).format("YYYYMMDD")
        with SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn() as conn:
            cur = conn.cursor(DictCursor)
            for old in list_backups(cur):
                if old[-8:] < keep_from:
                    cur.execute(f"DROP DATABASE IF EXISTS {old}")
                    logging.info("dropped %s", old)
            logging.info("backups kept (>= %s): %s", keep_from, list_backups(cur))

    name >> backup_database >> prune_old_backups(name)


hw32_daily_backup()
