"""9.4 Restore from the most recent backup (manual trigger only).

Only the SNAPSHOTS schema holds state nothing else can rebuild: RAW is reloaded from the source on
every run and ANALYTICS is rebuilt by dbt from the snapshot. So the restore:
  1. picks the newest HW32_BACKUP_<yyyymmdd> (or the `backup_date` param),
  2. clones its SNAPSHOTS schema into HW32.SNAPSHOTS_RESTORE and checks it isn't empty,
  3. atomically swaps it with HW32.SNAPSHOTS and keeps the old one as SNAPSHOTS_PRE_RESTORE_<ts>,
  4. re-runs hw32_products_pipeline to reload RAW and rebuild ANALYTICS on the restored history.
"""
import logging

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from snowflake.connector import DictCursor

from hw32_common import BACKUP_PREFIX, DEFAULT_ARGS, PROD_DB, SNOWFLAKE_CONN, TZ, list_backups


@dag(schedule=None, start_date=pendulum.datetime(2026, 11, 1, tz=TZ), catchup=False,
     tags=["hw32", "backup"], default_args=DEFAULT_ARGS,
     params={"backup_date": Param("", type="string",
                                  description="yyyymmdd to restore; blank = most recent backup")})
def hw32_restore():

    @task
    def pick_backup() -> str:
        wanted = get_current_context()["params"]["backup_date"].strip()
        with SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn() as conn:
            backups = list_backups(conn.cursor(DictCursor))
        if not backups:
            raise ValueError("no backups found")
        chosen = f"{BACKUP_PREFIX}{wanted}" if wanted else backups[-1]
        if chosen not in backups:
            raise ValueError(f"{chosen} not found; available: {backups}")
        logging.info("restoring from %s (available: %s)", chosen, backups)
        return chosen

    @task(retries=0)  # a swap must never be retried automatically
    def restore_snapshots(backup_db: str):
        stamp = pendulum.now(TZ).format("YYYYMMDD_HHmmss")
        with SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN).get_conn() as conn:
            cur = conn.cursor()
            # Plain CREATE (no OR REPLACE): a leftover SNAPSHOTS_RESTORE from a failed run stops us
            # instead of being silently overwritten.
            cur.execute(f"CREATE SCHEMA {PROD_DB}.SNAPSHOTS_RESTORE CLONE {backup_db}.SNAPSHOTS")
            n = cur.execute(
                f"SELECT COUNT(*) FROM {PROD_DB}.SNAPSHOTS_RESTORE.PRODUCTS_SNAPSHOT").fetchone()[0]
            if n == 0:
                cur.execute(f"DROP SCHEMA {PROD_DB}.SNAPSHOTS_RESTORE")
                raise ValueError(f"{backup_db}.SNAPSHOTS.PRODUCTS_SNAPSHOT is empty; nothing restored")
            cur.execute(f"ALTER SCHEMA {PROD_DB}.SNAPSHOTS SWAP WITH {PROD_DB}.SNAPSHOTS_RESTORE")
            cur.execute(f"ALTER SCHEMA {PROD_DB}.SNAPSHOTS_RESTORE "
                        f"RENAME TO {PROD_DB}.SNAPSHOTS_PRE_RESTORE_{stamp}")
        logging.info("restored %s snapshot rows from %s; previous schema kept as "
                     "SNAPSHOTS_PRE_RESTORE_%s", n, backup_db, stamp)

    rebuild = TriggerDagRunOperator(
        task_id="rerun_pipeline",
        trigger_dag_id="hw32_products_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        retries=0,
    )

    restore_snapshots(pick_backup()) >> rebuild


hw32_restore()
