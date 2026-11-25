"""Shared settings for the HW 3.2 DAGs. Not a DAG file (listed in .airflowignore)."""
from datetime import timedelta
from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

HOME = Path("/usr/local/airflow")
TZ = "America/Los_Angeles"
SNOWFLAKE_CONN = "snowflake_conn"  # defined in .env as AIRFLOW_CONN_SNOWFLAKE_CONN
PROD_DB = "HW32"
DEV_DB = "HW32_DEV"
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=1)}
BACKUP_PREFIX = f"{PROD_DB}_BACKUP_"  # 9.4 daily database clones: HW32_BACKUP_<yyyymmdd>

DBT_PROJECT = HOME / "dags/dbt/hw32_dbt"
PROFILES = HOME / "include/profiles.yml"
DBT_BIN = HOME / "dbt_venv/bin/dbt"


def dbt_group(group_id, target="prod", select=None, dbt_vars=None):
    """Cosmos task group: one Airflow task per dbt model/snapshot, with each model's tests after it.

    target picks the profiles.yml output: prod (HW32), dev (HW32_DEV clone), proto (HW32.PROTOTYPE).
    """
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(DBT_PROJECT, dbt_vars=dbt_vars),
        profile_config=ProfileConfig(profile_name="hw32_dbt", target_name=target,
                                     profiles_yml_filepath=PROFILES),
        execution_config=ExecutionConfig(dbt_executable_path=str(DBT_BIN)),
        # Tests that ref two models (scd2_matches_snapshot) get their own task after both parents,
        # instead of running right after the snapshot against a stale dim_products.
        render_config=RenderConfig(select=select or [], should_detach_multiple_parents_tests=True),
    )


def list_backups(dict_cursor) -> list:
    """Sorted backup database names whose suffix is a yyyymmdd date (needs a DictCursor)."""
    rows = dict_cursor.execute(f"SHOW DATABASES LIKE '{BACKUP_PREFIX}%'").fetchall()
    names = [r["name"] for r in rows]
    return sorted(n for n in names if len(n) == len(BACKUP_PREFIX) + 8 and n[-8:].isdigit())
