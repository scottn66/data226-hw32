FROM astrocrpublic.azurecr.io/runtime:3.4-1
RUN pip install --no-cache-dir uv && \
    uv venv --python 3.12 /usr/local/airflow/dbt_venv && \
    uv pip install --python /usr/local/airflow/dbt_venv/bin/python "dbt-core~=1.10.0" "dbt-snowflake~=1.10.0"