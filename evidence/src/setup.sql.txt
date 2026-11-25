-- DATA 226 HW 3.2: one-time Snowflake setup (section 4 of the writeup).
-- Recovered from the account's query history (Nov 20, 2026) and GET_DDL on Nov 24, 2026.
-- Everything else is created by the pipeline: dbt builds ANALYTICS and SNAPSHOTS, and the DAGs
-- create the clones (HW32_DEV, HW32_BACKUP_<yyyymmdd>, HW32.PROTOTYPE, SNAPSHOTS.*_PIT_*).

USE ROLE ACCOUNTADMIN;

-- Compute: smallest size, suspends after 60 s idle.
CREATE WAREHOUSE IF NOT EXISTS hw32_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS HW32;
CREATE SCHEMA IF NOT EXISTS HW32.RAW;        -- landing table, stage, file format
CREATE SCHEMA IF NOT EXISTS HW32.ANALYTICS;  -- dbt models (stg_products view, dim_products)
-- HW32.SNAPSHOTS is created by `dbt snapshot` (target_schema in products_snapshot.sql).

-- Load path used by the Airflow tasks extract (PUT) and load_staging (COPY INTO).
CREATE FILE FORMAT IF NOT EXISTS HW32.RAW.CSV_FMT
  TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE STAGE IF NOT EXISTS HW32.RAW.PRODUCTS_STAGE FILE_FORMAT = HW32.RAW.CSV_FMT;

-- Landing table: one column per CSV column (Kaggle Home Depot export, 14 columns).
-- This is the table as it exists today. The first version, written by hand, had typed price
-- and audit columns (_loaded_at, _source_file); it was replaced on Nov 20 by the Snowsight
-- load-wizard table so COPY maps the file 1:1. Typing and cleanup happen in dbt (stg_products).
CREATE TABLE IF NOT EXISTS HW32.RAW.STG_PRODUCTS_RAW (
    INDEX         NUMBER(38,0),
    URL           VARCHAR,
    TITLE         VARCHAR,
    IMAGES        VARCHAR,
    DESCRIPTION   VARCHAR,
    PRODUCT_ID    NUMBER(38,0),
    SKU           VARCHAR,
    GTIN13        VARCHAR,
    BRAND         VARCHAR,
    PRICE         FLOAT,
    CURRENCY      VARCHAR,
    AVAILABILITY  VARCHAR,
    UNIQ_ID       VARCHAR,
    SCRAPED_AT    TIMESTAMP_NTZ(9)
);

-- Key-pair auth for Airflow and dbt (the private key lives in airflow/include/rsa_key.p8,
-- which is not committed). Generate the pair with:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- then paste the body of rsa_key.pub (without the BEGIN/END lines):
ALTER USER <your_user> SET RSA_PUBLIC_KEY = '<rsa_key.pub body>';
