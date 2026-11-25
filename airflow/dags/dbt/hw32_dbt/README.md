# hw32_dbt

dbt project for DATA 226 HW 3.2 (Home Depot product catalog, SCD Type 2 on Snowflake).
Airflow runs it through Cosmos; the DAGs are listed in the README at the hw32 root.

| Node | Target / flag | What it does |
|---|---|---|
| `stg_products` (view) | all | Casts price, trims title, strips `.0` from SKU/GTIN |
| `products_snapshot` | all | SCD2 change capture, `check` strategy on title, price, availability, brand |
| `dim_products` (table) | all | SCD2 dimension: `version`, `previous_price`, `change_type`, `valid_from`/`valid_to` via `LEAD()`, `is_current`; adds `rating` when `enable_rating` |
| `price_yoy` (table) | `build_yoy: true` | 9.2: frozen year-end catalog vs. today |
| `price_bands` (table) | target `proto` | 9.3 prototype, built only in `HW32.PROTOTYPE` |

Tests: `unique`/`not_null`/`accepted_values` in the `schema.yml` files, plus singular tests
`one_current_row_per_product`, `no_overlapping_validity`, `scd2_matches_snapshot` (LEAD-derived
window = dbt's `dbt_valid_to`) and `rating_in_range` (only with `enable_rating`).

Targets (`airflow/include/profiles.yml`): `prod` = HW32, `dev` = HW32_DEV (zero-copy clone),
`proto` = HW32.PROTOTYPE. Sources use `{{ target.database }}`, so a dev run reads the clone, not prod.
