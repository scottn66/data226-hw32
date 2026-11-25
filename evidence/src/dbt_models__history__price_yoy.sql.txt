{{ config(materialized='table', enabled=var('build_yoy', false)) }}
-- 9.2 year-over-year: the catalog as frozen by the year-end clone vs. the catalog today.
-- Built only by hw32_eoy_snapshot, which first clones PRODUCTS_SNAPSHOT AT(TIMESTAMP => year end)
-- and points the PRODUCTS_SNAPSHOT_FROZEN view at that clone.

with then_catalog as (
    select frozen_label, product_id, brand, title, price
    from {{ source('snapshots', 'products_snapshot_frozen') }}
    where dbt_valid_to is null                      -- versions current at clone time
),

now_catalog as (
    select product_id, brand, title, price
    from {{ ref('dim_products') }}
    where is_current
)

select
    coalesce(n.product_id, t.product_id)                       as product_id,
    coalesce(n.brand, t.brand)                                 as brand,
    t.price                                                    as price_then,
    n.price                                                    as price_now,
    n.price - t.price                                          as price_change,
    round(100 * (n.price - t.price) / nullif(t.price, 0), 2)   as pct_change,
    case
        when t.product_id is null then 'added'
        when n.product_id is null then 'removed'
        when n.price <> t.price   then 'repriced'
        when n.title <> t.title   then 'retitled'
        else 'unchanged'
    end                                                        as status,
    (select max(frozen_label) from then_catalog)               as compared_to
from now_catalog n
full outer join then_catalog t
    on t.product_id = n.product_id
