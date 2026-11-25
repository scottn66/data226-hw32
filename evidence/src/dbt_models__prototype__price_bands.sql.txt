{{ config(materialized='table', enabled=(target.name == 'proto')) }}
-- 9.3 prototype, built only with the `proto` target, i.e. in HW32.PROTOTYPE, a zero-copy clone of
-- ANALYTICS that hw32_prototype creates and drops. ref('dim_products') therefore reads the clone.
-- Buckets the current catalog into price bands and uses the SCD2 history to show how much of each
-- band has been repriced.

with history as (
    select
        product_id,
        count(*) - 1                   as n_changes,
        count(distinct price) - 1      as n_price_changes
    from {{ ref('dim_products') }}
    group by product_id
),

current_products as (
    select d.product_id, d.brand, d.price, h.n_changes, h.n_price_changes
    from {{ ref('dim_products') }} d
    join history h
        on h.product_id = d.product_id
    where d.is_current
),

banded as (
    select
        *,
        case
            when price < 25  then '1: under $25'
            when price < 100 then '2: $25-99'
            when price < 500 then '3: $100-499'
            else                  '4: $500+'
        end as price_band
    from current_products
)

select
    price_band,
    count(*)                                                  as products,
    count(distinct brand)                                     as brands,
    min(price)                                                as min_price,
    median(price)                                             as median_price,
    max(price)                                                as max_price,
    round(100 * ratio_to_report(count(*)) over (), 1)         as pct_of_catalog,
    count_if(n_changes > 0)                                   as changed_products,
    count_if(n_price_changes > 0)                             as repriced_products,
    round(100 * count_if(n_price_changes > 0) / count(*), 1)  as pct_repriced
from banded
group by price_band
