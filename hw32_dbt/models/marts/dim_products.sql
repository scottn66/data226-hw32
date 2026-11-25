{{ config(materialized='table') }}
-- SCD Type 2 product dimension.
-- products_snapshot captures a new row whenever title, price, availability or brand changes.
-- This model derives each version's validity window itself with window functions
-- (tests/scd2_matches_snapshot.sql checks it agrees with dbt's own dbt_valid_to).

with versions as (
    select
        dbt_scd_id      as product_sk,
        product_id,
        title,
        brand,
        price,
        availability,
        dbt_valid_from  as valid_from
    from {{ ref('products_snapshot') }}
),

windowed as (
    select
        *,
        row_number()     over (partition by product_id order by valid_from) as version,
        lag(price)       over (partition by product_id order by valid_from) as previous_price,
        lag(title)       over (partition by product_id order by valid_from) as previous_title,
        lead(valid_from) over (partition by product_id order by valid_from) as next_valid_from
    from versions
)

select
    w.product_sk,
    w.product_id,
    w.version,
    w.title,
    w.brand,
    w.price,
    w.availability,
{%- if var('enable_rating', false) %}
    r.rating,                                   -- 9.1 feature: current rating, joined to every version
{%- endif %}
    w.previous_price,
    case
        when w.version = 1               then 'initial'
        when w.price <> w.previous_price then 'price_change'
        when w.title <> w.previous_title then 'title_change'
        else 'other_change'
    end                                                       as change_type,
    w.valid_from,
    coalesce(w.next_valid_from, '9999-12-31'::timestamp_ntz)  as valid_to,
    w.next_valid_from is null                                 as is_current
from windowed w
{%- if var('enable_rating', false) %}
left join {{ source('raw', 'product_ratings') }} r
    on r.product_id = w.product_id
{%- endif %}
