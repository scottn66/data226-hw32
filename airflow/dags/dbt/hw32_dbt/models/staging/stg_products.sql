{{ config(materialized='view') }}
select
    product_id,
    trim(title)                              as title,
    brand,
    price::number(10,2)                      as price,
    currency,
    availability,
    regexp_replace(sku, '\\.0$', '')         as sku,
    regexp_replace(gtin13, '\\.0$', '')      as gtin13,
    url,
    description,
    scraped_at
from {{ source('raw', 'stg_products_raw') }}
