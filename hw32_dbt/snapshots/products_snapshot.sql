{% snapshot products_snapshot %}
{{ config(
    target_schema='SNAPSHOTS',
    unique_key='product_id',
    strategy='check',
    check_cols=['title', 'price', 'availability', 'brand']
) }}
select * from {{ ref('stg_products') }}
{% endsnapshot %}
