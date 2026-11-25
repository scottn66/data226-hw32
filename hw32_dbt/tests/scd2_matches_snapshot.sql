-- The validity window dim_products computes with LEAD() must match the one dbt's snapshot recorded.
select d.product_sk
from {{ ref('dim_products') }} d
join {{ ref('products_snapshot') }} s
  on s.dbt_scd_id = d.product_sk
where d.valid_to <> coalesce(s.dbt_valid_to, '9999-12-31'::timestamp_ntz)
   or d.is_current <> (s.dbt_valid_to is null)
