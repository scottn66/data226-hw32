select a.product_id
from {{ ref('dim_products') }} a
join {{ ref('dim_products') }} b
  on a.product_id = b.product_id
 and a.product_sk <> b.product_sk
 and a.valid_from < b.valid_to
 and b.valid_from < a.valid_to
