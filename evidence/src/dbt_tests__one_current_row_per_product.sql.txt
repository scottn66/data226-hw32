select product_id, count(*) as n
from {{ ref('dim_products') }}
where is_current
group by product_id
having count(*) <> 1
