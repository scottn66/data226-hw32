{{ config(enabled=var('enable_rating', false)) }}
-- 9.1: every product gets a rating, and ratings stay on the 1-5 scale.
select product_id, rating
from {{ ref('dim_products') }}
where is_current
  and (rating is null or rating not between 1 and 5)
