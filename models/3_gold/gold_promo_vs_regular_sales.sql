{{ config(
    materialized='view',
    tags=['gold']
) }}

select
    category_value,
    sum(promo_sales) as promo_sales,
    sum(regular_sales) as regular_sales,
    dense_rank() over (order by sum(promo_sales) desc) as promo_sales_rank,
    dense_rank() over (order by sum(regular_sales) desc) as regular_sales_rank
from {{ ref('silver_promo_effectiveness') }}
group by category_value
