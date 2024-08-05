{{ config(
    materialized='view',
    tags=['gold']
) }}

select
    category_value,
    month,
    sum(total_sales) as monthly_sales,
    rank() over (partition by category_value order by month) as sales_rank
from {{ ref('silver_category_sales') }}
group by category_value, month
order by category_value, month
