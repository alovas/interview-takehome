{{ config(
    materialized='view',
    tags=['gold']
) }}

select
    month,
    sum(total_sales) as monthly_sales,
    avg(avg_price) as avg_sales_price,
    lag(sum(total_sales)) over (order by month) as prev_month_sales,
    sum(total_sales) - lag(sum(total_sales)) over (order by month) as sales_diff
from {{ ref('silver_monthly_sales') }}
group by month
order by month
