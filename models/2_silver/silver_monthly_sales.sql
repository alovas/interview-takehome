{{ config(
    materialized='table',
    tags=['silver']
) }}

with monthly_sales as (
    select
        date_trunc('month', v.dw_insert_timestamp::timestamp) as month,
        sum(v.price) as total_sales,
        avg(v.price) as avg_price
    from {{ ref('bronze_rugs_usa_variant') }} v
    group by month
)

select *,
       lag(total_sales) over (order by month) as prev_month_sales,
       total_sales - lag(total_sales) over (order by month) as sales_diff
from monthly_sales
