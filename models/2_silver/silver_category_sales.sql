{{ config(
    materialized='table',
    tags=['silver']
) }}

with category_sales as (
    select
        c.category_value,
        c.pid,
        sum(v.price) as total_sales,
        avg(v.price) as avg_price,
        date_trunc('month', v.dw_insert_timestamp::timestamp) as month
    from {{ ref('bronze_rugs_usa_category_map') }} c
    join {{ ref('bronze_rugs_usa_parent') }} p on c.pid = p.pid
    join {{ ref('bronze_rugs_usa_variant') }} v on p.pid = v.pid
    group by c.category_value, c.pid, month
)

select *,
       lag(total_sales) over (partition by category_value order by month) as prev_month_sales,
       total_sales - lag(total_sales) over (partition by category_value order by month) as sales_diff,
       rank() over (partition by category_value order by total_sales desc) as sales_rank
from category_sales
