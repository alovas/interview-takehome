{{ config(
    materialized='table',
    tags=['silver']
) }}

with variant_performance as (
    select
        v.variant,
        v.pid,
        sum(v.price) as total_sales,
        avg(v.price) as avg_price,
        sum(v.stock_level) as total_stock
    from {{ ref('bronze_rugs_usa_variant') }} v
    group by v.variant, v.pid
)

select *,
       dense_rank() over (partition by variant order by total_sales desc) as sales_rank
from variant_performance
