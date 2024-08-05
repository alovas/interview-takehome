{{ config(
    materialized='table',
    tags=['silver']
) }}

with sales_by_origin as (
    select
        p.origin,
        sum(v.price) as total_sales,
        avg(v.price) as avg_price,
        sum(v.stock_level) as total_stock
    from {{ ref('bronze_rugs_usa_parent') }} p
    join {{ ref('bronze_rugs_usa_variant') }} v on p.pid = v.pid
    group by p.origin
)

select *
from sales_by_origin
