{{ config(
    materialized='table',
    tags=['silver']
) }}

with top_categories as (
    select
        c.category_value,
        sum(v.price) as total_sales
    from {{ ref('bronze_rugs_usa_category_map') }} c
    join {{ ref('bronze_rugs_usa_parent') }} p on c.pid = p.pid
    join {{ ref('bronze_rugs_usa_variant') }} v on p.pid = v.pid
    group by c.category_value
)

select *
from top_categories
