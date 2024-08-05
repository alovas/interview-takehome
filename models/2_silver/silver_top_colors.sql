{{ config(
    materialized='table',
    tags=['silver']
) }}

with top_colors as (
    select
        c.color_value,
        sum(v.price) as total_sales
    from {{ ref('bronze_rugs_usa_color_map') }} c
    join {{ ref('bronze_rugs_usa_parent') }} p on c.pid = p.pid
    join {{ ref('bronze_rugs_usa_variant') }} v on p.pid = v.pid
    group by c.color_value
)

select *
from top_colors
