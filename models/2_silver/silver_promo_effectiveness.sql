{{ config(
    materialized='table',
    tags=['silver']
) }}

with promo_effectiveness as (
    select
        c.category_value,
        sum(case when p.clearance = 'Y' then v.price else 0 end) as promo_sales,
        sum(case when p.clearance = 'N' then v.price else 0 end) as regular_sales
    from {{ ref('bronze_rugs_usa_category_map') }} c
    join {{ ref('bronze_rugs_usa_parent') }} p on c.pid = p.pid
    join {{ ref('bronze_rugs_usa_variant') }} v on p.pid = v.pid
    group by c.category_value
)

select *
from promo_effectiveness
