{{ config(
    materialized='table',
    tags=['silver']
) }}

with inventory_levels as (
    select
        p.pid,
        p.name,
        v.stock_level,
        v.depletion_level,
        v.estimated_delivery_date,
        v.dw_insert_timestamp::timestamp as dw_insert_timestamp
    from {{ ref('bronze_rugs_usa_parent') }} p
    join {{ ref('bronze_rugs_usa_variant') }} v on p.pid = v.pid
)

select *
from inventory_levels
