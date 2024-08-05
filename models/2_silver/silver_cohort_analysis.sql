{{ config(
    materialized='table',
    tags=['silver']
) }}

with cohort_analysis as (
    select
        v.pid,
        date_trunc('month', v.dw_insert_timestamp::timestamp) as cohort_month,
        count(distinct v.variant) as variant_count,
        sum(v.price) as total_sales
    from {{ ref('bronze_rugs_usa_variant') }} v
    group by v.pid, cohort_month
)

select *,
       rank() over (partition by cohort_month order by total_sales desc) as cohort_rank
from cohort_analysis
