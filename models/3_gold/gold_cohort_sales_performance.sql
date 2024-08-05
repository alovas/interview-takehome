{{ config(
    materialized='view',
    tags=['gold']
) }}

select
    cohort_month,
    sum(total_sales) as total_sales,
    sum(variant_count) as total_variants,
    rank() over (partition by cohort_month order by sum(total_sales) desc) as cohort_rank
from {{ ref('silver_cohort_analysis') }}
group by cohort_month
order by cohort_month
