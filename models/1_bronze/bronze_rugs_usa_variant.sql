{{ config(
    materialized='table',
    tags=['bronze']
) }}
select * from {{ source('rugs_source', 'rugs_usa_variant') }}