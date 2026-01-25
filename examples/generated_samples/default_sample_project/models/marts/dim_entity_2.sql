{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

-- Dimension table for business analysis
with base as (
    select * from {{ ref('stg_source_2') }}
),

final as (
    select
        stg_source_2_id as dim_entity_2_id,
        base.*,
        'dimension' as model_type
    from base
)

select * from final
