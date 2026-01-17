{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

-- Dimension table for business analysis
with base as (
    select * from {{ ref('stg_source_1') }}
),

final as (
    select
        stg_source_1_id as dim_entity_4_id,
        base.*,
        'dimension' as model_type
    from base
)

select * from final
