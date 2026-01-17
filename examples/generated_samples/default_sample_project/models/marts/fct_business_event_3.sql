{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

-- Fact table for business analysis
with base as (
    select * from {{ ref('stg_source_1') }}
),

final as (
    select
        stg_source_1_id as fct_business_event_3_id,
        base.*,
        'fact' as model_type
    from base
)

select * from final
