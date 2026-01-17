{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

-- Fact table for business analysis
with base as (
    select * from {{ ref('int_transformed_3') }}
),

final as (
    select
        int_transformed_3_id as fct_business_event_1_id,
        base.*,
        'fact' as model_type
    from base
)

select * from final
