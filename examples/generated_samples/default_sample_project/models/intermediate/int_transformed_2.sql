{{
    config(
        materialized='table',
        tags=['intermediate']
    )
}}

-- Medium complexity intermediate model with transformations
with source_2 as (
    select * from {{ ref('stg_source_2') }}
),

source_3 as (
    select * from {{ ref('stg_source_3') }}
),

source_1 as (
    select * from {{ ref('stg_source_1') }}
),

transformed as (
    select
        source_2.stg_source_2_id as int_transformed_2_id,
        source_2.*,
        {{ dbt.current_timestamp() }} as transformed_at
    from source_2
)

select * from transformed
