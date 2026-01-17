{{
    config(
        materialized='table',
        tags=['intermediate']
    )
}}

-- Medium complexity intermediate model with transformations
with source_3 as (
    select * from {{ ref('stg_source_3') }}
),

source_1 as (
    select * from {{ ref('stg_source_1') }}
),

source_2 as (
    select * from {{ ref('stg_source_2') }}
),

transformed as (
    select
        source_3.stg_source_3_id as int_transformed_2_id,
        source_3.*,
        {{ dbt.current_timestamp() }} as transformed_at
    from source_3
)

select * from transformed
