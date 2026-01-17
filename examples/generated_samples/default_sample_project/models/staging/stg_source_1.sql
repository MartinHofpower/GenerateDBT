{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

-- Medium complexity staging model
with source as (
    select * from {{ ref('raw_data_1') }}
),

renamed as (
    select
        id as stg_source_1_id,
        value as stg_source_1_value,
        created_at as stg_source_1_created_at,
        {{ dbt.current_timestamp() }} as loaded_at
    from source
)

select * from renamed
