{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

-- Medium complexity staging model
with source as (
    select * from {{ ref('raw_data_3') }}
),

renamed as (
    select
        id as stg_source_3_id,
        value as stg_source_3_value,
        created_at as stg_source_3_created_at,
        {{ dbt.current_timestamp() }} as loaded_at
    from source
)

select * from renamed
