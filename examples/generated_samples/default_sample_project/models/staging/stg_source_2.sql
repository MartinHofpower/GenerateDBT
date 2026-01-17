{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

-- Medium complexity staging model
with source as (
    select * from {{ ref('raw_data_2') }}
),

renamed as (
    select
        id as stg_source_2_id,
        value as stg_source_2_value,
        created_at as stg_source_2_created_at,
        {{ dbt.current_timestamp() }} as loaded_at
    from source
)

select * from renamed
