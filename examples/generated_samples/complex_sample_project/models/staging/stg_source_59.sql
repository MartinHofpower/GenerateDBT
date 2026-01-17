{{
    config(
        materialized='view',
        tags=['staging', 'hourly']
    )
}}

-- Complex staging model with data quality checks
with source as (
    select * from {{ ref('raw_data_59') }}
),

cleaned as (
    select
        id,
        value,
        created_at,
        -- Data quality flags
        case
            when id is null then 'missing_id'
            when value is null then 'missing_value'
            else 'valid'
        end as data_quality_flag
    from source
),

renamed as (
    select
        id as stg_source_59_id,
        value as stg_source_59_value,
        created_at as stg_source_59_created_at,
        data_quality_flag,
        {{ dbt.current_timestamp() }} as loaded_at,
        '{{ run_started_at }}' as run_started_at
    from cleaned
    where data_quality_flag = 'valid'
)

select * from renamed
