{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_32') }}
),

dep_2 as (
    select * from {{ ref('stg_source_4') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_6 as (
    select * from {{ ref('stg_source_109') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_8 as (
    select * from {{ ref('stg_source_160') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_11 as (
    select * from {{ ref('stg_source_44') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_13 as (
    select * from {{ ref('stg_source_143') }}
),

dep_14 as (
    select * from {{ ref('stg_source_95') }}
),

dep_15 as (
    select * from {{ ref('stg_source_58') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_17 as (
    select * from {{ ref('stg_source_118') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_20 as (
    select * from {{ ref('stg_source_92') }}
),

dep_21 as (
    select * from {{ ref('stg_source_25') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_23 as (
    select * from {{ ref('stg_source_101') }}
),

dep_24 as (
    select * from {{ ref('stg_source_142') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_26 as (
    select * from {{ ref('stg_source_37') }}
),

dep_27 as (
    select * from {{ ref('stg_source_128') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_29 as (
    select * from {{ ref('stg_source_69') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_31 as (
    select * from {{ ref('stg_source_8') }}
),

dep_32 as (
    select * from {{ ref('stg_source_43') }}
),

dep_33 as (
    select * from {{ ref('stg_source_6') }}
),

dep_34 as (
    select * from {{ ref('stg_source_38') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_37 as (
    select * from {{ ref('stg_source_29') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_40 as (
    select * from {{ ref('stg_source_3') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_42 as (
    select * from {{ ref('stg_source_33') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_44 as (
    select * from {{ ref('stg_source_71') }}
),

dep_45 as (
    select * from {{ ref('stg_source_45') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_48 as (
    select * from {{ ref('stg_source_112') }}
),

dep_49 as (
    select * from {{ ref('stg_source_70') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_61') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_32_id']) }} as surrogate_id,
        dep_1.stg_source_32_id as fct_business_event_151_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_32_id = dep_2.stg_source_4_id
    left join dep_3 on dep_1.stg_source_32_id = dep_3.int_transformed_17_id
    left join dep_4 on dep_1.stg_source_32_id = dep_4.int_transformed_70_id
    left join dep_5 on dep_1.stg_source_32_id = dep_5.int_transformed_161_id
    left join dep_6 on dep_1.stg_source_32_id = dep_6.stg_source_109_id
    left join dep_7 on dep_1.stg_source_32_id = dep_7.int_transformed_31_id
    left join dep_8 on dep_1.stg_source_32_id = dep_8.stg_source_160_id
    left join dep_9 on dep_1.stg_source_32_id = dep_9.int_transformed_2_id
    left join dep_10 on dep_1.stg_source_32_id = dep_10.int_transformed_60_id
    left join dep_11 on dep_1.stg_source_32_id = dep_11.stg_source_44_id
    left join dep_12 on dep_1.stg_source_32_id = dep_12.int_transformed_9_id
    left join dep_13 on dep_1.stg_source_32_id = dep_13.stg_source_143_id
    left join dep_14 on dep_1.stg_source_32_id = dep_14.stg_source_95_id
    left join dep_15 on dep_1.stg_source_32_id = dep_15.stg_source_58_id
    left join dep_16 on dep_1.stg_source_32_id = dep_16.int_transformed_112_id
    left join dep_17 on dep_1.stg_source_32_id = dep_17.stg_source_118_id
    left join dep_18 on dep_1.stg_source_32_id = dep_18.int_transformed_147_id
    left join dep_19 on dep_1.stg_source_32_id = dep_19.int_transformed_69_id
    left join dep_20 on dep_1.stg_source_32_id = dep_20.stg_source_92_id
    left join dep_21 on dep_1.stg_source_32_id = dep_21.stg_source_25_id
    left join dep_22 on dep_1.stg_source_32_id = dep_22.int_transformed_10_id
    left join dep_23 on dep_1.stg_source_32_id = dep_23.stg_source_101_id
    left join dep_24 on dep_1.stg_source_32_id = dep_24.stg_source_142_id
    left join dep_25 on dep_1.stg_source_32_id = dep_25.int_transformed_119_id
    left join dep_26 on dep_1.stg_source_32_id = dep_26.stg_source_37_id
    left join dep_27 on dep_1.stg_source_32_id = dep_27.stg_source_128_id
    left join dep_28 on dep_1.stg_source_32_id = dep_28.int_transformed_143_id
    left join dep_29 on dep_1.stg_source_32_id = dep_29.stg_source_69_id
    left join dep_30 on dep_1.stg_source_32_id = dep_30.int_transformed_77_id
    left join dep_31 on dep_1.stg_source_32_id = dep_31.stg_source_8_id
    left join dep_32 on dep_1.stg_source_32_id = dep_32.stg_source_43_id
    left join dep_33 on dep_1.stg_source_32_id = dep_33.stg_source_6_id
    left join dep_34 on dep_1.stg_source_32_id = dep_34.stg_source_38_id
    left join dep_35 on dep_1.stg_source_32_id = dep_35.int_transformed_53_id
    left join dep_36 on dep_1.stg_source_32_id = dep_36.int_transformed_124_id
    left join dep_37 on dep_1.stg_source_32_id = dep_37.stg_source_29_id
    left join dep_38 on dep_1.stg_source_32_id = dep_38.int_transformed_38_id
    left join dep_39 on dep_1.stg_source_32_id = dep_39.int_transformed_145_id
    left join dep_40 on dep_1.stg_source_32_id = dep_40.stg_source_3_id
    left join dep_41 on dep_1.stg_source_32_id = dep_41.int_transformed_162_id
    left join dep_42 on dep_1.stg_source_32_id = dep_42.stg_source_33_id
    left join dep_43 on dep_1.stg_source_32_id = dep_43.int_transformed_139_id
    left join dep_44 on dep_1.stg_source_32_id = dep_44.stg_source_71_id
    left join dep_45 on dep_1.stg_source_32_id = dep_45.stg_source_45_id
    left join dep_46 on dep_1.stg_source_32_id = dep_46.int_transformed_117_id
    left join dep_47 on dep_1.stg_source_32_id = dep_47.int_transformed_123_id
    left join dep_48 on dep_1.stg_source_32_id = dep_48.stg_source_112_id
    left join dep_49 on dep_1.stg_source_32_id = dep_49.stg_source_70_id
    left join dep_50 on dep_1.stg_source_32_id = dep_50.int_transformed_61_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
