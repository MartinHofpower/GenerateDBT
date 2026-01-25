{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_151') }}
),

dep_2 as (
    select * from {{ ref('stg_source_93') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_8 as (
    select * from {{ ref('stg_source_146') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_10 as (
    select * from {{ ref('stg_source_152') }}
),

dep_11 as (
    select * from {{ ref('stg_source_39') }}
),

dep_12 as (
    select * from {{ ref('stg_source_144') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_14 as (
    select * from {{ ref('stg_source_137') }}
),

dep_15 as (
    select * from {{ ref('stg_source_24') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_17 as (
    select * from {{ ref('stg_source_85') }}
),

dep_18 as (
    select * from {{ ref('stg_source_67') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_25 as (
    select * from {{ ref('stg_source_60') }}
),

dep_26 as (
    select * from {{ ref('stg_source_8') }}
),

dep_27 as (
    select * from {{ ref('stg_source_116') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_30 as (
    select * from {{ ref('stg_source_157') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_34 as (
    select * from {{ ref('stg_source_128') }}
),

dep_35 as (
    select * from {{ ref('stg_source_88') }}
),

dep_36 as (
    select * from {{ ref('stg_source_82') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_38 as (
    select * from {{ ref('stg_source_138') }}
),

dep_39 as (
    select * from {{ ref('stg_source_45') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_41 as (
    select * from {{ ref('stg_source_150') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_44 as (
    select * from {{ ref('stg_source_96') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_46 as (
    select * from {{ ref('stg_source_139') }}
),

dep_47 as (
    select * from {{ ref('stg_source_160') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_50 as (
    select * from {{ ref('stg_source_66') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_151_id']) }} as surrogate_id,
        dep_1.stg_source_151_id as fct_business_event_1_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_151_id = dep_2.stg_source_93_id
    left join dep_3 on dep_1.stg_source_151_id = dep_3.int_transformed_146_id
    left join dep_4 on dep_1.stg_source_151_id = dep_4.int_transformed_74_id
    left join dep_5 on dep_1.stg_source_151_id = dep_5.int_transformed_100_id
    left join dep_6 on dep_1.stg_source_151_id = dep_6.int_transformed_62_id
    left join dep_7 on dep_1.stg_source_151_id = dep_7.int_transformed_23_id
    left join dep_8 on dep_1.stg_source_151_id = dep_8.stg_source_146_id
    left join dep_9 on dep_1.stg_source_151_id = dep_9.int_transformed_20_id
    left join dep_10 on dep_1.stg_source_151_id = dep_10.stg_source_152_id
    left join dep_11 on dep_1.stg_source_151_id = dep_11.stg_source_39_id
    left join dep_12 on dep_1.stg_source_151_id = dep_12.stg_source_144_id
    left join dep_13 on dep_1.stg_source_151_id = dep_13.int_transformed_107_id
    left join dep_14 on dep_1.stg_source_151_id = dep_14.stg_source_137_id
    left join dep_15 on dep_1.stg_source_151_id = dep_15.stg_source_24_id
    left join dep_16 on dep_1.stg_source_151_id = dep_16.int_transformed_144_id
    left join dep_17 on dep_1.stg_source_151_id = dep_17.stg_source_85_id
    left join dep_18 on dep_1.stg_source_151_id = dep_18.stg_source_67_id
    left join dep_19 on dep_1.stg_source_151_id = dep_19.int_transformed_65_id
    left join dep_20 on dep_1.stg_source_151_id = dep_20.int_transformed_16_id
    left join dep_21 on dep_1.stg_source_151_id = dep_21.int_transformed_61_id
    left join dep_22 on dep_1.stg_source_151_id = dep_22.int_transformed_69_id
    left join dep_23 on dep_1.stg_source_151_id = dep_23.int_transformed_116_id
    left join dep_24 on dep_1.stg_source_151_id = dep_24.int_transformed_6_id
    left join dep_25 on dep_1.stg_source_151_id = dep_25.stg_source_60_id
    left join dep_26 on dep_1.stg_source_151_id = dep_26.stg_source_8_id
    left join dep_27 on dep_1.stg_source_151_id = dep_27.stg_source_116_id
    left join dep_28 on dep_1.stg_source_151_id = dep_28.int_transformed_7_id
    left join dep_29 on dep_1.stg_source_151_id = dep_29.int_transformed_101_id
    left join dep_30 on dep_1.stg_source_151_id = dep_30.stg_source_157_id
    left join dep_31 on dep_1.stg_source_151_id = dep_31.int_transformed_134_id
    left join dep_32 on dep_1.stg_source_151_id = dep_32.int_transformed_29_id
    left join dep_33 on dep_1.stg_source_151_id = dep_33.int_transformed_95_id
    left join dep_34 on dep_1.stg_source_151_id = dep_34.stg_source_128_id
    left join dep_35 on dep_1.stg_source_151_id = dep_35.stg_source_88_id
    left join dep_36 on dep_1.stg_source_151_id = dep_36.stg_source_82_id
    left join dep_37 on dep_1.stg_source_151_id = dep_37.int_transformed_123_id
    left join dep_38 on dep_1.stg_source_151_id = dep_38.stg_source_138_id
    left join dep_39 on dep_1.stg_source_151_id = dep_39.stg_source_45_id
    left join dep_40 on dep_1.stg_source_151_id = dep_40.int_transformed_36_id
    left join dep_41 on dep_1.stg_source_151_id = dep_41.stg_source_150_id
    left join dep_42 on dep_1.stg_source_151_id = dep_42.int_transformed_85_id
    left join dep_43 on dep_1.stg_source_151_id = dep_43.int_transformed_160_id
    left join dep_44 on dep_1.stg_source_151_id = dep_44.stg_source_96_id
    left join dep_45 on dep_1.stg_source_151_id = dep_45.int_transformed_66_id
    left join dep_46 on dep_1.stg_source_151_id = dep_46.stg_source_139_id
    left join dep_47 on dep_1.stg_source_151_id = dep_47.stg_source_160_id
    left join dep_48 on dep_1.stg_source_151_id = dep_48.int_transformed_17_id
    left join dep_49 on dep_1.stg_source_151_id = dep_49.int_transformed_55_id
    left join dep_50 on dep_1.stg_source_151_id = dep_50.stg_source_66_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
