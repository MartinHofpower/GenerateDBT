{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_155') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_4 as (
    select * from {{ ref('stg_source_21') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_8 as (
    select * from {{ ref('stg_source_161') }}
),

dep_9 as (
    select * from {{ ref('stg_source_129') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_11 as (
    select * from {{ ref('stg_source_165') }}
),

dep_12 as (
    select * from {{ ref('stg_source_81') }}
),

dep_13 as (
    select * from {{ ref('stg_source_48') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_16 as (
    select * from {{ ref('stg_source_57') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_19 as (
    select * from {{ ref('stg_source_123') }}
),

dep_20 as (
    select * from {{ ref('stg_source_162') }}
),

dep_21 as (
    select * from {{ ref('stg_source_156') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_23 as (
    select * from {{ ref('stg_source_120') }}
),

dep_24 as (
    select * from {{ ref('stg_source_33') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_27 as (
    select * from {{ ref('stg_source_74') }}
),

dep_28 as (
    select * from {{ ref('stg_source_13') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_33 as (
    select * from {{ ref('stg_source_147') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_36 as (
    select * from {{ ref('stg_source_100') }}
),

dep_37 as (
    select * from {{ ref('stg_source_153') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_39 as (
    select * from {{ ref('stg_source_42') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_41 as (
    select * from {{ ref('stg_source_30') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_43 as (
    select * from {{ ref('stg_source_69') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_45 as (
    select * from {{ ref('stg_source_26') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_48 as (
    select * from {{ ref('stg_source_157') }}
),

dep_49 as (
    select * from {{ ref('stg_source_126') }}
),

dep_50 as (
    select * from {{ ref('stg_source_76') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_155_id']) }} as surrogate_id,
        dep_1.stg_source_155_id as dim_entity_120_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_155_id = dep_2.int_transformed_49_id
    left join dep_3 on dep_1.stg_source_155_id = dep_3.int_transformed_32_id
    left join dep_4 on dep_1.stg_source_155_id = dep_4.stg_source_21_id
    left join dep_5 on dep_1.stg_source_155_id = dep_5.int_transformed_61_id
    left join dep_6 on dep_1.stg_source_155_id = dep_6.int_transformed_142_id
    left join dep_7 on dep_1.stg_source_155_id = dep_7.int_transformed_139_id
    left join dep_8 on dep_1.stg_source_155_id = dep_8.stg_source_161_id
    left join dep_9 on dep_1.stg_source_155_id = dep_9.stg_source_129_id
    left join dep_10 on dep_1.stg_source_155_id = dep_10.int_transformed_73_id
    left join dep_11 on dep_1.stg_source_155_id = dep_11.stg_source_165_id
    left join dep_12 on dep_1.stg_source_155_id = dep_12.stg_source_81_id
    left join dep_13 on dep_1.stg_source_155_id = dep_13.stg_source_48_id
    left join dep_14 on dep_1.stg_source_155_id = dep_14.int_transformed_31_id
    left join dep_15 on dep_1.stg_source_155_id = dep_15.int_transformed_24_id
    left join dep_16 on dep_1.stg_source_155_id = dep_16.stg_source_57_id
    left join dep_17 on dep_1.stg_source_155_id = dep_17.int_transformed_128_id
    left join dep_18 on dep_1.stg_source_155_id = dep_18.int_transformed_44_id
    left join dep_19 on dep_1.stg_source_155_id = dep_19.stg_source_123_id
    left join dep_20 on dep_1.stg_source_155_id = dep_20.stg_source_162_id
    left join dep_21 on dep_1.stg_source_155_id = dep_21.stg_source_156_id
    left join dep_22 on dep_1.stg_source_155_id = dep_22.int_transformed_38_id
    left join dep_23 on dep_1.stg_source_155_id = dep_23.stg_source_120_id
    left join dep_24 on dep_1.stg_source_155_id = dep_24.stg_source_33_id
    left join dep_25 on dep_1.stg_source_155_id = dep_25.int_transformed_43_id
    left join dep_26 on dep_1.stg_source_155_id = dep_26.int_transformed_130_id
    left join dep_27 on dep_1.stg_source_155_id = dep_27.stg_source_74_id
    left join dep_28 on dep_1.stg_source_155_id = dep_28.stg_source_13_id
    left join dep_29 on dep_1.stg_source_155_id = dep_29.int_transformed_153_id
    left join dep_30 on dep_1.stg_source_155_id = dep_30.int_transformed_67_id
    left join dep_31 on dep_1.stg_source_155_id = dep_31.int_transformed_68_id
    left join dep_32 on dep_1.stg_source_155_id = dep_32.int_transformed_96_id
    left join dep_33 on dep_1.stg_source_155_id = dep_33.stg_source_147_id
    left join dep_34 on dep_1.stg_source_155_id = dep_34.int_transformed_123_id
    left join dep_35 on dep_1.stg_source_155_id = dep_35.int_transformed_110_id
    left join dep_36 on dep_1.stg_source_155_id = dep_36.stg_source_100_id
    left join dep_37 on dep_1.stg_source_155_id = dep_37.stg_source_153_id
    left join dep_38 on dep_1.stg_source_155_id = dep_38.int_transformed_94_id
    left join dep_39 on dep_1.stg_source_155_id = dep_39.stg_source_42_id
    left join dep_40 on dep_1.stg_source_155_id = dep_40.int_transformed_145_id
    left join dep_41 on dep_1.stg_source_155_id = dep_41.stg_source_30_id
    left join dep_42 on dep_1.stg_source_155_id = dep_42.int_transformed_36_id
    left join dep_43 on dep_1.stg_source_155_id = dep_43.stg_source_69_id
    left join dep_44 on dep_1.stg_source_155_id = dep_44.int_transformed_143_id
    left join dep_45 on dep_1.stg_source_155_id = dep_45.stg_source_26_id
    left join dep_46 on dep_1.stg_source_155_id = dep_46.int_transformed_106_id
    left join dep_47 on dep_1.stg_source_155_id = dep_47.int_transformed_103_id
    left join dep_48 on dep_1.stg_source_155_id = dep_48.stg_source_157_id
    left join dep_49 on dep_1.stg_source_155_id = dep_49.stg_source_126_id
    left join dep_50 on dep_1.stg_source_155_id = dep_50.stg_source_76_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
