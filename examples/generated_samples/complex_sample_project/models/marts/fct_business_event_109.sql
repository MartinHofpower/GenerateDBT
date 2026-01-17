{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_21') }}
),

dep_2 as (
    select * from {{ ref('stg_source_38') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_5 as (
    select * from {{ ref('stg_source_63') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_8 as (
    select * from {{ ref('stg_source_135') }}
),

dep_9 as (
    select * from {{ ref('stg_source_44') }}
),

dep_10 as (
    select * from {{ ref('stg_source_64') }}
),

dep_11 as (
    select * from {{ ref('stg_source_80') }}
),

dep_12 as (
    select * from {{ ref('stg_source_155') }}
),

dep_13 as (
    select * from {{ ref('stg_source_102') }}
),

dep_14 as (
    select * from {{ ref('stg_source_148') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_16 as (
    select * from {{ ref('stg_source_98') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_20 as (
    select * from {{ ref('stg_source_41') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_22 as (
    select * from {{ ref('stg_source_78') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_25 as (
    select * from {{ ref('stg_source_113') }}
),

dep_26 as (
    select * from {{ ref('stg_source_124') }}
),

dep_27 as (
    select * from {{ ref('stg_source_5') }}
),

dep_28 as (
    select * from {{ ref('stg_source_121') }}
),

dep_29 as (
    select * from {{ ref('stg_source_60') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_34 as (
    select * from {{ ref('stg_source_72') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_37 as (
    select * from {{ ref('stg_source_100') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_39 as (
    select * from {{ ref('stg_source_127') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_41 as (
    select * from {{ ref('stg_source_108') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_43 as (
    select * from {{ ref('stg_source_12') }}
),

dep_44 as (
    select * from {{ ref('stg_source_32') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_46 as (
    select * from {{ ref('stg_source_13') }}
),

dep_47 as (
    select * from {{ ref('stg_source_104') }}
),

dep_48 as (
    select * from {{ ref('stg_source_133') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_118') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_21_id']) }} as surrogate_id,
        dep_1.int_transformed_21_id as fct_business_event_109_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_21_id = dep_2.stg_source_38_id
    left join dep_3 on dep_1.int_transformed_21_id = dep_3.int_transformed_53_id
    left join dep_4 on dep_1.int_transformed_21_id = dep_4.int_transformed_44_id
    left join dep_5 on dep_1.int_transformed_21_id = dep_5.stg_source_63_id
    left join dep_6 on dep_1.int_transformed_21_id = dep_6.int_transformed_161_id
    left join dep_7 on dep_1.int_transformed_21_id = dep_7.int_transformed_58_id
    left join dep_8 on dep_1.int_transformed_21_id = dep_8.stg_source_135_id
    left join dep_9 on dep_1.int_transformed_21_id = dep_9.stg_source_44_id
    left join dep_10 on dep_1.int_transformed_21_id = dep_10.stg_source_64_id
    left join dep_11 on dep_1.int_transformed_21_id = dep_11.stg_source_80_id
    left join dep_12 on dep_1.int_transformed_21_id = dep_12.stg_source_155_id
    left join dep_13 on dep_1.int_transformed_21_id = dep_13.stg_source_102_id
    left join dep_14 on dep_1.int_transformed_21_id = dep_14.stg_source_148_id
    left join dep_15 on dep_1.int_transformed_21_id = dep_15.int_transformed_36_id
    left join dep_16 on dep_1.int_transformed_21_id = dep_16.stg_source_98_id
    left join dep_17 on dep_1.int_transformed_21_id = dep_17.int_transformed_156_id
    left join dep_18 on dep_1.int_transformed_21_id = dep_18.int_transformed_14_id
    left join dep_19 on dep_1.int_transformed_21_id = dep_19.int_transformed_46_id
    left join dep_20 on dep_1.int_transformed_21_id = dep_20.stg_source_41_id
    left join dep_21 on dep_1.int_transformed_21_id = dep_21.int_transformed_101_id
    left join dep_22 on dep_1.int_transformed_21_id = dep_22.stg_source_78_id
    left join dep_23 on dep_1.int_transformed_21_id = dep_23.int_transformed_15_id
    left join dep_24 on dep_1.int_transformed_21_id = dep_24.int_transformed_51_id
    left join dep_25 on dep_1.int_transformed_21_id = dep_25.stg_source_113_id
    left join dep_26 on dep_1.int_transformed_21_id = dep_26.stg_source_124_id
    left join dep_27 on dep_1.int_transformed_21_id = dep_27.stg_source_5_id
    left join dep_28 on dep_1.int_transformed_21_id = dep_28.stg_source_121_id
    left join dep_29 on dep_1.int_transformed_21_id = dep_29.stg_source_60_id
    left join dep_30 on dep_1.int_transformed_21_id = dep_30.int_transformed_26_id
    left join dep_31 on dep_1.int_transformed_21_id = dep_31.int_transformed_74_id
    left join dep_32 on dep_1.int_transformed_21_id = dep_32.int_transformed_40_id
    left join dep_33 on dep_1.int_transformed_21_id = dep_33.int_transformed_158_id
    left join dep_34 on dep_1.int_transformed_21_id = dep_34.stg_source_72_id
    left join dep_35 on dep_1.int_transformed_21_id = dep_35.int_transformed_142_id
    left join dep_36 on dep_1.int_transformed_21_id = dep_36.int_transformed_57_id
    left join dep_37 on dep_1.int_transformed_21_id = dep_37.stg_source_100_id
    left join dep_38 on dep_1.int_transformed_21_id = dep_38.int_transformed_160_id
    left join dep_39 on dep_1.int_transformed_21_id = dep_39.stg_source_127_id
    left join dep_40 on dep_1.int_transformed_21_id = dep_40.int_transformed_67_id
    left join dep_41 on dep_1.int_transformed_21_id = dep_41.stg_source_108_id
    left join dep_42 on dep_1.int_transformed_21_id = dep_42.int_transformed_2_id
    left join dep_43 on dep_1.int_transformed_21_id = dep_43.stg_source_12_id
    left join dep_44 on dep_1.int_transformed_21_id = dep_44.stg_source_32_id
    left join dep_45 on dep_1.int_transformed_21_id = dep_45.int_transformed_4_id
    left join dep_46 on dep_1.int_transformed_21_id = dep_46.stg_source_13_id
    left join dep_47 on dep_1.int_transformed_21_id = dep_47.stg_source_104_id
    left join dep_48 on dep_1.int_transformed_21_id = dep_48.stg_source_133_id
    left join dep_49 on dep_1.int_transformed_21_id = dep_49.int_transformed_150_id
    left join dep_50 on dep_1.int_transformed_21_id = dep_50.int_transformed_118_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
