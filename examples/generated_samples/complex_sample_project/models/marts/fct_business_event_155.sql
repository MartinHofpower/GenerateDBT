{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_88') }}
),

dep_2 as (
    select * from {{ ref('stg_source_70') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_4 as (
    select * from {{ ref('stg_source_67') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_8 as (
    select * from {{ ref('stg_source_139') }}
),

dep_9 as (
    select * from {{ ref('stg_source_147') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_13 as (
    select * from {{ ref('stg_source_143') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_15 as (
    select * from {{ ref('stg_source_165') }}
),

dep_16 as (
    select * from {{ ref('stg_source_103') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_21 as (
    select * from {{ ref('stg_source_60') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_23 as (
    select * from {{ ref('stg_source_125') }}
),

dep_24 as (
    select * from {{ ref('stg_source_136') }}
),

dep_25 as (
    select * from {{ ref('stg_source_85') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_27 as (
    select * from {{ ref('stg_source_49') }}
),

dep_28 as (
    select * from {{ ref('stg_source_137') }}
),

dep_29 as (
    select * from {{ ref('stg_source_151') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_32 as (
    select * from {{ ref('stg_source_84') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_35 as (
    select * from {{ ref('stg_source_2') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_38 as (
    select * from {{ ref('stg_source_101') }}
),

dep_39 as (
    select * from {{ ref('stg_source_90') }}
),

dep_40 as (
    select * from {{ ref('stg_source_62') }}
),

dep_41 as (
    select * from {{ ref('stg_source_43') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_43 as (
    select * from {{ ref('stg_source_27') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_50 as (
    select * from {{ ref('stg_source_17') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_88_id']) }} as surrogate_id,
        dep_1.stg_source_88_id as fct_business_event_155_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_88_id = dep_2.stg_source_70_id
    left join dep_3 on dep_1.stg_source_88_id = dep_3.int_transformed_87_id
    left join dep_4 on dep_1.stg_source_88_id = dep_4.stg_source_67_id
    left join dep_5 on dep_1.stg_source_88_id = dep_5.int_transformed_28_id
    left join dep_6 on dep_1.stg_source_88_id = dep_6.int_transformed_80_id
    left join dep_7 on dep_1.stg_source_88_id = dep_7.int_transformed_166_id
    left join dep_8 on dep_1.stg_source_88_id = dep_8.stg_source_139_id
    left join dep_9 on dep_1.stg_source_88_id = dep_9.stg_source_147_id
    left join dep_10 on dep_1.stg_source_88_id = dep_10.int_transformed_161_id
    left join dep_11 on dep_1.stg_source_88_id = dep_11.int_transformed_82_id
    left join dep_12 on dep_1.stg_source_88_id = dep_12.int_transformed_47_id
    left join dep_13 on dep_1.stg_source_88_id = dep_13.stg_source_143_id
    left join dep_14 on dep_1.stg_source_88_id = dep_14.int_transformed_70_id
    left join dep_15 on dep_1.stg_source_88_id = dep_15.stg_source_165_id
    left join dep_16 on dep_1.stg_source_88_id = dep_16.stg_source_103_id
    left join dep_17 on dep_1.stg_source_88_id = dep_17.int_transformed_128_id
    left join dep_18 on dep_1.stg_source_88_id = dep_18.int_transformed_30_id
    left join dep_19 on dep_1.stg_source_88_id = dep_19.int_transformed_96_id
    left join dep_20 on dep_1.stg_source_88_id = dep_20.int_transformed_76_id
    left join dep_21 on dep_1.stg_source_88_id = dep_21.stg_source_60_id
    left join dep_22 on dep_1.stg_source_88_id = dep_22.int_transformed_140_id
    left join dep_23 on dep_1.stg_source_88_id = dep_23.stg_source_125_id
    left join dep_24 on dep_1.stg_source_88_id = dep_24.stg_source_136_id
    left join dep_25 on dep_1.stg_source_88_id = dep_25.stg_source_85_id
    left join dep_26 on dep_1.stg_source_88_id = dep_26.int_transformed_99_id
    left join dep_27 on dep_1.stg_source_88_id = dep_27.stg_source_49_id
    left join dep_28 on dep_1.stg_source_88_id = dep_28.stg_source_137_id
    left join dep_29 on dep_1.stg_source_88_id = dep_29.stg_source_151_id
    left join dep_30 on dep_1.stg_source_88_id = dep_30.int_transformed_26_id
    left join dep_31 on dep_1.stg_source_88_id = dep_31.int_transformed_71_id
    left join dep_32 on dep_1.stg_source_88_id = dep_32.stg_source_84_id
    left join dep_33 on dep_1.stg_source_88_id = dep_33.int_transformed_12_id
    left join dep_34 on dep_1.stg_source_88_id = dep_34.int_transformed_97_id
    left join dep_35 on dep_1.stg_source_88_id = dep_35.stg_source_2_id
    left join dep_36 on dep_1.stg_source_88_id = dep_36.int_transformed_133_id
    left join dep_37 on dep_1.stg_source_88_id = dep_37.int_transformed_54_id
    left join dep_38 on dep_1.stg_source_88_id = dep_38.stg_source_101_id
    left join dep_39 on dep_1.stg_source_88_id = dep_39.stg_source_90_id
    left join dep_40 on dep_1.stg_source_88_id = dep_40.stg_source_62_id
    left join dep_41 on dep_1.stg_source_88_id = dep_41.stg_source_43_id
    left join dep_42 on dep_1.stg_source_88_id = dep_42.int_transformed_120_id
    left join dep_43 on dep_1.stg_source_88_id = dep_43.stg_source_27_id
    left join dep_44 on dep_1.stg_source_88_id = dep_44.int_transformed_104_id
    left join dep_45 on dep_1.stg_source_88_id = dep_45.int_transformed_9_id
    left join dep_46 on dep_1.stg_source_88_id = dep_46.int_transformed_3_id
    left join dep_47 on dep_1.stg_source_88_id = dep_47.int_transformed_6_id
    left join dep_48 on dep_1.stg_source_88_id = dep_48.int_transformed_18_id
    left join dep_49 on dep_1.stg_source_88_id = dep_49.int_transformed_86_id
    left join dep_50 on dep_1.stg_source_88_id = dep_50.stg_source_17_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
