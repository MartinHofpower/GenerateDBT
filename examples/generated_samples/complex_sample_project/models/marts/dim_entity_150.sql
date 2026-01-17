{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_104') }}
),

dep_2 as (
    select * from {{ ref('stg_source_23') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_8 as (
    select * from {{ ref('stg_source_130') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_11 as (
    select * from {{ ref('stg_source_56') }}
),

dep_12 as (
    select * from {{ ref('stg_source_160') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_16 as (
    select * from {{ ref('stg_source_153') }}
),

dep_17 as (
    select * from {{ ref('stg_source_88') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_19 as (
    select * from {{ ref('stg_source_82') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_21 as (
    select * from {{ ref('stg_source_45') }}
),

dep_22 as (
    select * from {{ ref('stg_source_120') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_24 as (
    select * from {{ ref('stg_source_95') }}
),

dep_25 as (
    select * from {{ ref('stg_source_90') }}
),

dep_26 as (
    select * from {{ ref('stg_source_102') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_28 as (
    select * from {{ ref('stg_source_17') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_33 as (
    select * from {{ ref('stg_source_33') }}
),

dep_34 as (
    select * from {{ ref('stg_source_133') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_37 as (
    select * from {{ ref('stg_source_58') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_44 as (
    select * from {{ ref('stg_source_6') }}
),

dep_45 as (
    select * from {{ ref('stg_source_63') }}
),

dep_46 as (
    select * from {{ ref('stg_source_94') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_49 as (
    select * from {{ ref('stg_source_22') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_13') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_104_id']) }} as surrogate_id,
        dep_1.stg_source_104_id as dim_entity_150_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_104_id = dep_2.stg_source_23_id
    left join dep_3 on dep_1.stg_source_104_id = dep_3.int_transformed_154_id
    left join dep_4 on dep_1.stg_source_104_id = dep_4.int_transformed_12_id
    left join dep_5 on dep_1.stg_source_104_id = dep_5.int_transformed_17_id
    left join dep_6 on dep_1.stg_source_104_id = dep_6.int_transformed_84_id
    left join dep_7 on dep_1.stg_source_104_id = dep_7.int_transformed_45_id
    left join dep_8 on dep_1.stg_source_104_id = dep_8.stg_source_130_id
    left join dep_9 on dep_1.stg_source_104_id = dep_9.int_transformed_81_id
    left join dep_10 on dep_1.stg_source_104_id = dep_10.int_transformed_86_id
    left join dep_11 on dep_1.stg_source_104_id = dep_11.stg_source_56_id
    left join dep_12 on dep_1.stg_source_104_id = dep_12.stg_source_160_id
    left join dep_13 on dep_1.stg_source_104_id = dep_13.int_transformed_158_id
    left join dep_14 on dep_1.stg_source_104_id = dep_14.int_transformed_78_id
    left join dep_15 on dep_1.stg_source_104_id = dep_15.int_transformed_67_id
    left join dep_16 on dep_1.stg_source_104_id = dep_16.stg_source_153_id
    left join dep_17 on dep_1.stg_source_104_id = dep_17.stg_source_88_id
    left join dep_18 on dep_1.stg_source_104_id = dep_18.int_transformed_11_id
    left join dep_19 on dep_1.stg_source_104_id = dep_19.stg_source_82_id
    left join dep_20 on dep_1.stg_source_104_id = dep_20.int_transformed_137_id
    left join dep_21 on dep_1.stg_source_104_id = dep_21.stg_source_45_id
    left join dep_22 on dep_1.stg_source_104_id = dep_22.stg_source_120_id
    left join dep_23 on dep_1.stg_source_104_id = dep_23.int_transformed_55_id
    left join dep_24 on dep_1.stg_source_104_id = dep_24.stg_source_95_id
    left join dep_25 on dep_1.stg_source_104_id = dep_25.stg_source_90_id
    left join dep_26 on dep_1.stg_source_104_id = dep_26.stg_source_102_id
    left join dep_27 on dep_1.stg_source_104_id = dep_27.int_transformed_85_id
    left join dep_28 on dep_1.stg_source_104_id = dep_28.stg_source_17_id
    left join dep_29 on dep_1.stg_source_104_id = dep_29.int_transformed_166_id
    left join dep_30 on dep_1.stg_source_104_id = dep_30.int_transformed_149_id
    left join dep_31 on dep_1.stg_source_104_id = dep_31.int_transformed_146_id
    left join dep_32 on dep_1.stg_source_104_id = dep_32.int_transformed_38_id
    left join dep_33 on dep_1.stg_source_104_id = dep_33.stg_source_33_id
    left join dep_34 on dep_1.stg_source_104_id = dep_34.stg_source_133_id
    left join dep_35 on dep_1.stg_source_104_id = dep_35.int_transformed_157_id
    left join dep_36 on dep_1.stg_source_104_id = dep_36.int_transformed_135_id
    left join dep_37 on dep_1.stg_source_104_id = dep_37.stg_source_58_id
    left join dep_38 on dep_1.stg_source_104_id = dep_38.int_transformed_71_id
    left join dep_39 on dep_1.stg_source_104_id = dep_39.int_transformed_24_id
    left join dep_40 on dep_1.stg_source_104_id = dep_40.int_transformed_93_id
    left join dep_41 on dep_1.stg_source_104_id = dep_41.int_transformed_95_id
    left join dep_42 on dep_1.stg_source_104_id = dep_42.int_transformed_125_id
    left join dep_43 on dep_1.stg_source_104_id = dep_43.int_transformed_106_id
    left join dep_44 on dep_1.stg_source_104_id = dep_44.stg_source_6_id
    left join dep_45 on dep_1.stg_source_104_id = dep_45.stg_source_63_id
    left join dep_46 on dep_1.stg_source_104_id = dep_46.stg_source_94_id
    left join dep_47 on dep_1.stg_source_104_id = dep_47.int_transformed_141_id
    left join dep_48 on dep_1.stg_source_104_id = dep_48.int_transformed_107_id
    left join dep_49 on dep_1.stg_source_104_id = dep_49.stg_source_22_id
    left join dep_50 on dep_1.stg_source_104_id = dep_50.int_transformed_13_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
