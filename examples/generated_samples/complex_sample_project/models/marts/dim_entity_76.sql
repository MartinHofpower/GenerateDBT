{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_3 as (
    select * from {{ ref('stg_source_142') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_7 as (
    select * from {{ ref('stg_source_25') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_10 as (
    select * from {{ ref('stg_source_117') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_12 as (
    select * from {{ ref('stg_source_86') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_14 as (
    select * from {{ ref('stg_source_54') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_22 as (
    select * from {{ ref('stg_source_136') }}
),

dep_23 as (
    select * from {{ ref('stg_source_18') }}
),

dep_24 as (
    select * from {{ ref('stg_source_47') }}
),

dep_25 as (
    select * from {{ ref('stg_source_129') }}
),

dep_26 as (
    select * from {{ ref('stg_source_7') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_29 as (
    select * from {{ ref('stg_source_70') }}
),

dep_30 as (
    select * from {{ ref('stg_source_53') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_32 as (
    select * from {{ ref('stg_source_12') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_34 as (
    select * from {{ ref('stg_source_72') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_36 as (
    select * from {{ ref('stg_source_29') }}
),

dep_37 as (
    select * from {{ ref('stg_source_49') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_40 as (
    select * from {{ ref('stg_source_106') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_43 as (
    select * from {{ ref('stg_source_27') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_45 as (
    select * from {{ ref('stg_source_48') }}
),

dep_46 as (
    select * from {{ ref('stg_source_111') }}
),

dep_47 as (
    select * from {{ ref('stg_source_121') }}
),

dep_48 as (
    select * from {{ ref('stg_source_147') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_50 as (
    select * from {{ ref('stg_source_44') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_43_id']) }} as surrogate_id,
        dep_1.int_transformed_43_id as dim_entity_76_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_43_id = dep_2.int_transformed_80_id
    left join dep_3 on dep_1.int_transformed_43_id = dep_3.stg_source_142_id
    left join dep_4 on dep_1.int_transformed_43_id = dep_4.int_transformed_15_id
    left join dep_5 on dep_1.int_transformed_43_id = dep_5.int_transformed_109_id
    left join dep_6 on dep_1.int_transformed_43_id = dep_6.int_transformed_77_id
    left join dep_7 on dep_1.int_transformed_43_id = dep_7.stg_source_25_id
    left join dep_8 on dep_1.int_transformed_43_id = dep_8.int_transformed_129_id
    left join dep_9 on dep_1.int_transformed_43_id = dep_9.int_transformed_110_id
    left join dep_10 on dep_1.int_transformed_43_id = dep_10.stg_source_117_id
    left join dep_11 on dep_1.int_transformed_43_id = dep_11.int_transformed_67_id
    left join dep_12 on dep_1.int_transformed_43_id = dep_12.stg_source_86_id
    left join dep_13 on dep_1.int_transformed_43_id = dep_13.int_transformed_163_id
    left join dep_14 on dep_1.int_transformed_43_id = dep_14.stg_source_54_id
    left join dep_15 on dep_1.int_transformed_43_id = dep_15.int_transformed_49_id
    left join dep_16 on dep_1.int_transformed_43_id = dep_16.int_transformed_45_id
    left join dep_17 on dep_1.int_transformed_43_id = dep_17.int_transformed_141_id
    left join dep_18 on dep_1.int_transformed_43_id = dep_18.int_transformed_150_id
    left join dep_19 on dep_1.int_transformed_43_id = dep_19.int_transformed_91_id
    left join dep_20 on dep_1.int_transformed_43_id = dep_20.int_transformed_125_id
    left join dep_21 on dep_1.int_transformed_43_id = dep_21.int_transformed_142_id
    left join dep_22 on dep_1.int_transformed_43_id = dep_22.stg_source_136_id
    left join dep_23 on dep_1.int_transformed_43_id = dep_23.stg_source_18_id
    left join dep_24 on dep_1.int_transformed_43_id = dep_24.stg_source_47_id
    left join dep_25 on dep_1.int_transformed_43_id = dep_25.stg_source_129_id
    left join dep_26 on dep_1.int_transformed_43_id = dep_26.stg_source_7_id
    left join dep_27 on dep_1.int_transformed_43_id = dep_27.int_transformed_132_id
    left join dep_28 on dep_1.int_transformed_43_id = dep_28.int_transformed_159_id
    left join dep_29 on dep_1.int_transformed_43_id = dep_29.stg_source_70_id
    left join dep_30 on dep_1.int_transformed_43_id = dep_30.stg_source_53_id
    left join dep_31 on dep_1.int_transformed_43_id = dep_31.int_transformed_95_id
    left join dep_32 on dep_1.int_transformed_43_id = dep_32.stg_source_12_id
    left join dep_33 on dep_1.int_transformed_43_id = dep_33.int_transformed_131_id
    left join dep_34 on dep_1.int_transformed_43_id = dep_34.stg_source_72_id
    left join dep_35 on dep_1.int_transformed_43_id = dep_35.int_transformed_123_id
    left join dep_36 on dep_1.int_transformed_43_id = dep_36.stg_source_29_id
    left join dep_37 on dep_1.int_transformed_43_id = dep_37.stg_source_49_id
    left join dep_38 on dep_1.int_transformed_43_id = dep_38.int_transformed_100_id
    left join dep_39 on dep_1.int_transformed_43_id = dep_39.int_transformed_65_id
    left join dep_40 on dep_1.int_transformed_43_id = dep_40.stg_source_106_id
    left join dep_41 on dep_1.int_transformed_43_id = dep_41.int_transformed_20_id
    left join dep_42 on dep_1.int_transformed_43_id = dep_42.int_transformed_126_id
    left join dep_43 on dep_1.int_transformed_43_id = dep_43.stg_source_27_id
    left join dep_44 on dep_1.int_transformed_43_id = dep_44.int_transformed_153_id
    left join dep_45 on dep_1.int_transformed_43_id = dep_45.stg_source_48_id
    left join dep_46 on dep_1.int_transformed_43_id = dep_46.stg_source_111_id
    left join dep_47 on dep_1.int_transformed_43_id = dep_47.stg_source_121_id
    left join dep_48 on dep_1.int_transformed_43_id = dep_48.stg_source_147_id
    left join dep_49 on dep_1.int_transformed_43_id = dep_49.int_transformed_161_id
    left join dep_50 on dep_1.int_transformed_43_id = dep_50.stg_source_44_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
