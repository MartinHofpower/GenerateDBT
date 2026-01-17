{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_2 as (
    select * from {{ ref('stg_source_66') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_6 as (
    select * from {{ ref('stg_source_139') }}
),

dep_7 as (
    select * from {{ ref('stg_source_94') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_9 as (
    select * from {{ ref('stg_source_89') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_14 as (
    select * from {{ ref('stg_source_134') }}
),

dep_15 as (
    select * from {{ ref('stg_source_42') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_18 as (
    select * from {{ ref('stg_source_114') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_20 as (
    select * from {{ ref('stg_source_95') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_23 as (
    select * from {{ ref('stg_source_133') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_28 as (
    select * from {{ ref('stg_source_19') }}
),

dep_29 as (
    select * from {{ ref('stg_source_58') }}
),

dep_30 as (
    select * from {{ ref('stg_source_37') }}
),

dep_31 as (
    select * from {{ ref('stg_source_80') }}
),

dep_32 as (
    select * from {{ ref('stg_source_138') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_35 as (
    select * from {{ ref('stg_source_69') }}
),

dep_36 as (
    select * from {{ ref('stg_source_147') }}
),

dep_37 as (
    select * from {{ ref('stg_source_14') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_39 as (
    select * from {{ ref('stg_source_46') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_42 as (
    select * from {{ ref('stg_source_74') }}
),

dep_43 as (
    select * from {{ ref('stg_source_71') }}
),

dep_44 as (
    select * from {{ ref('stg_source_110') }}
),

dep_45 as (
    select * from {{ ref('stg_source_70') }}
),

dep_46 as (
    select * from {{ ref('stg_source_67') }}
),

dep_47 as (
    select * from {{ ref('stg_source_8') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_49 as (
    select * from {{ ref('stg_source_21') }}
),

dep_50 as (
    select * from {{ ref('stg_source_118') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_122_id']) }} as surrogate_id,
        dep_1.int_transformed_122_id as dim_entity_108_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_122_id = dep_2.stg_source_66_id
    left join dep_3 on dep_1.int_transformed_122_id = dep_3.int_transformed_141_id
    left join dep_4 on dep_1.int_transformed_122_id = dep_4.int_transformed_146_id
    left join dep_5 on dep_1.int_transformed_122_id = dep_5.int_transformed_86_id
    left join dep_6 on dep_1.int_transformed_122_id = dep_6.stg_source_139_id
    left join dep_7 on dep_1.int_transformed_122_id = dep_7.stg_source_94_id
    left join dep_8 on dep_1.int_transformed_122_id = dep_8.int_transformed_69_id
    left join dep_9 on dep_1.int_transformed_122_id = dep_9.stg_source_89_id
    left join dep_10 on dep_1.int_transformed_122_id = dep_10.int_transformed_13_id
    left join dep_11 on dep_1.int_transformed_122_id = dep_11.int_transformed_164_id
    left join dep_12 on dep_1.int_transformed_122_id = dep_12.int_transformed_71_id
    left join dep_13 on dep_1.int_transformed_122_id = dep_13.int_transformed_88_id
    left join dep_14 on dep_1.int_transformed_122_id = dep_14.stg_source_134_id
    left join dep_15 on dep_1.int_transformed_122_id = dep_15.stg_source_42_id
    left join dep_16 on dep_1.int_transformed_122_id = dep_16.int_transformed_91_id
    left join dep_17 on dep_1.int_transformed_122_id = dep_17.int_transformed_23_id
    left join dep_18 on dep_1.int_transformed_122_id = dep_18.stg_source_114_id
    left join dep_19 on dep_1.int_transformed_122_id = dep_19.int_transformed_27_id
    left join dep_20 on dep_1.int_transformed_122_id = dep_20.stg_source_95_id
    left join dep_21 on dep_1.int_transformed_122_id = dep_21.int_transformed_85_id
    left join dep_22 on dep_1.int_transformed_122_id = dep_22.int_transformed_53_id
    left join dep_23 on dep_1.int_transformed_122_id = dep_23.stg_source_133_id
    left join dep_24 on dep_1.int_transformed_122_id = dep_24.int_transformed_80_id
    left join dep_25 on dep_1.int_transformed_122_id = dep_25.int_transformed_46_id
    left join dep_26 on dep_1.int_transformed_122_id = dep_26.int_transformed_81_id
    left join dep_27 on dep_1.int_transformed_122_id = dep_27.int_transformed_100_id
    left join dep_28 on dep_1.int_transformed_122_id = dep_28.stg_source_19_id
    left join dep_29 on dep_1.int_transformed_122_id = dep_29.stg_source_58_id
    left join dep_30 on dep_1.int_transformed_122_id = dep_30.stg_source_37_id
    left join dep_31 on dep_1.int_transformed_122_id = dep_31.stg_source_80_id
    left join dep_32 on dep_1.int_transformed_122_id = dep_32.stg_source_138_id
    left join dep_33 on dep_1.int_transformed_122_id = dep_33.int_transformed_44_id
    left join dep_34 on dep_1.int_transformed_122_id = dep_34.int_transformed_41_id
    left join dep_35 on dep_1.int_transformed_122_id = dep_35.stg_source_69_id
    left join dep_36 on dep_1.int_transformed_122_id = dep_36.stg_source_147_id
    left join dep_37 on dep_1.int_transformed_122_id = dep_37.stg_source_14_id
    left join dep_38 on dep_1.int_transformed_122_id = dep_38.int_transformed_72_id
    left join dep_39 on dep_1.int_transformed_122_id = dep_39.stg_source_46_id
    left join dep_40 on dep_1.int_transformed_122_id = dep_40.int_transformed_94_id
    left join dep_41 on dep_1.int_transformed_122_id = dep_41.int_transformed_107_id
    left join dep_42 on dep_1.int_transformed_122_id = dep_42.stg_source_74_id
    left join dep_43 on dep_1.int_transformed_122_id = dep_43.stg_source_71_id
    left join dep_44 on dep_1.int_transformed_122_id = dep_44.stg_source_110_id
    left join dep_45 on dep_1.int_transformed_122_id = dep_45.stg_source_70_id
    left join dep_46 on dep_1.int_transformed_122_id = dep_46.stg_source_67_id
    left join dep_47 on dep_1.int_transformed_122_id = dep_47.stg_source_8_id
    left join dep_48 on dep_1.int_transformed_122_id = dep_48.int_transformed_9_id
    left join dep_49 on dep_1.int_transformed_122_id = dep_49.stg_source_21_id
    left join dep_50 on dep_1.int_transformed_122_id = dep_50.stg_source_118_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
