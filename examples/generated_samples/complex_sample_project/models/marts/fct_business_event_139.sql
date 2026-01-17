{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_2 as (
    select * from {{ ref('stg_source_127') }}
),

dep_3 as (
    select * from {{ ref('stg_source_21') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_165') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_7 as (
    select * from {{ ref('stg_source_79') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_9 as (
    select * from {{ ref('stg_source_35') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_12 as (
    select * from {{ ref('stg_source_62') }}
),

dep_13 as (
    select * from {{ ref('stg_source_28') }}
),

dep_14 as (
    select * from {{ ref('stg_source_133') }}
),

dep_15 as (
    select * from {{ ref('stg_source_72') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_18 as (
    select * from {{ ref('stg_source_134') }}
),

dep_19 as (
    select * from {{ ref('stg_source_93') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_22 as (
    select * from {{ ref('stg_source_66') }}
),

dep_23 as (
    select * from {{ ref('stg_source_140') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_25 as (
    select * from {{ ref('stg_source_13') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_28 as (
    select * from {{ ref('stg_source_42') }}
),

dep_29 as (
    select * from {{ ref('stg_source_106') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_32 as (
    select * from {{ ref('stg_source_39') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_34 as (
    select * from {{ ref('stg_source_118') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_36 as (
    select * from {{ ref('stg_source_88') }}
),

dep_37 as (
    select * from {{ ref('stg_source_145') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_39 as (
    select * from {{ ref('stg_source_166') }}
),

dep_40 as (
    select * from {{ ref('stg_source_91') }}
),

dep_41 as (
    select * from {{ ref('stg_source_1') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_43 as (
    select * from {{ ref('stg_source_161') }}
),

dep_44 as (
    select * from {{ ref('stg_source_96') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_46 as (
    select * from {{ ref('stg_source_146') }}
),

dep_47 as (
    select * from {{ ref('stg_source_74') }}
),

dep_48 as (
    select * from {{ ref('stg_source_120') }}
),

dep_49 as (
    select * from {{ ref('stg_source_33') }}
),

dep_50 as (
    select * from {{ ref('stg_source_9') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_34_id']) }} as surrogate_id,
        dep_1.int_transformed_34_id as fct_business_event_139_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_34_id = dep_2.stg_source_127_id
    left join dep_3 on dep_1.int_transformed_34_id = dep_3.stg_source_21_id
    left join dep_4 on dep_1.int_transformed_34_id = dep_4.int_transformed_165_id
    left join dep_5 on dep_1.int_transformed_34_id = dep_5.int_transformed_122_id
    left join dep_6 on dep_1.int_transformed_34_id = dep_6.int_transformed_115_id
    left join dep_7 on dep_1.int_transformed_34_id = dep_7.stg_source_79_id
    left join dep_8 on dep_1.int_transformed_34_id = dep_8.int_transformed_153_id
    left join dep_9 on dep_1.int_transformed_34_id = dep_9.stg_source_35_id
    left join dep_10 on dep_1.int_transformed_34_id = dep_10.int_transformed_46_id
    left join dep_11 on dep_1.int_transformed_34_id = dep_11.int_transformed_14_id
    left join dep_12 on dep_1.int_transformed_34_id = dep_12.stg_source_62_id
    left join dep_13 on dep_1.int_transformed_34_id = dep_13.stg_source_28_id
    left join dep_14 on dep_1.int_transformed_34_id = dep_14.stg_source_133_id
    left join dep_15 on dep_1.int_transformed_34_id = dep_15.stg_source_72_id
    left join dep_16 on dep_1.int_transformed_34_id = dep_16.int_transformed_147_id
    left join dep_17 on dep_1.int_transformed_34_id = dep_17.int_transformed_61_id
    left join dep_18 on dep_1.int_transformed_34_id = dep_18.stg_source_134_id
    left join dep_19 on dep_1.int_transformed_34_id = dep_19.stg_source_93_id
    left join dep_20 on dep_1.int_transformed_34_id = dep_20.int_transformed_111_id
    left join dep_21 on dep_1.int_transformed_34_id = dep_21.int_transformed_95_id
    left join dep_22 on dep_1.int_transformed_34_id = dep_22.stg_source_66_id
    left join dep_23 on dep_1.int_transformed_34_id = dep_23.stg_source_140_id
    left join dep_24 on dep_1.int_transformed_34_id = dep_24.int_transformed_28_id
    left join dep_25 on dep_1.int_transformed_34_id = dep_25.stg_source_13_id
    left join dep_26 on dep_1.int_transformed_34_id = dep_26.int_transformed_42_id
    left join dep_27 on dep_1.int_transformed_34_id = dep_27.int_transformed_163_id
    left join dep_28 on dep_1.int_transformed_34_id = dep_28.stg_source_42_id
    left join dep_29 on dep_1.int_transformed_34_id = dep_29.stg_source_106_id
    left join dep_30 on dep_1.int_transformed_34_id = dep_30.int_transformed_41_id
    left join dep_31 on dep_1.int_transformed_34_id = dep_31.int_transformed_143_id
    left join dep_32 on dep_1.int_transformed_34_id = dep_32.stg_source_39_id
    left join dep_33 on dep_1.int_transformed_34_id = dep_33.int_transformed_44_id
    left join dep_34 on dep_1.int_transformed_34_id = dep_34.stg_source_118_id
    left join dep_35 on dep_1.int_transformed_34_id = dep_35.int_transformed_101_id
    left join dep_36 on dep_1.int_transformed_34_id = dep_36.stg_source_88_id
    left join dep_37 on dep_1.int_transformed_34_id = dep_37.stg_source_145_id
    left join dep_38 on dep_1.int_transformed_34_id = dep_38.int_transformed_117_id
    left join dep_39 on dep_1.int_transformed_34_id = dep_39.stg_source_166_id
    left join dep_40 on dep_1.int_transformed_34_id = dep_40.stg_source_91_id
    left join dep_41 on dep_1.int_transformed_34_id = dep_41.stg_source_1_id
    left join dep_42 on dep_1.int_transformed_34_id = dep_42.int_transformed_60_id
    left join dep_43 on dep_1.int_transformed_34_id = dep_43.stg_source_161_id
    left join dep_44 on dep_1.int_transformed_34_id = dep_44.stg_source_96_id
    left join dep_45 on dep_1.int_transformed_34_id = dep_45.int_transformed_109_id
    left join dep_46 on dep_1.int_transformed_34_id = dep_46.stg_source_146_id
    left join dep_47 on dep_1.int_transformed_34_id = dep_47.stg_source_74_id
    left join dep_48 on dep_1.int_transformed_34_id = dep_48.stg_source_120_id
    left join dep_49 on dep_1.int_transformed_34_id = dep_49.stg_source_33_id
    left join dep_50 on dep_1.int_transformed_34_id = dep_50.stg_source_9_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
