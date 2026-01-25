{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_3 as (
    select * from {{ ref('stg_source_130') }}
),

dep_4 as (
    select * from {{ ref('stg_source_69') }}
),

dep_5 as (
    select * from {{ ref('stg_source_1') }}
),

dep_6 as (
    select * from {{ ref('stg_source_166') }}
),

dep_7 as (
    select * from {{ ref('stg_source_62') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_9 as (
    select * from {{ ref('stg_source_94') }}
),

dep_10 as (
    select * from {{ ref('stg_source_46') }}
),

dep_11 as (
    select * from {{ ref('stg_source_83') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_13 as (
    select * from {{ ref('stg_source_61') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_18 as (
    select * from {{ ref('stg_source_116') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_21 as (
    select * from {{ ref('stg_source_71') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_23 as (
    select * from {{ ref('stg_source_16') }}
),

dep_24 as (
    select * from {{ ref('stg_source_82') }}
),

dep_25 as (
    select * from {{ ref('stg_source_95') }}
),

dep_26 as (
    select * from {{ ref('stg_source_134') }}
),

dep_27 as (
    select * from {{ ref('stg_source_48') }}
),

dep_28 as (
    select * from {{ ref('stg_source_121') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_30 as (
    select * from {{ ref('stg_source_38') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_32 as (
    select * from {{ ref('stg_source_73') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_34 as (
    select * from {{ ref('stg_source_105') }}
),

dep_35 as (
    select * from {{ ref('stg_source_6') }}
),

dep_36 as (
    select * from {{ ref('stg_source_52') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_39 as (
    select * from {{ ref('stg_source_30') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_41 as (
    select * from {{ ref('stg_source_157') }}
),

dep_42 as (
    select * from {{ ref('stg_source_151') }}
),

dep_43 as (
    select * from {{ ref('stg_source_118') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_46 as (
    select * from {{ ref('stg_source_74') }}
),

dep_47 as (
    select * from {{ ref('stg_source_34') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_49 as (
    select * from {{ ref('stg_source_45') }}
),

dep_50 as (
    select * from {{ ref('stg_source_44') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_141_id']) }} as surrogate_id,
        dep_1.int_transformed_141_id as fct_business_event_117_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_141_id = dep_2.int_transformed_36_id
    left join dep_3 on dep_1.int_transformed_141_id = dep_3.stg_source_130_id
    left join dep_4 on dep_1.int_transformed_141_id = dep_4.stg_source_69_id
    left join dep_5 on dep_1.int_transformed_141_id = dep_5.stg_source_1_id
    left join dep_6 on dep_1.int_transformed_141_id = dep_6.stg_source_166_id
    left join dep_7 on dep_1.int_transformed_141_id = dep_7.stg_source_62_id
    left join dep_8 on dep_1.int_transformed_141_id = dep_8.int_transformed_7_id
    left join dep_9 on dep_1.int_transformed_141_id = dep_9.stg_source_94_id
    left join dep_10 on dep_1.int_transformed_141_id = dep_10.stg_source_46_id
    left join dep_11 on dep_1.int_transformed_141_id = dep_11.stg_source_83_id
    left join dep_12 on dep_1.int_transformed_141_id = dep_12.int_transformed_58_id
    left join dep_13 on dep_1.int_transformed_141_id = dep_13.stg_source_61_id
    left join dep_14 on dep_1.int_transformed_141_id = dep_14.int_transformed_146_id
    left join dep_15 on dep_1.int_transformed_141_id = dep_15.int_transformed_109_id
    left join dep_16 on dep_1.int_transformed_141_id = dep_16.int_transformed_110_id
    left join dep_17 on dep_1.int_transformed_141_id = dep_17.int_transformed_132_id
    left join dep_18 on dep_1.int_transformed_141_id = dep_18.stg_source_116_id
    left join dep_19 on dep_1.int_transformed_141_id = dep_19.int_transformed_164_id
    left join dep_20 on dep_1.int_transformed_141_id = dep_20.int_transformed_64_id
    left join dep_21 on dep_1.int_transformed_141_id = dep_21.stg_source_71_id
    left join dep_22 on dep_1.int_transformed_141_id = dep_22.int_transformed_100_id
    left join dep_23 on dep_1.int_transformed_141_id = dep_23.stg_source_16_id
    left join dep_24 on dep_1.int_transformed_141_id = dep_24.stg_source_82_id
    left join dep_25 on dep_1.int_transformed_141_id = dep_25.stg_source_95_id
    left join dep_26 on dep_1.int_transformed_141_id = dep_26.stg_source_134_id
    left join dep_27 on dep_1.int_transformed_141_id = dep_27.stg_source_48_id
    left join dep_28 on dep_1.int_transformed_141_id = dep_28.stg_source_121_id
    left join dep_29 on dep_1.int_transformed_141_id = dep_29.int_transformed_29_id
    left join dep_30 on dep_1.int_transformed_141_id = dep_30.stg_source_38_id
    left join dep_31 on dep_1.int_transformed_141_id = dep_31.int_transformed_5_id
    left join dep_32 on dep_1.int_transformed_141_id = dep_32.stg_source_73_id
    left join dep_33 on dep_1.int_transformed_141_id = dep_33.int_transformed_139_id
    left join dep_34 on dep_1.int_transformed_141_id = dep_34.stg_source_105_id
    left join dep_35 on dep_1.int_transformed_141_id = dep_35.stg_source_6_id
    left join dep_36 on dep_1.int_transformed_141_id = dep_36.stg_source_52_id
    left join dep_37 on dep_1.int_transformed_141_id = dep_37.int_transformed_125_id
    left join dep_38 on dep_1.int_transformed_141_id = dep_38.int_transformed_23_id
    left join dep_39 on dep_1.int_transformed_141_id = dep_39.stg_source_30_id
    left join dep_40 on dep_1.int_transformed_141_id = dep_40.int_transformed_161_id
    left join dep_41 on dep_1.int_transformed_141_id = dep_41.stg_source_157_id
    left join dep_42 on dep_1.int_transformed_141_id = dep_42.stg_source_151_id
    left join dep_43 on dep_1.int_transformed_141_id = dep_43.stg_source_118_id
    left join dep_44 on dep_1.int_transformed_141_id = dep_44.int_transformed_62_id
    left join dep_45 on dep_1.int_transformed_141_id = dep_45.int_transformed_89_id
    left join dep_46 on dep_1.int_transformed_141_id = dep_46.stg_source_74_id
    left join dep_47 on dep_1.int_transformed_141_id = dep_47.stg_source_34_id
    left join dep_48 on dep_1.int_transformed_141_id = dep_48.int_transformed_152_id
    left join dep_49 on dep_1.int_transformed_141_id = dep_49.stg_source_45_id
    left join dep_50 on dep_1.int_transformed_141_id = dep_50.stg_source_44_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
