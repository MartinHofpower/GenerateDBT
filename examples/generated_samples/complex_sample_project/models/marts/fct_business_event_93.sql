{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_2 as (
    select * from {{ ref('stg_source_158') }}
),

dep_3 as (
    select * from {{ ref('stg_source_63') }}
),

dep_4 as (
    select * from {{ ref('stg_source_29') }}
),

dep_5 as (
    select * from {{ ref('stg_source_163') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_10 as (
    select * from {{ ref('stg_source_93') }}
),

dep_11 as (
    select * from {{ ref('stg_source_24') }}
),

dep_12 as (
    select * from {{ ref('stg_source_130') }}
),

dep_13 as (
    select * from {{ ref('stg_source_118') }}
),

dep_14 as (
    select * from {{ ref('stg_source_82') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_16 as (
    select * from {{ ref('stg_source_80') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_18 as (
    select * from {{ ref('stg_source_149') }}
),

dep_19 as (
    select * from {{ ref('stg_source_139') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_21 as (
    select * from {{ ref('stg_source_17') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_24 as (
    select * from {{ ref('stg_source_147') }}
),

dep_25 as (
    select * from {{ ref('stg_source_78') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_27 as (
    select * from {{ ref('stg_source_83') }}
),

dep_28 as (
    select * from {{ ref('stg_source_122') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_31 as (
    select * from {{ ref('stg_source_40') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_33 as (
    select * from {{ ref('stg_source_124') }}
),

dep_34 as (
    select * from {{ ref('stg_source_68') }}
),

dep_35 as (
    select * from {{ ref('stg_source_90') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_37 as (
    select * from {{ ref('stg_source_30') }}
),

dep_38 as (
    select * from {{ ref('stg_source_144') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_40 as (
    select * from {{ ref('stg_source_21') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_43 as (
    select * from {{ ref('stg_source_133') }}
),

dep_44 as (
    select * from {{ ref('stg_source_9') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_48 as (
    select * from {{ ref('stg_source_44') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_67') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_118_id']) }} as surrogate_id,
        dep_1.int_transformed_118_id as fct_business_event_93_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_118_id = dep_2.stg_source_158_id
    left join dep_3 on dep_1.int_transformed_118_id = dep_3.stg_source_63_id
    left join dep_4 on dep_1.int_transformed_118_id = dep_4.stg_source_29_id
    left join dep_5 on dep_1.int_transformed_118_id = dep_5.stg_source_163_id
    left join dep_6 on dep_1.int_transformed_118_id = dep_6.int_transformed_108_id
    left join dep_7 on dep_1.int_transformed_118_id = dep_7.int_transformed_143_id
    left join dep_8 on dep_1.int_transformed_118_id = dep_8.int_transformed_28_id
    left join dep_9 on dep_1.int_transformed_118_id = dep_9.int_transformed_135_id
    left join dep_10 on dep_1.int_transformed_118_id = dep_10.stg_source_93_id
    left join dep_11 on dep_1.int_transformed_118_id = dep_11.stg_source_24_id
    left join dep_12 on dep_1.int_transformed_118_id = dep_12.stg_source_130_id
    left join dep_13 on dep_1.int_transformed_118_id = dep_13.stg_source_118_id
    left join dep_14 on dep_1.int_transformed_118_id = dep_14.stg_source_82_id
    left join dep_15 on dep_1.int_transformed_118_id = dep_15.int_transformed_18_id
    left join dep_16 on dep_1.int_transformed_118_id = dep_16.stg_source_80_id
    left join dep_17 on dep_1.int_transformed_118_id = dep_17.int_transformed_141_id
    left join dep_18 on dep_1.int_transformed_118_id = dep_18.stg_source_149_id
    left join dep_19 on dep_1.int_transformed_118_id = dep_19.stg_source_139_id
    left join dep_20 on dep_1.int_transformed_118_id = dep_20.int_transformed_150_id
    left join dep_21 on dep_1.int_transformed_118_id = dep_21.stg_source_17_id
    left join dep_22 on dep_1.int_transformed_118_id = dep_22.int_transformed_82_id
    left join dep_23 on dep_1.int_transformed_118_id = dep_23.int_transformed_113_id
    left join dep_24 on dep_1.int_transformed_118_id = dep_24.stg_source_147_id
    left join dep_25 on dep_1.int_transformed_118_id = dep_25.stg_source_78_id
    left join dep_26 on dep_1.int_transformed_118_id = dep_26.int_transformed_47_id
    left join dep_27 on dep_1.int_transformed_118_id = dep_27.stg_source_83_id
    left join dep_28 on dep_1.int_transformed_118_id = dep_28.stg_source_122_id
    left join dep_29 on dep_1.int_transformed_118_id = dep_29.int_transformed_148_id
    left join dep_30 on dep_1.int_transformed_118_id = dep_30.int_transformed_2_id
    left join dep_31 on dep_1.int_transformed_118_id = dep_31.stg_source_40_id
    left join dep_32 on dep_1.int_transformed_118_id = dep_32.int_transformed_38_id
    left join dep_33 on dep_1.int_transformed_118_id = dep_33.stg_source_124_id
    left join dep_34 on dep_1.int_transformed_118_id = dep_34.stg_source_68_id
    left join dep_35 on dep_1.int_transformed_118_id = dep_35.stg_source_90_id
    left join dep_36 on dep_1.int_transformed_118_id = dep_36.int_transformed_128_id
    left join dep_37 on dep_1.int_transformed_118_id = dep_37.stg_source_30_id
    left join dep_38 on dep_1.int_transformed_118_id = dep_38.stg_source_144_id
    left join dep_39 on dep_1.int_transformed_118_id = dep_39.int_transformed_41_id
    left join dep_40 on dep_1.int_transformed_118_id = dep_40.stg_source_21_id
    left join dep_41 on dep_1.int_transformed_118_id = dep_41.int_transformed_103_id
    left join dep_42 on dep_1.int_transformed_118_id = dep_42.int_transformed_70_id
    left join dep_43 on dep_1.int_transformed_118_id = dep_43.stg_source_133_id
    left join dep_44 on dep_1.int_transformed_118_id = dep_44.stg_source_9_id
    left join dep_45 on dep_1.int_transformed_118_id = dep_45.int_transformed_104_id
    left join dep_46 on dep_1.int_transformed_118_id = dep_46.int_transformed_140_id
    left join dep_47 on dep_1.int_transformed_118_id = dep_47.int_transformed_20_id
    left join dep_48 on dep_1.int_transformed_118_id = dep_48.stg_source_44_id
    left join dep_49 on dep_1.int_transformed_118_id = dep_49.int_transformed_49_id
    left join dep_50 on dep_1.int_transformed_118_id = dep_50.int_transformed_67_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
