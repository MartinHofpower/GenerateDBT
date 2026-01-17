{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_74') }}
),

dep_2 as (
    select * from {{ ref('stg_source_106') }}
),

dep_3 as (
    select * from {{ ref('stg_source_13') }}
),

dep_4 as (
    select * from {{ ref('stg_source_2') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_6 as (
    select * from {{ ref('stg_source_129') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_8 as (
    select * from {{ ref('stg_source_55') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_10 as (
    select * from {{ ref('stg_source_26') }}
),

dep_11 as (
    select * from {{ ref('stg_source_132') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_16 as (
    select * from {{ ref('stg_source_25') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_18 as (
    select * from {{ ref('stg_source_82') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_20 as (
    select * from {{ ref('stg_source_150') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_22 as (
    select * from {{ ref('stg_source_22') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_25 as (
    select * from {{ ref('stg_source_86') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_28 as (
    select * from {{ ref('stg_source_163') }}
),

dep_29 as (
    select * from {{ ref('stg_source_127') }}
),

dep_30 as (
    select * from {{ ref('stg_source_60') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_33 as (
    select * from {{ ref('stg_source_99') }}
),

dep_34 as (
    select * from {{ ref('stg_source_66') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_36 as (
    select * from {{ ref('stg_source_48') }}
),

dep_37 as (
    select * from {{ ref('stg_source_21') }}
),

dep_38 as (
    select * from {{ ref('stg_source_141') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_41 as (
    select * from {{ ref('stg_source_33') }}
),

dep_42 as (
    select * from {{ ref('stg_source_95') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_44 as (
    select * from {{ ref('stg_source_53') }}
),

dep_45 as (
    select * from {{ ref('stg_source_84') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_47 as (
    select * from {{ ref('stg_source_16') }}
),

dep_48 as (
    select * from {{ ref('stg_source_8') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_113') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_74_id']) }} as surrogate_id,
        dep_1.stg_source_74_id as fct_business_event_27_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_74_id = dep_2.stg_source_106_id
    left join dep_3 on dep_1.stg_source_74_id = dep_3.stg_source_13_id
    left join dep_4 on dep_1.stg_source_74_id = dep_4.stg_source_2_id
    left join dep_5 on dep_1.stg_source_74_id = dep_5.int_transformed_50_id
    left join dep_6 on dep_1.stg_source_74_id = dep_6.stg_source_129_id
    left join dep_7 on dep_1.stg_source_74_id = dep_7.int_transformed_32_id
    left join dep_8 on dep_1.stg_source_74_id = dep_8.stg_source_55_id
    left join dep_9 on dep_1.stg_source_74_id = dep_9.int_transformed_22_id
    left join dep_10 on dep_1.stg_source_74_id = dep_10.stg_source_26_id
    left join dep_11 on dep_1.stg_source_74_id = dep_11.stg_source_132_id
    left join dep_12 on dep_1.stg_source_74_id = dep_12.int_transformed_118_id
    left join dep_13 on dep_1.stg_source_74_id = dep_13.int_transformed_122_id
    left join dep_14 on dep_1.stg_source_74_id = dep_14.int_transformed_63_id
    left join dep_15 on dep_1.stg_source_74_id = dep_15.int_transformed_7_id
    left join dep_16 on dep_1.stg_source_74_id = dep_16.stg_source_25_id
    left join dep_17 on dep_1.stg_source_74_id = dep_17.int_transformed_94_id
    left join dep_18 on dep_1.stg_source_74_id = dep_18.stg_source_82_id
    left join dep_19 on dep_1.stg_source_74_id = dep_19.int_transformed_130_id
    left join dep_20 on dep_1.stg_source_74_id = dep_20.stg_source_150_id
    left join dep_21 on dep_1.stg_source_74_id = dep_21.int_transformed_24_id
    left join dep_22 on dep_1.stg_source_74_id = dep_22.stg_source_22_id
    left join dep_23 on dep_1.stg_source_74_id = dep_23.int_transformed_40_id
    left join dep_24 on dep_1.stg_source_74_id = dep_24.int_transformed_46_id
    left join dep_25 on dep_1.stg_source_74_id = dep_25.stg_source_86_id
    left join dep_26 on dep_1.stg_source_74_id = dep_26.int_transformed_53_id
    left join dep_27 on dep_1.stg_source_74_id = dep_27.int_transformed_142_id
    left join dep_28 on dep_1.stg_source_74_id = dep_28.stg_source_163_id
    left join dep_29 on dep_1.stg_source_74_id = dep_29.stg_source_127_id
    left join dep_30 on dep_1.stg_source_74_id = dep_30.stg_source_60_id
    left join dep_31 on dep_1.stg_source_74_id = dep_31.int_transformed_154_id
    left join dep_32 on dep_1.stg_source_74_id = dep_32.int_transformed_96_id
    left join dep_33 on dep_1.stg_source_74_id = dep_33.stg_source_99_id
    left join dep_34 on dep_1.stg_source_74_id = dep_34.stg_source_66_id
    left join dep_35 on dep_1.stg_source_74_id = dep_35.int_transformed_3_id
    left join dep_36 on dep_1.stg_source_74_id = dep_36.stg_source_48_id
    left join dep_37 on dep_1.stg_source_74_id = dep_37.stg_source_21_id
    left join dep_38 on dep_1.stg_source_74_id = dep_38.stg_source_141_id
    left join dep_39 on dep_1.stg_source_74_id = dep_39.int_transformed_17_id
    left join dep_40 on dep_1.stg_source_74_id = dep_40.int_transformed_106_id
    left join dep_41 on dep_1.stg_source_74_id = dep_41.stg_source_33_id
    left join dep_42 on dep_1.stg_source_74_id = dep_42.stg_source_95_id
    left join dep_43 on dep_1.stg_source_74_id = dep_43.int_transformed_108_id
    left join dep_44 on dep_1.stg_source_74_id = dep_44.stg_source_53_id
    left join dep_45 on dep_1.stg_source_74_id = dep_45.stg_source_84_id
    left join dep_46 on dep_1.stg_source_74_id = dep_46.int_transformed_126_id
    left join dep_47 on dep_1.stg_source_74_id = dep_47.stg_source_16_id
    left join dep_48 on dep_1.stg_source_74_id = dep_48.stg_source_8_id
    left join dep_49 on dep_1.stg_source_74_id = dep_49.int_transformed_103_id
    left join dep_50 on dep_1.stg_source_74_id = dep_50.int_transformed_113_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
