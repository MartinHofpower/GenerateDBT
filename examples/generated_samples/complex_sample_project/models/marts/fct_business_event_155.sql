{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_50') }}
),

dep_2 as (
    select * from {{ ref('stg_source_142') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_4 as (
    select * from {{ ref('stg_source_123') }}
),

dep_5 as (
    select * from {{ ref('stg_source_158') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_10 as (
    select * from {{ ref('stg_source_128') }}
),

dep_11 as (
    select * from {{ ref('stg_source_73') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_13 as (
    select * from {{ ref('stg_source_136') }}
),

dep_14 as (
    select * from {{ ref('stg_source_31') }}
),

dep_15 as (
    select * from {{ ref('stg_source_27') }}
),

dep_16 as (
    select * from {{ ref('stg_source_2') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_19 as (
    select * from {{ ref('stg_source_3') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_21 as (
    select * from {{ ref('stg_source_94') }}
),

dep_22 as (
    select * from {{ ref('stg_source_91') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_24 as (
    select * from {{ ref('stg_source_51') }}
),

dep_25 as (
    select * from {{ ref('stg_source_95') }}
),

dep_26 as (
    select * from {{ ref('stg_source_10') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_30 as (
    select * from {{ ref('stg_source_35') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_165') }}
),

dep_32 as (
    select * from {{ ref('stg_source_74') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_36 as (
    select * from {{ ref('stg_source_54') }}
),

dep_37 as (
    select * from {{ ref('stg_source_79') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_40 as (
    select * from {{ ref('stg_source_49') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_42 as (
    select * from {{ ref('stg_source_88') }}
),

dep_43 as (
    select * from {{ ref('stg_source_9') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_45 as (
    select * from {{ ref('stg_source_56') }}
),

dep_46 as (
    select * from {{ ref('stg_source_115') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_48 as (
    select * from {{ ref('stg_source_103') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_113') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_50_id']) }} as surrogate_id,
        dep_1.stg_source_50_id as fct_business_event_155_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_50_id = dep_2.stg_source_142_id
    left join dep_3 on dep_1.stg_source_50_id = dep_3.int_transformed_110_id
    left join dep_4 on dep_1.stg_source_50_id = dep_4.stg_source_123_id
    left join dep_5 on dep_1.stg_source_50_id = dep_5.stg_source_158_id
    left join dep_6 on dep_1.stg_source_50_id = dep_6.int_transformed_103_id
    left join dep_7 on dep_1.stg_source_50_id = dep_7.int_transformed_107_id
    left join dep_8 on dep_1.stg_source_50_id = dep_8.int_transformed_148_id
    left join dep_9 on dep_1.stg_source_50_id = dep_9.int_transformed_32_id
    left join dep_10 on dep_1.stg_source_50_id = dep_10.stg_source_128_id
    left join dep_11 on dep_1.stg_source_50_id = dep_11.stg_source_73_id
    left join dep_12 on dep_1.stg_source_50_id = dep_12.int_transformed_90_id
    left join dep_13 on dep_1.stg_source_50_id = dep_13.stg_source_136_id
    left join dep_14 on dep_1.stg_source_50_id = dep_14.stg_source_31_id
    left join dep_15 on dep_1.stg_source_50_id = dep_15.stg_source_27_id
    left join dep_16 on dep_1.stg_source_50_id = dep_16.stg_source_2_id
    left join dep_17 on dep_1.stg_source_50_id = dep_17.int_transformed_19_id
    left join dep_18 on dep_1.stg_source_50_id = dep_18.int_transformed_53_id
    left join dep_19 on dep_1.stg_source_50_id = dep_19.stg_source_3_id
    left join dep_20 on dep_1.stg_source_50_id = dep_20.int_transformed_92_id
    left join dep_21 on dep_1.stg_source_50_id = dep_21.stg_source_94_id
    left join dep_22 on dep_1.stg_source_50_id = dep_22.stg_source_91_id
    left join dep_23 on dep_1.stg_source_50_id = dep_23.int_transformed_81_id
    left join dep_24 on dep_1.stg_source_50_id = dep_24.stg_source_51_id
    left join dep_25 on dep_1.stg_source_50_id = dep_25.stg_source_95_id
    left join dep_26 on dep_1.stg_source_50_id = dep_26.stg_source_10_id
    left join dep_27 on dep_1.stg_source_50_id = dep_27.int_transformed_161_id
    left join dep_28 on dep_1.stg_source_50_id = dep_28.int_transformed_1_id
    left join dep_29 on dep_1.stg_source_50_id = dep_29.int_transformed_98_id
    left join dep_30 on dep_1.stg_source_50_id = dep_30.stg_source_35_id
    left join dep_31 on dep_1.stg_source_50_id = dep_31.int_transformed_165_id
    left join dep_32 on dep_1.stg_source_50_id = dep_32.stg_source_74_id
    left join dep_33 on dep_1.stg_source_50_id = dep_33.int_transformed_105_id
    left join dep_34 on dep_1.stg_source_50_id = dep_34.int_transformed_72_id
    left join dep_35 on dep_1.stg_source_50_id = dep_35.int_transformed_133_id
    left join dep_36 on dep_1.stg_source_50_id = dep_36.stg_source_54_id
    left join dep_37 on dep_1.stg_source_50_id = dep_37.stg_source_79_id
    left join dep_38 on dep_1.stg_source_50_id = dep_38.int_transformed_18_id
    left join dep_39 on dep_1.stg_source_50_id = dep_39.int_transformed_120_id
    left join dep_40 on dep_1.stg_source_50_id = dep_40.stg_source_49_id
    left join dep_41 on dep_1.stg_source_50_id = dep_41.int_transformed_114_id
    left join dep_42 on dep_1.stg_source_50_id = dep_42.stg_source_88_id
    left join dep_43 on dep_1.stg_source_50_id = dep_43.stg_source_9_id
    left join dep_44 on dep_1.stg_source_50_id = dep_44.int_transformed_78_id
    left join dep_45 on dep_1.stg_source_50_id = dep_45.stg_source_56_id
    left join dep_46 on dep_1.stg_source_50_id = dep_46.stg_source_115_id
    left join dep_47 on dep_1.stg_source_50_id = dep_47.int_transformed_155_id
    left join dep_48 on dep_1.stg_source_50_id = dep_48.stg_source_103_id
    left join dep_49 on dep_1.stg_source_50_id = dep_49.int_transformed_82_id
    left join dep_50 on dep_1.stg_source_50_id = dep_50.int_transformed_113_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
