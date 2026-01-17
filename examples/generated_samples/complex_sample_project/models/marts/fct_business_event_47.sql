{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_94') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_5 as (
    select * from {{ ref('stg_source_123') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_8 as (
    select * from {{ ref('stg_source_137') }}
),

dep_9 as (
    select * from {{ ref('stg_source_31') }}
),

dep_10 as (
    select * from {{ ref('stg_source_40') }}
),

dep_11 as (
    select * from {{ ref('stg_source_62') }}
),

dep_12 as (
    select * from {{ ref('stg_source_47') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_14 as (
    select * from {{ ref('stg_source_44') }}
),

dep_15 as (
    select * from {{ ref('stg_source_41') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_18 as (
    select * from {{ ref('stg_source_126') }}
),

dep_19 as (
    select * from {{ ref('stg_source_27') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_21 as (
    select * from {{ ref('stg_source_156') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_24 as (
    select * from {{ ref('stg_source_21') }}
),

dep_25 as (
    select * from {{ ref('stg_source_2') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_27 as (
    select * from {{ ref('stg_source_115') }}
),

dep_28 as (
    select * from {{ ref('stg_source_42') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_30 as (
    select * from {{ ref('stg_source_79') }}
),

dep_31 as (
    select * from {{ ref('stg_source_33') }}
),

dep_32 as (
    select * from {{ ref('stg_source_129') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_34 as (
    select * from {{ ref('stg_source_5') }}
),

dep_35 as (
    select * from {{ ref('stg_source_139') }}
),

dep_36 as (
    select * from {{ ref('stg_source_152') }}
),

dep_37 as (
    select * from {{ ref('stg_source_20') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_40 as (
    select * from {{ ref('stg_source_155') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_42 as (
    select * from {{ ref('stg_source_95') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_44 as (
    select * from {{ ref('stg_source_101') }}
),

dep_45 as (
    select * from {{ ref('stg_source_97') }}
),

dep_46 as (
    select * from {{ ref('stg_source_77') }}
),

dep_47 as (
    select * from {{ ref('stg_source_130') }}
),

dep_48 as (
    select * from {{ ref('stg_source_131') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_50 as (
    select * from {{ ref('stg_source_17') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_94_id']) }} as surrogate_id,
        dep_1.stg_source_94_id as fct_business_event_47_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_94_id = dep_2.int_transformed_152_id
    left join dep_3 on dep_1.stg_source_94_id = dep_3.int_transformed_15_id
    left join dep_4 on dep_1.stg_source_94_id = dep_4.int_transformed_76_id
    left join dep_5 on dep_1.stg_source_94_id = dep_5.stg_source_123_id
    left join dep_6 on dep_1.stg_source_94_id = dep_6.int_transformed_148_id
    left join dep_7 on dep_1.stg_source_94_id = dep_7.int_transformed_120_id
    left join dep_8 on dep_1.stg_source_94_id = dep_8.stg_source_137_id
    left join dep_9 on dep_1.stg_source_94_id = dep_9.stg_source_31_id
    left join dep_10 on dep_1.stg_source_94_id = dep_10.stg_source_40_id
    left join dep_11 on dep_1.stg_source_94_id = dep_11.stg_source_62_id
    left join dep_12 on dep_1.stg_source_94_id = dep_12.stg_source_47_id
    left join dep_13 on dep_1.stg_source_94_id = dep_13.int_transformed_113_id
    left join dep_14 on dep_1.stg_source_94_id = dep_14.stg_source_44_id
    left join dep_15 on dep_1.stg_source_94_id = dep_15.stg_source_41_id
    left join dep_16 on dep_1.stg_source_94_id = dep_16.int_transformed_58_id
    left join dep_17 on dep_1.stg_source_94_id = dep_17.int_transformed_10_id
    left join dep_18 on dep_1.stg_source_94_id = dep_18.stg_source_126_id
    left join dep_19 on dep_1.stg_source_94_id = dep_19.stg_source_27_id
    left join dep_20 on dep_1.stg_source_94_id = dep_20.int_transformed_71_id
    left join dep_21 on dep_1.stg_source_94_id = dep_21.stg_source_156_id
    left join dep_22 on dep_1.stg_source_94_id = dep_22.int_transformed_147_id
    left join dep_23 on dep_1.stg_source_94_id = dep_23.int_transformed_47_id
    left join dep_24 on dep_1.stg_source_94_id = dep_24.stg_source_21_id
    left join dep_25 on dep_1.stg_source_94_id = dep_25.stg_source_2_id
    left join dep_26 on dep_1.stg_source_94_id = dep_26.int_transformed_68_id
    left join dep_27 on dep_1.stg_source_94_id = dep_27.stg_source_115_id
    left join dep_28 on dep_1.stg_source_94_id = dep_28.stg_source_42_id
    left join dep_29 on dep_1.stg_source_94_id = dep_29.int_transformed_100_id
    left join dep_30 on dep_1.stg_source_94_id = dep_30.stg_source_79_id
    left join dep_31 on dep_1.stg_source_94_id = dep_31.stg_source_33_id
    left join dep_32 on dep_1.stg_source_94_id = dep_32.stg_source_129_id
    left join dep_33 on dep_1.stg_source_94_id = dep_33.int_transformed_102_id
    left join dep_34 on dep_1.stg_source_94_id = dep_34.stg_source_5_id
    left join dep_35 on dep_1.stg_source_94_id = dep_35.stg_source_139_id
    left join dep_36 on dep_1.stg_source_94_id = dep_36.stg_source_152_id
    left join dep_37 on dep_1.stg_source_94_id = dep_37.stg_source_20_id
    left join dep_38 on dep_1.stg_source_94_id = dep_38.int_transformed_141_id
    left join dep_39 on dep_1.stg_source_94_id = dep_39.int_transformed_41_id
    left join dep_40 on dep_1.stg_source_94_id = dep_40.stg_source_155_id
    left join dep_41 on dep_1.stg_source_94_id = dep_41.int_transformed_64_id
    left join dep_42 on dep_1.stg_source_94_id = dep_42.stg_source_95_id
    left join dep_43 on dep_1.stg_source_94_id = dep_43.int_transformed_9_id
    left join dep_44 on dep_1.stg_source_94_id = dep_44.stg_source_101_id
    left join dep_45 on dep_1.stg_source_94_id = dep_45.stg_source_97_id
    left join dep_46 on dep_1.stg_source_94_id = dep_46.stg_source_77_id
    left join dep_47 on dep_1.stg_source_94_id = dep_47.stg_source_130_id
    left join dep_48 on dep_1.stg_source_94_id = dep_48.stg_source_131_id
    left join dep_49 on dep_1.stg_source_94_id = dep_49.int_transformed_97_id
    left join dep_50 on dep_1.stg_source_94_id = dep_50.stg_source_17_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
