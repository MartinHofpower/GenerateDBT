{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_141') }}
),

dep_2 as (
    select * from {{ ref('stg_source_57') }}
),

dep_3 as (
    select * from {{ ref('stg_source_22') }}
),

dep_4 as (
    select * from {{ ref('stg_source_26') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_8 as (
    select * from {{ ref('stg_source_69') }}
),

dep_9 as (
    select * from {{ ref('stg_source_90') }}
),

dep_10 as (
    select * from {{ ref('stg_source_17') }}
),

dep_11 as (
    select * from {{ ref('stg_source_47') }}
),

dep_12 as (
    select * from {{ ref('stg_source_79') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_14 as (
    select * from {{ ref('stg_source_37') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_16 as (
    select * from {{ ref('stg_source_161') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_18 as (
    select * from {{ ref('stg_source_27') }}
),

dep_19 as (
    select * from {{ ref('stg_source_142') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_21 as (
    select * from {{ ref('stg_source_153') }}
),

dep_22 as (
    select * from {{ ref('stg_source_65') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_24 as (
    select * from {{ ref('stg_source_89') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_27 as (
    select * from {{ ref('stg_source_29') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_29 as (
    select * from {{ ref('stg_source_33') }}
),

dep_30 as (
    select * from {{ ref('stg_source_18') }}
),

dep_31 as (
    select * from {{ ref('stg_source_6') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_33 as (
    select * from {{ ref('stg_source_64') }}
),

dep_34 as (
    select * from {{ ref('stg_source_45') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_38 as (
    select * from {{ ref('stg_source_54') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_41 as (
    select * from {{ ref('stg_source_41') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_44 as (
    select * from {{ ref('stg_source_102') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_46 as (
    select * from {{ ref('stg_source_108') }}
),

dep_47 as (
    select * from {{ ref('stg_source_144') }}
),

dep_48 as (
    select * from {{ ref('stg_source_155') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_50 as (
    select * from {{ ref('stg_source_94') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_141_id']) }} as surrogate_id,
        dep_1.stg_source_141_id as dim_entity_156_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_141_id = dep_2.stg_source_57_id
    left join dep_3 on dep_1.stg_source_141_id = dep_3.stg_source_22_id
    left join dep_4 on dep_1.stg_source_141_id = dep_4.stg_source_26_id
    left join dep_5 on dep_1.stg_source_141_id = dep_5.int_transformed_59_id
    left join dep_6 on dep_1.stg_source_141_id = dep_6.int_transformed_50_id
    left join dep_7 on dep_1.stg_source_141_id = dep_7.int_transformed_10_id
    left join dep_8 on dep_1.stg_source_141_id = dep_8.stg_source_69_id
    left join dep_9 on dep_1.stg_source_141_id = dep_9.stg_source_90_id
    left join dep_10 on dep_1.stg_source_141_id = dep_10.stg_source_17_id
    left join dep_11 on dep_1.stg_source_141_id = dep_11.stg_source_47_id
    left join dep_12 on dep_1.stg_source_141_id = dep_12.stg_source_79_id
    left join dep_13 on dep_1.stg_source_141_id = dep_13.int_transformed_147_id
    left join dep_14 on dep_1.stg_source_141_id = dep_14.stg_source_37_id
    left join dep_15 on dep_1.stg_source_141_id = dep_15.int_transformed_14_id
    left join dep_16 on dep_1.stg_source_141_id = dep_16.stg_source_161_id
    left join dep_17 on dep_1.stg_source_141_id = dep_17.int_transformed_73_id
    left join dep_18 on dep_1.stg_source_141_id = dep_18.stg_source_27_id
    left join dep_19 on dep_1.stg_source_141_id = dep_19.stg_source_142_id
    left join dep_20 on dep_1.stg_source_141_id = dep_20.int_transformed_105_id
    left join dep_21 on dep_1.stg_source_141_id = dep_21.stg_source_153_id
    left join dep_22 on dep_1.stg_source_141_id = dep_22.stg_source_65_id
    left join dep_23 on dep_1.stg_source_141_id = dep_23.int_transformed_102_id
    left join dep_24 on dep_1.stg_source_141_id = dep_24.stg_source_89_id
    left join dep_25 on dep_1.stg_source_141_id = dep_25.int_transformed_137_id
    left join dep_26 on dep_1.stg_source_141_id = dep_26.int_transformed_6_id
    left join dep_27 on dep_1.stg_source_141_id = dep_27.stg_source_29_id
    left join dep_28 on dep_1.stg_source_141_id = dep_28.int_transformed_41_id
    left join dep_29 on dep_1.stg_source_141_id = dep_29.stg_source_33_id
    left join dep_30 on dep_1.stg_source_141_id = dep_30.stg_source_18_id
    left join dep_31 on dep_1.stg_source_141_id = dep_31.stg_source_6_id
    left join dep_32 on dep_1.stg_source_141_id = dep_32.int_transformed_42_id
    left join dep_33 on dep_1.stg_source_141_id = dep_33.stg_source_64_id
    left join dep_34 on dep_1.stg_source_141_id = dep_34.stg_source_45_id
    left join dep_35 on dep_1.stg_source_141_id = dep_35.int_transformed_109_id
    left join dep_36 on dep_1.stg_source_141_id = dep_36.int_transformed_58_id
    left join dep_37 on dep_1.stg_source_141_id = dep_37.int_transformed_139_id
    left join dep_38 on dep_1.stg_source_141_id = dep_38.stg_source_54_id
    left join dep_39 on dep_1.stg_source_141_id = dep_39.int_transformed_45_id
    left join dep_40 on dep_1.stg_source_141_id = dep_40.int_transformed_141_id
    left join dep_41 on dep_1.stg_source_141_id = dep_41.stg_source_41_id
    left join dep_42 on dep_1.stg_source_141_id = dep_42.int_transformed_150_id
    left join dep_43 on dep_1.stg_source_141_id = dep_43.int_transformed_92_id
    left join dep_44 on dep_1.stg_source_141_id = dep_44.stg_source_102_id
    left join dep_45 on dep_1.stg_source_141_id = dep_45.int_transformed_3_id
    left join dep_46 on dep_1.stg_source_141_id = dep_46.stg_source_108_id
    left join dep_47 on dep_1.stg_source_141_id = dep_47.stg_source_144_id
    left join dep_48 on dep_1.stg_source_141_id = dep_48.stg_source_155_id
    left join dep_49 on dep_1.stg_source_141_id = dep_49.int_transformed_35_id
    left join dep_50 on dep_1.stg_source_141_id = dep_50.stg_source_94_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
