{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_5 as (
    select * from {{ ref('stg_source_144') }}
),

dep_6 as (
    select * from {{ ref('stg_source_160') }}
),

dep_7 as (
    select * from {{ ref('stg_source_84') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_11 as (
    select * from {{ ref('stg_source_39') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_13 as (
    select * from {{ ref('stg_source_109') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_19 as (
    select * from {{ ref('stg_source_2') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_22 as (
    select * from {{ ref('stg_source_75') }}
),

dep_23 as (
    select * from {{ ref('stg_source_28') }}
),

dep_24 as (
    select * from {{ ref('stg_source_133') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_26 as (
    select * from {{ ref('stg_source_7') }}
),

dep_27 as (
    select * from {{ ref('stg_source_135') }}
),

dep_28 as (
    select * from {{ ref('stg_source_83') }}
),

dep_29 as (
    select * from {{ ref('stg_source_44') }}
),

dep_30 as (
    select * from {{ ref('stg_source_138') }}
),

dep_31 as (
    select * from {{ ref('stg_source_152') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_38 as (
    select * from {{ ref('stg_source_51') }}
),

dep_39 as (
    select * from {{ ref('stg_source_113') }}
),

dep_40 as (
    select * from {{ ref('stg_source_11') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_45 as (
    select * from {{ ref('stg_source_157') }}
),

dep_46 as (
    select * from {{ ref('stg_source_43') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_50 as (
    select * from {{ ref('stg_source_38') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_29_id']) }} as surrogate_id,
        dep_1.int_transformed_29_id as fct_business_event_95_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_29_id = dep_2.int_transformed_94_id
    left join dep_3 on dep_1.int_transformed_29_id = dep_3.int_transformed_79_id
    left join dep_4 on dep_1.int_transformed_29_id = dep_4.int_transformed_120_id
    left join dep_5 on dep_1.int_transformed_29_id = dep_5.stg_source_144_id
    left join dep_6 on dep_1.int_transformed_29_id = dep_6.stg_source_160_id
    left join dep_7 on dep_1.int_transformed_29_id = dep_7.stg_source_84_id
    left join dep_8 on dep_1.int_transformed_29_id = dep_8.int_transformed_75_id
    left join dep_9 on dep_1.int_transformed_29_id = dep_9.int_transformed_13_id
    left join dep_10 on dep_1.int_transformed_29_id = dep_10.int_transformed_28_id
    left join dep_11 on dep_1.int_transformed_29_id = dep_11.stg_source_39_id
    left join dep_12 on dep_1.int_transformed_29_id = dep_12.int_transformed_90_id
    left join dep_13 on dep_1.int_transformed_29_id = dep_13.stg_source_109_id
    left join dep_14 on dep_1.int_transformed_29_id = dep_14.int_transformed_136_id
    left join dep_15 on dep_1.int_transformed_29_id = dep_15.int_transformed_126_id
    left join dep_16 on dep_1.int_transformed_29_id = dep_16.int_transformed_53_id
    left join dep_17 on dep_1.int_transformed_29_id = dep_17.int_transformed_55_id
    left join dep_18 on dep_1.int_transformed_29_id = dep_18.int_transformed_17_id
    left join dep_19 on dep_1.int_transformed_29_id = dep_19.stg_source_2_id
    left join dep_20 on dep_1.int_transformed_29_id = dep_20.int_transformed_106_id
    left join dep_21 on dep_1.int_transformed_29_id = dep_21.int_transformed_33_id
    left join dep_22 on dep_1.int_transformed_29_id = dep_22.stg_source_75_id
    left join dep_23 on dep_1.int_transformed_29_id = dep_23.stg_source_28_id
    left join dep_24 on dep_1.int_transformed_29_id = dep_24.stg_source_133_id
    left join dep_25 on dep_1.int_transformed_29_id = dep_25.int_transformed_44_id
    left join dep_26 on dep_1.int_transformed_29_id = dep_26.stg_source_7_id
    left join dep_27 on dep_1.int_transformed_29_id = dep_27.stg_source_135_id
    left join dep_28 on dep_1.int_transformed_29_id = dep_28.stg_source_83_id
    left join dep_29 on dep_1.int_transformed_29_id = dep_29.stg_source_44_id
    left join dep_30 on dep_1.int_transformed_29_id = dep_30.stg_source_138_id
    left join dep_31 on dep_1.int_transformed_29_id = dep_31.stg_source_152_id
    left join dep_32 on dep_1.int_transformed_29_id = dep_32.int_transformed_7_id
    left join dep_33 on dep_1.int_transformed_29_id = dep_33.int_transformed_158_id
    left join dep_34 on dep_1.int_transformed_29_id = dep_34.int_transformed_122_id
    left join dep_35 on dep_1.int_transformed_29_id = dep_35.int_transformed_150_id
    left join dep_36 on dep_1.int_transformed_29_id = dep_36.int_transformed_38_id
    left join dep_37 on dep_1.int_transformed_29_id = dep_37.int_transformed_145_id
    left join dep_38 on dep_1.int_transformed_29_id = dep_38.stg_source_51_id
    left join dep_39 on dep_1.int_transformed_29_id = dep_39.stg_source_113_id
    left join dep_40 on dep_1.int_transformed_29_id = dep_40.stg_source_11_id
    left join dep_41 on dep_1.int_transformed_29_id = dep_41.int_transformed_163_id
    left join dep_42 on dep_1.int_transformed_29_id = dep_42.int_transformed_22_id
    left join dep_43 on dep_1.int_transformed_29_id = dep_43.int_transformed_149_id
    left join dep_44 on dep_1.int_transformed_29_id = dep_44.int_transformed_103_id
    left join dep_45 on dep_1.int_transformed_29_id = dep_45.stg_source_157_id
    left join dep_46 on dep_1.int_transformed_29_id = dep_46.stg_source_43_id
    left join dep_47 on dep_1.int_transformed_29_id = dep_47.int_transformed_78_id
    left join dep_48 on dep_1.int_transformed_29_id = dep_48.int_transformed_50_id
    left join dep_49 on dep_1.int_transformed_29_id = dep_49.int_transformed_105_id
    left join dep_50 on dep_1.int_transformed_29_id = dep_50.stg_source_38_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
