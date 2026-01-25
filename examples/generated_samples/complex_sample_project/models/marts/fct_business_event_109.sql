{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_6 as (
    select * from {{ ref('stg_source_50') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_9 as (
    select * from {{ ref('stg_source_126') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_13 as (
    select * from {{ ref('stg_source_119') }}
),

dep_14 as (
    select * from {{ ref('stg_source_34') }}
),

dep_15 as (
    select * from {{ ref('stg_source_17') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_18 as (
    select * from {{ ref('stg_source_151') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_22 as (
    select * from {{ ref('stg_source_144') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_24 as (
    select * from {{ ref('stg_source_54') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_26 as (
    select * from {{ ref('stg_source_141') }}
),

dep_27 as (
    select * from {{ ref('stg_source_132') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_32 as (
    select * from {{ ref('stg_source_94') }}
),

dep_33 as (
    select * from {{ ref('stg_source_165') }}
),

dep_34 as (
    select * from {{ ref('stg_source_4') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_39 as (
    select * from {{ ref('stg_source_63') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_41 as (
    select * from {{ ref('stg_source_145') }}
),

dep_42 as (
    select * from {{ ref('stg_source_39') }}
),

dep_43 as (
    select * from {{ ref('stg_source_31') }}
),

dep_44 as (
    select * from {{ ref('stg_source_2') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_46 as (
    select * from {{ ref('stg_source_24') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_49 as (
    select * from {{ ref('stg_source_102') }}
),

dep_50 as (
    select * from {{ ref('stg_source_38') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_131_id']) }} as surrogate_id,
        dep_1.int_transformed_131_id as fct_business_event_109_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_131_id = dep_2.int_transformed_134_id
    left join dep_3 on dep_1.int_transformed_131_id = dep_3.int_transformed_14_id
    left join dep_4 on dep_1.int_transformed_131_id = dep_4.int_transformed_91_id
    left join dep_5 on dep_1.int_transformed_131_id = dep_5.int_transformed_78_id
    left join dep_6 on dep_1.int_transformed_131_id = dep_6.stg_source_50_id
    left join dep_7 on dep_1.int_transformed_131_id = dep_7.int_transformed_84_id
    left join dep_8 on dep_1.int_transformed_131_id = dep_8.int_transformed_117_id
    left join dep_9 on dep_1.int_transformed_131_id = dep_9.stg_source_126_id
    left join dep_10 on dep_1.int_transformed_131_id = dep_10.int_transformed_58_id
    left join dep_11 on dep_1.int_transformed_131_id = dep_11.int_transformed_109_id
    left join dep_12 on dep_1.int_transformed_131_id = dep_12.int_transformed_132_id
    left join dep_13 on dep_1.int_transformed_131_id = dep_13.stg_source_119_id
    left join dep_14 on dep_1.int_transformed_131_id = dep_14.stg_source_34_id
    left join dep_15 on dep_1.int_transformed_131_id = dep_15.stg_source_17_id
    left join dep_16 on dep_1.int_transformed_131_id = dep_16.int_transformed_99_id
    left join dep_17 on dep_1.int_transformed_131_id = dep_17.int_transformed_125_id
    left join dep_18 on dep_1.int_transformed_131_id = dep_18.stg_source_151_id
    left join dep_19 on dep_1.int_transformed_131_id = dep_19.int_transformed_159_id
    left join dep_20 on dep_1.int_transformed_131_id = dep_20.int_transformed_124_id
    left join dep_21 on dep_1.int_transformed_131_id = dep_21.int_transformed_31_id
    left join dep_22 on dep_1.int_transformed_131_id = dep_22.stg_source_144_id
    left join dep_23 on dep_1.int_transformed_131_id = dep_23.int_transformed_28_id
    left join dep_24 on dep_1.int_transformed_131_id = dep_24.stg_source_54_id
    left join dep_25 on dep_1.int_transformed_131_id = dep_25.int_transformed_136_id
    left join dep_26 on dep_1.int_transformed_131_id = dep_26.stg_source_141_id
    left join dep_27 on dep_1.int_transformed_131_id = dep_27.stg_source_132_id
    left join dep_28 on dep_1.int_transformed_131_id = dep_28.int_transformed_86_id
    left join dep_29 on dep_1.int_transformed_131_id = dep_29.int_transformed_158_id
    left join dep_30 on dep_1.int_transformed_131_id = dep_30.int_transformed_96_id
    left join dep_31 on dep_1.int_transformed_131_id = dep_31.int_transformed_114_id
    left join dep_32 on dep_1.int_transformed_131_id = dep_32.stg_source_94_id
    left join dep_33 on dep_1.int_transformed_131_id = dep_33.stg_source_165_id
    left join dep_34 on dep_1.int_transformed_131_id = dep_34.stg_source_4_id
    left join dep_35 on dep_1.int_transformed_131_id = dep_35.int_transformed_143_id
    left join dep_36 on dep_1.int_transformed_131_id = dep_36.int_transformed_71_id
    left join dep_37 on dep_1.int_transformed_131_id = dep_37.int_transformed_160_id
    left join dep_38 on dep_1.int_transformed_131_id = dep_38.int_transformed_33_id
    left join dep_39 on dep_1.int_transformed_131_id = dep_39.stg_source_63_id
    left join dep_40 on dep_1.int_transformed_131_id = dep_40.int_transformed_7_id
    left join dep_41 on dep_1.int_transformed_131_id = dep_41.stg_source_145_id
    left join dep_42 on dep_1.int_transformed_131_id = dep_42.stg_source_39_id
    left join dep_43 on dep_1.int_transformed_131_id = dep_43.stg_source_31_id
    left join dep_44 on dep_1.int_transformed_131_id = dep_44.stg_source_2_id
    left join dep_45 on dep_1.int_transformed_131_id = dep_45.int_transformed_87_id
    left join dep_46 on dep_1.int_transformed_131_id = dep_46.stg_source_24_id
    left join dep_47 on dep_1.int_transformed_131_id = dep_47.int_transformed_116_id
    left join dep_48 on dep_1.int_transformed_131_id = dep_48.int_transformed_133_id
    left join dep_49 on dep_1.int_transformed_131_id = dep_49.stg_source_102_id
    left join dep_50 on dep_1.int_transformed_131_id = dep_50.stg_source_38_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
