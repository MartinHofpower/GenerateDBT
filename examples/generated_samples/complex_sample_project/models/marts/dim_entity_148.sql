{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_3 as (
    select * from {{ ref('stg_source_137') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_6 as (
    select * from {{ ref('stg_source_110') }}
),

dep_7 as (
    select * from {{ ref('stg_source_74') }}
),

dep_8 as (
    select * from {{ ref('stg_source_53') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_14 as (
    select * from {{ ref('stg_source_161') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_16 as (
    select * from {{ ref('stg_source_149') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_18 as (
    select * from {{ ref('stg_source_121') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_20 as (
    select * from {{ ref('stg_source_122') }}
),

dep_21 as (
    select * from {{ ref('stg_source_69') }}
),

dep_22 as (
    select * from {{ ref('stg_source_52') }}
),

dep_23 as (
    select * from {{ ref('stg_source_8') }}
),

dep_24 as (
    select * from {{ ref('stg_source_20') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_26 as (
    select * from {{ ref('stg_source_59') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_32 as (
    select * from {{ ref('stg_source_36') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_34 as (
    select * from {{ ref('stg_source_115') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_37 as (
    select * from {{ ref('stg_source_114') }}
),

dep_38 as (
    select * from {{ ref('stg_source_7') }}
),

dep_39 as (
    select * from {{ ref('stg_source_22') }}
),

dep_40 as (
    select * from {{ ref('stg_source_166') }}
),

dep_41 as (
    select * from {{ ref('stg_source_132') }}
),

dep_42 as (
    select * from {{ ref('stg_source_47') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_45 as (
    select * from {{ ref('stg_source_78') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_49 as (
    select * from {{ ref('stg_source_10') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_81') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_128_id']) }} as surrogate_id,
        dep_1.int_transformed_128_id as dim_entity_148_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_128_id = dep_2.int_transformed_94_id
    left join dep_3 on dep_1.int_transformed_128_id = dep_3.stg_source_137_id
    left join dep_4 on dep_1.int_transformed_128_id = dep_4.int_transformed_46_id
    left join dep_5 on dep_1.int_transformed_128_id = dep_5.int_transformed_113_id
    left join dep_6 on dep_1.int_transformed_128_id = dep_6.stg_source_110_id
    left join dep_7 on dep_1.int_transformed_128_id = dep_7.stg_source_74_id
    left join dep_8 on dep_1.int_transformed_128_id = dep_8.stg_source_53_id
    left join dep_9 on dep_1.int_transformed_128_id = dep_9.int_transformed_33_id
    left join dep_10 on dep_1.int_transformed_128_id = dep_10.int_transformed_98_id
    left join dep_11 on dep_1.int_transformed_128_id = dep_11.int_transformed_34_id
    left join dep_12 on dep_1.int_transformed_128_id = dep_12.int_transformed_85_id
    left join dep_13 on dep_1.int_transformed_128_id = dep_13.int_transformed_122_id
    left join dep_14 on dep_1.int_transformed_128_id = dep_14.stg_source_161_id
    left join dep_15 on dep_1.int_transformed_128_id = dep_15.int_transformed_90_id
    left join dep_16 on dep_1.int_transformed_128_id = dep_16.stg_source_149_id
    left join dep_17 on dep_1.int_transformed_128_id = dep_17.int_transformed_106_id
    left join dep_18 on dep_1.int_transformed_128_id = dep_18.stg_source_121_id
    left join dep_19 on dep_1.int_transformed_128_id = dep_19.int_transformed_157_id
    left join dep_20 on dep_1.int_transformed_128_id = dep_20.stg_source_122_id
    left join dep_21 on dep_1.int_transformed_128_id = dep_21.stg_source_69_id
    left join dep_22 on dep_1.int_transformed_128_id = dep_22.stg_source_52_id
    left join dep_23 on dep_1.int_transformed_128_id = dep_23.stg_source_8_id
    left join dep_24 on dep_1.int_transformed_128_id = dep_24.stg_source_20_id
    left join dep_25 on dep_1.int_transformed_128_id = dep_25.int_transformed_141_id
    left join dep_26 on dep_1.int_transformed_128_id = dep_26.stg_source_59_id
    left join dep_27 on dep_1.int_transformed_128_id = dep_27.int_transformed_20_id
    left join dep_28 on dep_1.int_transformed_128_id = dep_28.int_transformed_31_id
    left join dep_29 on dep_1.int_transformed_128_id = dep_29.int_transformed_139_id
    left join dep_30 on dep_1.int_transformed_128_id = dep_30.int_transformed_126_id
    left join dep_31 on dep_1.int_transformed_128_id = dep_31.int_transformed_130_id
    left join dep_32 on dep_1.int_transformed_128_id = dep_32.stg_source_36_id
    left join dep_33 on dep_1.int_transformed_128_id = dep_33.int_transformed_82_id
    left join dep_34 on dep_1.int_transformed_128_id = dep_34.stg_source_115_id
    left join dep_35 on dep_1.int_transformed_128_id = dep_35.int_transformed_77_id
    left join dep_36 on dep_1.int_transformed_128_id = dep_36.int_transformed_110_id
    left join dep_37 on dep_1.int_transformed_128_id = dep_37.stg_source_114_id
    left join dep_38 on dep_1.int_transformed_128_id = dep_38.stg_source_7_id
    left join dep_39 on dep_1.int_transformed_128_id = dep_39.stg_source_22_id
    left join dep_40 on dep_1.int_transformed_128_id = dep_40.stg_source_166_id
    left join dep_41 on dep_1.int_transformed_128_id = dep_41.stg_source_132_id
    left join dep_42 on dep_1.int_transformed_128_id = dep_42.stg_source_47_id
    left join dep_43 on dep_1.int_transformed_128_id = dep_43.int_transformed_100_id
    left join dep_44 on dep_1.int_transformed_128_id = dep_44.int_transformed_45_id
    left join dep_45 on dep_1.int_transformed_128_id = dep_45.stg_source_78_id
    left join dep_46 on dep_1.int_transformed_128_id = dep_46.int_transformed_2_id
    left join dep_47 on dep_1.int_transformed_128_id = dep_47.int_transformed_111_id
    left join dep_48 on dep_1.int_transformed_128_id = dep_48.int_transformed_102_id
    left join dep_49 on dep_1.int_transformed_128_id = dep_49.stg_source_10_id
    left join dep_50 on dep_1.int_transformed_128_id = dep_50.int_transformed_81_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
