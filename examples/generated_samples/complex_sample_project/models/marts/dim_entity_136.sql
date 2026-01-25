{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_21') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_3 as (
    select * from {{ ref('stg_source_15') }}
),

dep_4 as (
    select * from {{ ref('stg_source_43') }}
),

dep_5 as (
    select * from {{ ref('stg_source_114') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_8 as (
    select * from {{ ref('stg_source_48') }}
),

dep_9 as (
    select * from {{ ref('stg_source_71') }}
),

dep_10 as (
    select * from {{ ref('stg_source_111') }}
),

dep_11 as (
    select * from {{ ref('stg_source_23') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_13 as (
    select * from {{ ref('stg_source_50') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_17 as (
    select * from {{ ref('stg_source_97') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_21 as (
    select * from {{ ref('stg_source_75') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_24 as (
    select * from {{ ref('stg_source_107') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_26 as (
    select * from {{ ref('stg_source_82') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_28 as (
    select * from {{ ref('stg_source_16') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_32 as (
    select * from {{ ref('stg_source_156') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_34 as (
    select * from {{ ref('stg_source_153') }}
),

dep_35 as (
    select * from {{ ref('stg_source_93') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_38 as (
    select * from {{ ref('stg_source_77') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_43 as (
    select * from {{ ref('stg_source_49') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_94') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_21_id']) }} as surrogate_id,
        dep_1.stg_source_21_id as dim_entity_136_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_21_id = dep_2.int_transformed_107_id
    left join dep_3 on dep_1.stg_source_21_id = dep_3.stg_source_15_id
    left join dep_4 on dep_1.stg_source_21_id = dep_4.stg_source_43_id
    left join dep_5 on dep_1.stg_source_21_id = dep_5.stg_source_114_id
    left join dep_6 on dep_1.stg_source_21_id = dep_6.int_transformed_47_id
    left join dep_7 on dep_1.stg_source_21_id = dep_7.int_transformed_65_id
    left join dep_8 on dep_1.stg_source_21_id = dep_8.stg_source_48_id
    left join dep_9 on dep_1.stg_source_21_id = dep_9.stg_source_71_id
    left join dep_10 on dep_1.stg_source_21_id = dep_10.stg_source_111_id
    left join dep_11 on dep_1.stg_source_21_id = dep_11.stg_source_23_id
    left join dep_12 on dep_1.stg_source_21_id = dep_12.int_transformed_8_id
    left join dep_13 on dep_1.stg_source_21_id = dep_13.stg_source_50_id
    left join dep_14 on dep_1.stg_source_21_id = dep_14.int_transformed_42_id
    left join dep_15 on dep_1.stg_source_21_id = dep_15.int_transformed_5_id
    left join dep_16 on dep_1.stg_source_21_id = dep_16.int_transformed_147_id
    left join dep_17 on dep_1.stg_source_21_id = dep_17.stg_source_97_id
    left join dep_18 on dep_1.stg_source_21_id = dep_18.int_transformed_32_id
    left join dep_19 on dep_1.stg_source_21_id = dep_19.int_transformed_128_id
    left join dep_20 on dep_1.stg_source_21_id = dep_20.int_transformed_122_id
    left join dep_21 on dep_1.stg_source_21_id = dep_21.stg_source_75_id
    left join dep_22 on dep_1.stg_source_21_id = dep_22.int_transformed_126_id
    left join dep_23 on dep_1.stg_source_21_id = dep_23.int_transformed_70_id
    left join dep_24 on dep_1.stg_source_21_id = dep_24.stg_source_107_id
    left join dep_25 on dep_1.stg_source_21_id = dep_25.int_transformed_52_id
    left join dep_26 on dep_1.stg_source_21_id = dep_26.stg_source_82_id
    left join dep_27 on dep_1.stg_source_21_id = dep_27.int_transformed_29_id
    left join dep_28 on dep_1.stg_source_21_id = dep_28.stg_source_16_id
    left join dep_29 on dep_1.stg_source_21_id = dep_29.int_transformed_152_id
    left join dep_30 on dep_1.stg_source_21_id = dep_30.int_transformed_155_id
    left join dep_31 on dep_1.stg_source_21_id = dep_31.int_transformed_117_id
    left join dep_32 on dep_1.stg_source_21_id = dep_32.stg_source_156_id
    left join dep_33 on dep_1.stg_source_21_id = dep_33.int_transformed_150_id
    left join dep_34 on dep_1.stg_source_21_id = dep_34.stg_source_153_id
    left join dep_35 on dep_1.stg_source_21_id = dep_35.stg_source_93_id
    left join dep_36 on dep_1.stg_source_21_id = dep_36.int_transformed_11_id
    left join dep_37 on dep_1.stg_source_21_id = dep_37.int_transformed_31_id
    left join dep_38 on dep_1.stg_source_21_id = dep_38.stg_source_77_id
    left join dep_39 on dep_1.stg_source_21_id = dep_39.int_transformed_149_id
    left join dep_40 on dep_1.stg_source_21_id = dep_40.int_transformed_83_id
    left join dep_41 on dep_1.stg_source_21_id = dep_41.int_transformed_78_id
    left join dep_42 on dep_1.stg_source_21_id = dep_42.int_transformed_101_id
    left join dep_43 on dep_1.stg_source_21_id = dep_43.stg_source_49_id
    left join dep_44 on dep_1.stg_source_21_id = dep_44.int_transformed_14_id
    left join dep_45 on dep_1.stg_source_21_id = dep_45.int_transformed_23_id
    left join dep_46 on dep_1.stg_source_21_id = dep_46.int_transformed_60_id
    left join dep_47 on dep_1.stg_source_21_id = dep_47.int_transformed_90_id
    left join dep_48 on dep_1.stg_source_21_id = dep_48.int_transformed_74_id
    left join dep_49 on dep_1.stg_source_21_id = dep_49.int_transformed_157_id
    left join dep_50 on dep_1.stg_source_21_id = dep_50.int_transformed_94_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
