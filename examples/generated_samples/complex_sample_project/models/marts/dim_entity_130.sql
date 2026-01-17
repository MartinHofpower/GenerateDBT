{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_13') }}
),

dep_2 as (
    select * from {{ ref('stg_source_97') }}
),

dep_3 as (
    select * from {{ ref('stg_source_96') }}
),

dep_4 as (
    select * from {{ ref('stg_source_156') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_6 as (
    select * from {{ ref('stg_source_152') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_9 as (
    select * from {{ ref('stg_source_29') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_12 as (
    select * from {{ ref('stg_source_95') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_14 as (
    select * from {{ ref('stg_source_101') }}
),

dep_15 as (
    select * from {{ ref('stg_source_135') }}
),

dep_16 as (
    select * from {{ ref('stg_source_17') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_18 as (
    select * from {{ ref('stg_source_124') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_23 as (
    select * from {{ ref('stg_source_3') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_25 as (
    select * from {{ ref('stg_source_155') }}
),

dep_26 as (
    select * from {{ ref('stg_source_25') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_29 as (
    select * from {{ ref('stg_source_26') }}
),

dep_30 as (
    select * from {{ ref('stg_source_105') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_32 as (
    select * from {{ ref('stg_source_49') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_37 as (
    select * from {{ ref('stg_source_72') }}
),

dep_38 as (
    select * from {{ ref('stg_source_67') }}
),

dep_39 as (
    select * from {{ ref('stg_source_24') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_41 as (
    select * from {{ ref('stg_source_15') }}
),

dep_42 as (
    select * from {{ ref('stg_source_134') }}
),

dep_43 as (
    select * from {{ ref('stg_source_47') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_46 as (
    select * from {{ ref('stg_source_66') }}
),

dep_47 as (
    select * from {{ ref('stg_source_6') }}
),

dep_48 as (
    select * from {{ ref('stg_source_50') }}
),

dep_49 as (
    select * from {{ ref('stg_source_128') }}
),

dep_50 as (
    select * from {{ ref('stg_source_125') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_13_id']) }} as surrogate_id,
        dep_1.stg_source_13_id as dim_entity_130_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_13_id = dep_2.stg_source_97_id
    left join dep_3 on dep_1.stg_source_13_id = dep_3.stg_source_96_id
    left join dep_4 on dep_1.stg_source_13_id = dep_4.stg_source_156_id
    left join dep_5 on dep_1.stg_source_13_id = dep_5.int_transformed_50_id
    left join dep_6 on dep_1.stg_source_13_id = dep_6.stg_source_152_id
    left join dep_7 on dep_1.stg_source_13_id = dep_7.int_transformed_150_id
    left join dep_8 on dep_1.stg_source_13_id = dep_8.int_transformed_8_id
    left join dep_9 on dep_1.stg_source_13_id = dep_9.stg_source_29_id
    left join dep_10 on dep_1.stg_source_13_id = dep_10.int_transformed_68_id
    left join dep_11 on dep_1.stg_source_13_id = dep_11.int_transformed_40_id
    left join dep_12 on dep_1.stg_source_13_id = dep_12.stg_source_95_id
    left join dep_13 on dep_1.stg_source_13_id = dep_13.int_transformed_143_id
    left join dep_14 on dep_1.stg_source_13_id = dep_14.stg_source_101_id
    left join dep_15 on dep_1.stg_source_13_id = dep_15.stg_source_135_id
    left join dep_16 on dep_1.stg_source_13_id = dep_16.stg_source_17_id
    left join dep_17 on dep_1.stg_source_13_id = dep_17.int_transformed_60_id
    left join dep_18 on dep_1.stg_source_13_id = dep_18.stg_source_124_id
    left join dep_19 on dep_1.stg_source_13_id = dep_19.int_transformed_148_id
    left join dep_20 on dep_1.stg_source_13_id = dep_20.int_transformed_135_id
    left join dep_21 on dep_1.stg_source_13_id = dep_21.int_transformed_101_id
    left join dep_22 on dep_1.stg_source_13_id = dep_22.int_transformed_83_id
    left join dep_23 on dep_1.stg_source_13_id = dep_23.stg_source_3_id
    left join dep_24 on dep_1.stg_source_13_id = dep_24.int_transformed_16_id
    left join dep_25 on dep_1.stg_source_13_id = dep_25.stg_source_155_id
    left join dep_26 on dep_1.stg_source_13_id = dep_26.stg_source_25_id
    left join dep_27 on dep_1.stg_source_13_id = dep_27.int_transformed_163_id
    left join dep_28 on dep_1.stg_source_13_id = dep_28.int_transformed_31_id
    left join dep_29 on dep_1.stg_source_13_id = dep_29.stg_source_26_id
    left join dep_30 on dep_1.stg_source_13_id = dep_30.stg_source_105_id
    left join dep_31 on dep_1.stg_source_13_id = dep_31.int_transformed_133_id
    left join dep_32 on dep_1.stg_source_13_id = dep_32.stg_source_49_id
    left join dep_33 on dep_1.stg_source_13_id = dep_33.int_transformed_166_id
    left join dep_34 on dep_1.stg_source_13_id = dep_34.int_transformed_107_id
    left join dep_35 on dep_1.stg_source_13_id = dep_35.int_transformed_86_id
    left join dep_36 on dep_1.stg_source_13_id = dep_36.int_transformed_80_id
    left join dep_37 on dep_1.stg_source_13_id = dep_37.stg_source_72_id
    left join dep_38 on dep_1.stg_source_13_id = dep_38.stg_source_67_id
    left join dep_39 on dep_1.stg_source_13_id = dep_39.stg_source_24_id
    left join dep_40 on dep_1.stg_source_13_id = dep_40.int_transformed_96_id
    left join dep_41 on dep_1.stg_source_13_id = dep_41.stg_source_15_id
    left join dep_42 on dep_1.stg_source_13_id = dep_42.stg_source_134_id
    left join dep_43 on dep_1.stg_source_13_id = dep_43.stg_source_47_id
    left join dep_44 on dep_1.stg_source_13_id = dep_44.int_transformed_127_id
    left join dep_45 on dep_1.stg_source_13_id = dep_45.int_transformed_39_id
    left join dep_46 on dep_1.stg_source_13_id = dep_46.stg_source_66_id
    left join dep_47 on dep_1.stg_source_13_id = dep_47.stg_source_6_id
    left join dep_48 on dep_1.stg_source_13_id = dep_48.stg_source_50_id
    left join dep_49 on dep_1.stg_source_13_id = dep_49.stg_source_128_id
    left join dep_50 on dep_1.stg_source_13_id = dep_50.stg_source_125_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
