{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_2 as (
    select * from {{ ref('stg_source_71') }}
),

dep_3 as (
    select * from {{ ref('stg_source_155') }}
),

dep_4 as (
    select * from {{ ref('stg_source_116') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_7 as (
    select * from {{ ref('stg_source_107') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_9 as (
    select * from {{ ref('stg_source_74') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_14 as (
    select * from {{ ref('stg_source_72') }}
),

dep_15 as (
    select * from {{ ref('stg_source_42') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_17 as (
    select * from {{ ref('stg_source_126') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_20 as (
    select * from {{ ref('stg_source_13') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_23 as (
    select * from {{ ref('stg_source_112') }}
),

dep_24 as (
    select * from {{ ref('stg_source_10') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_26 as (
    select * from {{ ref('stg_source_69') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_29 as (
    select * from {{ ref('stg_source_106') }}
),

dep_30 as (
    select * from {{ ref('stg_source_2') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_33 as (
    select * from {{ ref('stg_source_119') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_35 as (
    select * from {{ ref('stg_source_26') }}
),

dep_36 as (
    select * from {{ ref('stg_source_111') }}
),

dep_37 as (
    select * from {{ ref('stg_source_108') }}
),

dep_38 as (
    select * from {{ ref('stg_source_156') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_42 as (
    select * from {{ ref('stg_source_100') }}
),

dep_43 as (
    select * from {{ ref('stg_source_12') }}
),

dep_44 as (
    select * from {{ ref('stg_source_89') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_46 as (
    select * from {{ ref('stg_source_158') }}
),

dep_47 as (
    select * from {{ ref('stg_source_75') }}
),

dep_48 as (
    select * from {{ ref('stg_source_55') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_49') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_126_id']) }} as surrogate_id,
        dep_1.int_transformed_126_id as dim_entity_20_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_126_id = dep_2.stg_source_71_id
    left join dep_3 on dep_1.int_transformed_126_id = dep_3.stg_source_155_id
    left join dep_4 on dep_1.int_transformed_126_id = dep_4.stg_source_116_id
    left join dep_5 on dep_1.int_transformed_126_id = dep_5.int_transformed_113_id
    left join dep_6 on dep_1.int_transformed_126_id = dep_6.int_transformed_158_id
    left join dep_7 on dep_1.int_transformed_126_id = dep_7.stg_source_107_id
    left join dep_8 on dep_1.int_transformed_126_id = dep_8.int_transformed_152_id
    left join dep_9 on dep_1.int_transformed_126_id = dep_9.stg_source_74_id
    left join dep_10 on dep_1.int_transformed_126_id = dep_10.int_transformed_146_id
    left join dep_11 on dep_1.int_transformed_126_id = dep_11.int_transformed_59_id
    left join dep_12 on dep_1.int_transformed_126_id = dep_12.int_transformed_65_id
    left join dep_13 on dep_1.int_transformed_126_id = dep_13.int_transformed_88_id
    left join dep_14 on dep_1.int_transformed_126_id = dep_14.stg_source_72_id
    left join dep_15 on dep_1.int_transformed_126_id = dep_15.stg_source_42_id
    left join dep_16 on dep_1.int_transformed_126_id = dep_16.int_transformed_36_id
    left join dep_17 on dep_1.int_transformed_126_id = dep_17.stg_source_126_id
    left join dep_18 on dep_1.int_transformed_126_id = dep_18.int_transformed_13_id
    left join dep_19 on dep_1.int_transformed_126_id = dep_19.int_transformed_67_id
    left join dep_20 on dep_1.int_transformed_126_id = dep_20.stg_source_13_id
    left join dep_21 on dep_1.int_transformed_126_id = dep_21.int_transformed_41_id
    left join dep_22 on dep_1.int_transformed_126_id = dep_22.int_transformed_131_id
    left join dep_23 on dep_1.int_transformed_126_id = dep_23.stg_source_112_id
    left join dep_24 on dep_1.int_transformed_126_id = dep_24.stg_source_10_id
    left join dep_25 on dep_1.int_transformed_126_id = dep_25.int_transformed_89_id
    left join dep_26 on dep_1.int_transformed_126_id = dep_26.stg_source_69_id
    left join dep_27 on dep_1.int_transformed_126_id = dep_27.int_transformed_6_id
    left join dep_28 on dep_1.int_transformed_126_id = dep_28.int_transformed_98_id
    left join dep_29 on dep_1.int_transformed_126_id = dep_29.stg_source_106_id
    left join dep_30 on dep_1.int_transformed_126_id = dep_30.stg_source_2_id
    left join dep_31 on dep_1.int_transformed_126_id = dep_31.int_transformed_162_id
    left join dep_32 on dep_1.int_transformed_126_id = dep_32.int_transformed_8_id
    left join dep_33 on dep_1.int_transformed_126_id = dep_33.stg_source_119_id
    left join dep_34 on dep_1.int_transformed_126_id = dep_34.int_transformed_32_id
    left join dep_35 on dep_1.int_transformed_126_id = dep_35.stg_source_26_id
    left join dep_36 on dep_1.int_transformed_126_id = dep_36.stg_source_111_id
    left join dep_37 on dep_1.int_transformed_126_id = dep_37.stg_source_108_id
    left join dep_38 on dep_1.int_transformed_126_id = dep_38.stg_source_156_id
    left join dep_39 on dep_1.int_transformed_126_id = dep_39.int_transformed_12_id
    left join dep_40 on dep_1.int_transformed_126_id = dep_40.int_transformed_85_id
    left join dep_41 on dep_1.int_transformed_126_id = dep_41.int_transformed_114_id
    left join dep_42 on dep_1.int_transformed_126_id = dep_42.stg_source_100_id
    left join dep_43 on dep_1.int_transformed_126_id = dep_43.stg_source_12_id
    left join dep_44 on dep_1.int_transformed_126_id = dep_44.stg_source_89_id
    left join dep_45 on dep_1.int_transformed_126_id = dep_45.int_transformed_120_id
    left join dep_46 on dep_1.int_transformed_126_id = dep_46.stg_source_158_id
    left join dep_47 on dep_1.int_transformed_126_id = dep_47.stg_source_75_id
    left join dep_48 on dep_1.int_transformed_126_id = dep_48.stg_source_55_id
    left join dep_49 on dep_1.int_transformed_126_id = dep_49.int_transformed_78_id
    left join dep_50 on dep_1.int_transformed_126_id = dep_50.int_transformed_49_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
