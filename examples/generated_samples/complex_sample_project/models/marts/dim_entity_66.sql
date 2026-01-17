{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_49') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_4 as (
    select * from {{ ref('stg_source_100') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_6 as (
    select * from {{ ref('stg_source_130') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_8 as (
    select * from {{ ref('stg_source_149') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_10 as (
    select * from {{ ref('stg_source_138') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_13 as (
    select * from {{ ref('stg_source_158') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_15 as (
    select * from {{ ref('stg_source_36') }}
),

dep_16 as (
    select * from {{ ref('stg_source_70') }}
),

dep_17 as (
    select * from {{ ref('stg_source_161') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_21 as (
    select * from {{ ref('stg_source_17') }}
),

dep_22 as (
    select * from {{ ref('stg_source_10') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_25 as (
    select * from {{ ref('stg_source_81') }}
),

dep_26 as (
    select * from {{ ref('stg_source_87') }}
),

dep_27 as (
    select * from {{ ref('stg_source_34') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_29 as (
    select * from {{ ref('stg_source_13') }}
),

dep_30 as (
    select * from {{ ref('stg_source_86') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_32 as (
    select * from {{ ref('stg_source_89') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_34 as (
    select * from {{ ref('stg_source_91') }}
),

dep_35 as (
    select * from {{ ref('stg_source_18') }}
),

dep_36 as (
    select * from {{ ref('stg_source_25') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_40 as (
    select * from {{ ref('stg_source_88') }}
),

dep_41 as (
    select * from {{ ref('stg_source_31') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_45 as (
    select * from {{ ref('stg_source_32') }}
),

dep_46 as (
    select * from {{ ref('stg_source_37') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_49 as (
    select * from {{ ref('stg_source_117') }}
),

dep_50 as (
    select * from {{ ref('stg_source_116') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_49_id']) }} as surrogate_id,
        dep_1.stg_source_49_id as dim_entity_66_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_49_id = dep_2.int_transformed_16_id
    left join dep_3 on dep_1.stg_source_49_id = dep_3.int_transformed_14_id
    left join dep_4 on dep_1.stg_source_49_id = dep_4.stg_source_100_id
    left join dep_5 on dep_1.stg_source_49_id = dep_5.int_transformed_74_id
    left join dep_6 on dep_1.stg_source_49_id = dep_6.stg_source_130_id
    left join dep_7 on dep_1.stg_source_49_id = dep_7.int_transformed_119_id
    left join dep_8 on dep_1.stg_source_49_id = dep_8.stg_source_149_id
    left join dep_9 on dep_1.stg_source_49_id = dep_9.int_transformed_32_id
    left join dep_10 on dep_1.stg_source_49_id = dep_10.stg_source_138_id
    left join dep_11 on dep_1.stg_source_49_id = dep_11.int_transformed_55_id
    left join dep_12 on dep_1.stg_source_49_id = dep_12.int_transformed_75_id
    left join dep_13 on dep_1.stg_source_49_id = dep_13.stg_source_158_id
    left join dep_14 on dep_1.stg_source_49_id = dep_14.int_transformed_153_id
    left join dep_15 on dep_1.stg_source_49_id = dep_15.stg_source_36_id
    left join dep_16 on dep_1.stg_source_49_id = dep_16.stg_source_70_id
    left join dep_17 on dep_1.stg_source_49_id = dep_17.stg_source_161_id
    left join dep_18 on dep_1.stg_source_49_id = dep_18.int_transformed_96_id
    left join dep_19 on dep_1.stg_source_49_id = dep_19.int_transformed_82_id
    left join dep_20 on dep_1.stg_source_49_id = dep_20.int_transformed_5_id
    left join dep_21 on dep_1.stg_source_49_id = dep_21.stg_source_17_id
    left join dep_22 on dep_1.stg_source_49_id = dep_22.stg_source_10_id
    left join dep_23 on dep_1.stg_source_49_id = dep_23.int_transformed_151_id
    left join dep_24 on dep_1.stg_source_49_id = dep_24.int_transformed_107_id
    left join dep_25 on dep_1.stg_source_49_id = dep_25.stg_source_81_id
    left join dep_26 on dep_1.stg_source_49_id = dep_26.stg_source_87_id
    left join dep_27 on dep_1.stg_source_49_id = dep_27.stg_source_34_id
    left join dep_28 on dep_1.stg_source_49_id = dep_28.int_transformed_108_id
    left join dep_29 on dep_1.stg_source_49_id = dep_29.stg_source_13_id
    left join dep_30 on dep_1.stg_source_49_id = dep_30.stg_source_86_id
    left join dep_31 on dep_1.stg_source_49_id = dep_31.int_transformed_61_id
    left join dep_32 on dep_1.stg_source_49_id = dep_32.stg_source_89_id
    left join dep_33 on dep_1.stg_source_49_id = dep_33.int_transformed_78_id
    left join dep_34 on dep_1.stg_source_49_id = dep_34.stg_source_91_id
    left join dep_35 on dep_1.stg_source_49_id = dep_35.stg_source_18_id
    left join dep_36 on dep_1.stg_source_49_id = dep_36.stg_source_25_id
    left join dep_37 on dep_1.stg_source_49_id = dep_37.int_transformed_157_id
    left join dep_38 on dep_1.stg_source_49_id = dep_38.int_transformed_59_id
    left join dep_39 on dep_1.stg_source_49_id = dep_39.int_transformed_161_id
    left join dep_40 on dep_1.stg_source_49_id = dep_40.stg_source_88_id
    left join dep_41 on dep_1.stg_source_49_id = dep_41.stg_source_31_id
    left join dep_42 on dep_1.stg_source_49_id = dep_42.int_transformed_30_id
    left join dep_43 on dep_1.stg_source_49_id = dep_43.int_transformed_49_id
    left join dep_44 on dep_1.stg_source_49_id = dep_44.int_transformed_139_id
    left join dep_45 on dep_1.stg_source_49_id = dep_45.stg_source_32_id
    left join dep_46 on dep_1.stg_source_49_id = dep_46.stg_source_37_id
    left join dep_47 on dep_1.stg_source_49_id = dep_47.int_transformed_63_id
    left join dep_48 on dep_1.stg_source_49_id = dep_48.int_transformed_44_id
    left join dep_49 on dep_1.stg_source_49_id = dep_49.stg_source_117_id
    left join dep_50 on dep_1.stg_source_49_id = dep_50.stg_source_116_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
