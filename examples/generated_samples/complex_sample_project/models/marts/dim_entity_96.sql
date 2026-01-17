{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_86') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_3 as (
    select * from {{ ref('stg_source_58') }}
),

dep_4 as (
    select * from {{ ref('stg_source_102') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_6 as (
    select * from {{ ref('stg_source_33') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_8 as (
    select * from {{ ref('stg_source_127') }}
),

dep_9 as (
    select * from {{ ref('stg_source_134') }}
),

dep_10 as (
    select * from {{ ref('stg_source_54') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_12 as (
    select * from {{ ref('stg_source_17') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_15 as (
    select * from {{ ref('stg_source_130') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_18 as (
    select * from {{ ref('stg_source_24') }}
),

dep_19 as (
    select * from {{ ref('stg_source_125') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_24 as (
    select * from {{ ref('stg_source_30') }}
),

dep_25 as (
    select * from {{ ref('stg_source_112') }}
),

dep_26 as (
    select * from {{ ref('stg_source_48') }}
),

dep_27 as (
    select * from {{ ref('stg_source_120') }}
),

dep_28 as (
    select * from {{ ref('stg_source_70') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_35 as (
    select * from {{ ref('stg_source_119') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_37 as (
    select * from {{ ref('stg_source_61') }}
),

dep_38 as (
    select * from {{ ref('stg_source_76') }}
),

dep_39 as (
    select * from {{ ref('stg_source_14') }}
),

dep_40 as (
    select * from {{ ref('stg_source_65') }}
),

dep_41 as (
    select * from {{ ref('stg_source_66') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_44 as (
    select * from {{ ref('stg_source_63') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_46 as (
    select * from {{ ref('stg_source_81') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_48 as (
    select * from {{ ref('stg_source_118') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_146') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_86_id']) }} as surrogate_id,
        dep_1.stg_source_86_id as dim_entity_96_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_86_id = dep_2.int_transformed_150_id
    left join dep_3 on dep_1.stg_source_86_id = dep_3.stg_source_58_id
    left join dep_4 on dep_1.stg_source_86_id = dep_4.stg_source_102_id
    left join dep_5 on dep_1.stg_source_86_id = dep_5.int_transformed_118_id
    left join dep_6 on dep_1.stg_source_86_id = dep_6.stg_source_33_id
    left join dep_7 on dep_1.stg_source_86_id = dep_7.int_transformed_71_id
    left join dep_8 on dep_1.stg_source_86_id = dep_8.stg_source_127_id
    left join dep_9 on dep_1.stg_source_86_id = dep_9.stg_source_134_id
    left join dep_10 on dep_1.stg_source_86_id = dep_10.stg_source_54_id
    left join dep_11 on dep_1.stg_source_86_id = dep_11.int_transformed_69_id
    left join dep_12 on dep_1.stg_source_86_id = dep_12.stg_source_17_id
    left join dep_13 on dep_1.stg_source_86_id = dep_13.int_transformed_156_id
    left join dep_14 on dep_1.stg_source_86_id = dep_14.int_transformed_55_id
    left join dep_15 on dep_1.stg_source_86_id = dep_15.stg_source_130_id
    left join dep_16 on dep_1.stg_source_86_id = dep_16.int_transformed_9_id
    left join dep_17 on dep_1.stg_source_86_id = dep_17.int_transformed_44_id
    left join dep_18 on dep_1.stg_source_86_id = dep_18.stg_source_24_id
    left join dep_19 on dep_1.stg_source_86_id = dep_19.stg_source_125_id
    left join dep_20 on dep_1.stg_source_86_id = dep_20.int_transformed_145_id
    left join dep_21 on dep_1.stg_source_86_id = dep_21.int_transformed_101_id
    left join dep_22 on dep_1.stg_source_86_id = dep_22.int_transformed_137_id
    left join dep_23 on dep_1.stg_source_86_id = dep_23.int_transformed_20_id
    left join dep_24 on dep_1.stg_source_86_id = dep_24.stg_source_30_id
    left join dep_25 on dep_1.stg_source_86_id = dep_25.stg_source_112_id
    left join dep_26 on dep_1.stg_source_86_id = dep_26.stg_source_48_id
    left join dep_27 on dep_1.stg_source_86_id = dep_27.stg_source_120_id
    left join dep_28 on dep_1.stg_source_86_id = dep_28.stg_source_70_id
    left join dep_29 on dep_1.stg_source_86_id = dep_29.int_transformed_144_id
    left join dep_30 on dep_1.stg_source_86_id = dep_30.int_transformed_53_id
    left join dep_31 on dep_1.stg_source_86_id = dep_31.int_transformed_28_id
    left join dep_32 on dep_1.stg_source_86_id = dep_32.int_transformed_51_id
    left join dep_33 on dep_1.stg_source_86_id = dep_33.int_transformed_7_id
    left join dep_34 on dep_1.stg_source_86_id = dep_34.int_transformed_1_id
    left join dep_35 on dep_1.stg_source_86_id = dep_35.stg_source_119_id
    left join dep_36 on dep_1.stg_source_86_id = dep_36.int_transformed_54_id
    left join dep_37 on dep_1.stg_source_86_id = dep_37.stg_source_61_id
    left join dep_38 on dep_1.stg_source_86_id = dep_38.stg_source_76_id
    left join dep_39 on dep_1.stg_source_86_id = dep_39.stg_source_14_id
    left join dep_40 on dep_1.stg_source_86_id = dep_40.stg_source_65_id
    left join dep_41 on dep_1.stg_source_86_id = dep_41.stg_source_66_id
    left join dep_42 on dep_1.stg_source_86_id = dep_42.int_transformed_46_id
    left join dep_43 on dep_1.stg_source_86_id = dep_43.int_transformed_59_id
    left join dep_44 on dep_1.stg_source_86_id = dep_44.stg_source_63_id
    left join dep_45 on dep_1.stg_source_86_id = dep_45.int_transformed_61_id
    left join dep_46 on dep_1.stg_source_86_id = dep_46.stg_source_81_id
    left join dep_47 on dep_1.stg_source_86_id = dep_47.int_transformed_100_id
    left join dep_48 on dep_1.stg_source_86_id = dep_48.stg_source_118_id
    left join dep_49 on dep_1.stg_source_86_id = dep_49.int_transformed_103_id
    left join dep_50 on dep_1.stg_source_86_id = dep_50.int_transformed_146_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
