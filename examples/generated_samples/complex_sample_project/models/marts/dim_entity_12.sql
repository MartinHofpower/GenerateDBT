{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_2 as (
    select * from {{ ref('stg_source_70') }}
),

dep_3 as (
    select * from {{ ref('stg_source_89') }}
),

dep_4 as (
    select * from {{ ref('stg_source_136') }}
),

dep_5 as (
    select * from {{ ref('stg_source_114') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_7 as (
    select * from {{ ref('stg_source_98') }}
),

dep_8 as (
    select * from {{ ref('stg_source_11') }}
),

dep_9 as (
    select * from {{ ref('stg_source_120') }}
),

dep_10 as (
    select * from {{ ref('stg_source_4') }}
),

dep_11 as (
    select * from {{ ref('stg_source_21') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_15 as (
    select * from {{ ref('stg_source_134') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_20 as (
    select * from {{ ref('stg_source_69') }}
),

dep_21 as (
    select * from {{ ref('stg_source_61') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_23 as (
    select * from {{ ref('stg_source_66') }}
),

dep_24 as (
    select * from {{ ref('stg_source_147') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_26 as (
    select * from {{ ref('stg_source_133') }}
),

dep_27 as (
    select * from {{ ref('stg_source_101') }}
),

dep_28 as (
    select * from {{ ref('stg_source_74') }}
),

dep_29 as (
    select * from {{ ref('stg_source_73') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_31 as (
    select * from {{ ref('stg_source_137') }}
),

dep_32 as (
    select * from {{ ref('stg_source_47') }}
),

dep_33 as (
    select * from {{ ref('stg_source_128') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_39 as (
    select * from {{ ref('stg_source_24') }}
),

dep_40 as (
    select * from {{ ref('stg_source_44') }}
),

dep_41 as (
    select * from {{ ref('stg_source_30') }}
),

dep_42 as (
    select * from {{ ref('stg_source_124') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_45 as (
    select * from {{ ref('stg_source_13') }}
),

dep_46 as (
    select * from {{ ref('stg_source_64') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_48 as (
    select * from {{ ref('stg_source_87') }}
),

dep_49 as (
    select * from {{ ref('stg_source_46') }}
),

dep_50 as (
    select * from {{ ref('stg_source_159') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_76_id']) }} as surrogate_id,
        dep_1.int_transformed_76_id as dim_entity_12_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_76_id = dep_2.stg_source_70_id
    left join dep_3 on dep_1.int_transformed_76_id = dep_3.stg_source_89_id
    left join dep_4 on dep_1.int_transformed_76_id = dep_4.stg_source_136_id
    left join dep_5 on dep_1.int_transformed_76_id = dep_5.stg_source_114_id
    left join dep_6 on dep_1.int_transformed_76_id = dep_6.int_transformed_34_id
    left join dep_7 on dep_1.int_transformed_76_id = dep_7.stg_source_98_id
    left join dep_8 on dep_1.int_transformed_76_id = dep_8.stg_source_11_id
    left join dep_9 on dep_1.int_transformed_76_id = dep_9.stg_source_120_id
    left join dep_10 on dep_1.int_transformed_76_id = dep_10.stg_source_4_id
    left join dep_11 on dep_1.int_transformed_76_id = dep_11.stg_source_21_id
    left join dep_12 on dep_1.int_transformed_76_id = dep_12.int_transformed_17_id
    left join dep_13 on dep_1.int_transformed_76_id = dep_13.int_transformed_43_id
    left join dep_14 on dep_1.int_transformed_76_id = dep_14.int_transformed_72_id
    left join dep_15 on dep_1.int_transformed_76_id = dep_15.stg_source_134_id
    left join dep_16 on dep_1.int_transformed_76_id = dep_16.int_transformed_32_id
    left join dep_17 on dep_1.int_transformed_76_id = dep_17.int_transformed_35_id
    left join dep_18 on dep_1.int_transformed_76_id = dep_18.int_transformed_47_id
    left join dep_19 on dep_1.int_transformed_76_id = dep_19.int_transformed_73_id
    left join dep_20 on dep_1.int_transformed_76_id = dep_20.stg_source_69_id
    left join dep_21 on dep_1.int_transformed_76_id = dep_21.stg_source_61_id
    left join dep_22 on dep_1.int_transformed_76_id = dep_22.int_transformed_164_id
    left join dep_23 on dep_1.int_transformed_76_id = dep_23.stg_source_66_id
    left join dep_24 on dep_1.int_transformed_76_id = dep_24.stg_source_147_id
    left join dep_25 on dep_1.int_transformed_76_id = dep_25.int_transformed_132_id
    left join dep_26 on dep_1.int_transformed_76_id = dep_26.stg_source_133_id
    left join dep_27 on dep_1.int_transformed_76_id = dep_27.stg_source_101_id
    left join dep_28 on dep_1.int_transformed_76_id = dep_28.stg_source_74_id
    left join dep_29 on dep_1.int_transformed_76_id = dep_29.stg_source_73_id
    left join dep_30 on dep_1.int_transformed_76_id = dep_30.int_transformed_161_id
    left join dep_31 on dep_1.int_transformed_76_id = dep_31.stg_source_137_id
    left join dep_32 on dep_1.int_transformed_76_id = dep_32.stg_source_47_id
    left join dep_33 on dep_1.int_transformed_76_id = dep_33.stg_source_128_id
    left join dep_34 on dep_1.int_transformed_76_id = dep_34.int_transformed_61_id
    left join dep_35 on dep_1.int_transformed_76_id = dep_35.int_transformed_36_id
    left join dep_36 on dep_1.int_transformed_76_id = dep_36.int_transformed_59_id
    left join dep_37 on dep_1.int_transformed_76_id = dep_37.int_transformed_55_id
    left join dep_38 on dep_1.int_transformed_76_id = dep_38.int_transformed_45_id
    left join dep_39 on dep_1.int_transformed_76_id = dep_39.stg_source_24_id
    left join dep_40 on dep_1.int_transformed_76_id = dep_40.stg_source_44_id
    left join dep_41 on dep_1.int_transformed_76_id = dep_41.stg_source_30_id
    left join dep_42 on dep_1.int_transformed_76_id = dep_42.stg_source_124_id
    left join dep_43 on dep_1.int_transformed_76_id = dep_43.int_transformed_83_id
    left join dep_44 on dep_1.int_transformed_76_id = dep_44.int_transformed_4_id
    left join dep_45 on dep_1.int_transformed_76_id = dep_45.stg_source_13_id
    left join dep_46 on dep_1.int_transformed_76_id = dep_46.stg_source_64_id
    left join dep_47 on dep_1.int_transformed_76_id = dep_47.int_transformed_98_id
    left join dep_48 on dep_1.int_transformed_76_id = dep_48.stg_source_87_id
    left join dep_49 on dep_1.int_transformed_76_id = dep_49.stg_source_46_id
    left join dep_50 on dep_1.int_transformed_76_id = dep_50.stg_source_159_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
