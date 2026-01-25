{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_2 as (
    select * from {{ ref('stg_source_17') }}
),

dep_3 as (
    select * from {{ ref('stg_source_125') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_5 as (
    select * from {{ ref('stg_source_117') }}
),

dep_6 as (
    select * from {{ ref('stg_source_26') }}
),

dep_7 as (
    select * from {{ ref('stg_source_108') }}
),

dep_8 as (
    select * from {{ ref('stg_source_71') }}
),

dep_9 as (
    select * from {{ ref('stg_source_25') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_13 as (
    select * from {{ ref('stg_source_95') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_15 as (
    select * from {{ ref('stg_source_139') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_17 as (
    select * from {{ ref('stg_source_132') }}
),

dep_18 as (
    select * from {{ ref('stg_source_9') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_20 as (
    select * from {{ ref('stg_source_129') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_22 as (
    select * from {{ ref('stg_source_7') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_25 as (
    select * from {{ ref('stg_source_38') }}
),

dep_26 as (
    select * from {{ ref('stg_source_36') }}
),

dep_27 as (
    select * from {{ ref('stg_source_23') }}
),

dep_28 as (
    select * from {{ ref('stg_source_135') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_32 as (
    select * from {{ ref('stg_source_141') }}
),

dep_33 as (
    select * from {{ ref('stg_source_124') }}
),

dep_34 as (
    select * from {{ ref('stg_source_50') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_36 as (
    select * from {{ ref('stg_source_76') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_41 as (
    select * from {{ ref('stg_source_28') }}
),

dep_42 as (
    select * from {{ ref('stg_source_83') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_44 as (
    select * from {{ ref('stg_source_15') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_51') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_163_id']) }} as surrogate_id,
        dep_1.int_transformed_163_id as dim_entity_160_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_163_id = dep_2.stg_source_17_id
    left join dep_3 on dep_1.int_transformed_163_id = dep_3.stg_source_125_id
    left join dep_4 on dep_1.int_transformed_163_id = dep_4.int_transformed_9_id
    left join dep_5 on dep_1.int_transformed_163_id = dep_5.stg_source_117_id
    left join dep_6 on dep_1.int_transformed_163_id = dep_6.stg_source_26_id
    left join dep_7 on dep_1.int_transformed_163_id = dep_7.stg_source_108_id
    left join dep_8 on dep_1.int_transformed_163_id = dep_8.stg_source_71_id
    left join dep_9 on dep_1.int_transformed_163_id = dep_9.stg_source_25_id
    left join dep_10 on dep_1.int_transformed_163_id = dep_10.int_transformed_47_id
    left join dep_11 on dep_1.int_transformed_163_id = dep_11.int_transformed_134_id
    left join dep_12 on dep_1.int_transformed_163_id = dep_12.int_transformed_146_id
    left join dep_13 on dep_1.int_transformed_163_id = dep_13.stg_source_95_id
    left join dep_14 on dep_1.int_transformed_163_id = dep_14.int_transformed_76_id
    left join dep_15 on dep_1.int_transformed_163_id = dep_15.stg_source_139_id
    left join dep_16 on dep_1.int_transformed_163_id = dep_16.int_transformed_80_id
    left join dep_17 on dep_1.int_transformed_163_id = dep_17.stg_source_132_id
    left join dep_18 on dep_1.int_transformed_163_id = dep_18.stg_source_9_id
    left join dep_19 on dep_1.int_transformed_163_id = dep_19.int_transformed_58_id
    left join dep_20 on dep_1.int_transformed_163_id = dep_20.stg_source_129_id
    left join dep_21 on dep_1.int_transformed_163_id = dep_21.int_transformed_156_id
    left join dep_22 on dep_1.int_transformed_163_id = dep_22.stg_source_7_id
    left join dep_23 on dep_1.int_transformed_163_id = dep_23.int_transformed_62_id
    left join dep_24 on dep_1.int_transformed_163_id = dep_24.int_transformed_89_id
    left join dep_25 on dep_1.int_transformed_163_id = dep_25.stg_source_38_id
    left join dep_26 on dep_1.int_transformed_163_id = dep_26.stg_source_36_id
    left join dep_27 on dep_1.int_transformed_163_id = dep_27.stg_source_23_id
    left join dep_28 on dep_1.int_transformed_163_id = dep_28.stg_source_135_id
    left join dep_29 on dep_1.int_transformed_163_id = dep_29.int_transformed_140_id
    left join dep_30 on dep_1.int_transformed_163_id = dep_30.int_transformed_73_id
    left join dep_31 on dep_1.int_transformed_163_id = dep_31.int_transformed_60_id
    left join dep_32 on dep_1.int_transformed_163_id = dep_32.stg_source_141_id
    left join dep_33 on dep_1.int_transformed_163_id = dep_33.stg_source_124_id
    left join dep_34 on dep_1.int_transformed_163_id = dep_34.stg_source_50_id
    left join dep_35 on dep_1.int_transformed_163_id = dep_35.int_transformed_35_id
    left join dep_36 on dep_1.int_transformed_163_id = dep_36.stg_source_76_id
    left join dep_37 on dep_1.int_transformed_163_id = dep_37.int_transformed_7_id
    left join dep_38 on dep_1.int_transformed_163_id = dep_38.int_transformed_158_id
    left join dep_39 on dep_1.int_transformed_163_id = dep_39.int_transformed_125_id
    left join dep_40 on dep_1.int_transformed_163_id = dep_40.int_transformed_25_id
    left join dep_41 on dep_1.int_transformed_163_id = dep_41.stg_source_28_id
    left join dep_42 on dep_1.int_transformed_163_id = dep_42.stg_source_83_id
    left join dep_43 on dep_1.int_transformed_163_id = dep_43.int_transformed_162_id
    left join dep_44 on dep_1.int_transformed_163_id = dep_44.stg_source_15_id
    left join dep_45 on dep_1.int_transformed_163_id = dep_45.int_transformed_129_id
    left join dep_46 on dep_1.int_transformed_163_id = dep_46.int_transformed_43_id
    left join dep_47 on dep_1.int_transformed_163_id = dep_47.int_transformed_8_id
    left join dep_48 on dep_1.int_transformed_163_id = dep_48.int_transformed_17_id
    left join dep_49 on dep_1.int_transformed_163_id = dep_49.int_transformed_49_id
    left join dep_50 on dep_1.int_transformed_163_id = dep_50.int_transformed_51_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
