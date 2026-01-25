{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_3 as (
    select * from {{ ref('stg_source_147') }}
),

dep_4 as (
    select * from {{ ref('stg_source_83') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_7 as (
    select * from {{ ref('stg_source_90') }}
),

dep_8 as (
    select * from {{ ref('stg_source_112') }}
),

dep_9 as (
    select * from {{ ref('stg_source_93') }}
),

dep_10 as (
    select * from {{ ref('stg_source_88') }}
),

dep_11 as (
    select * from {{ ref('stg_source_76') }}
),

dep_12 as (
    select * from {{ ref('stg_source_10') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_15 as (
    select * from {{ ref('stg_source_94') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_18 as (
    select * from {{ ref('stg_source_118') }}
),

dep_19 as (
    select * from {{ ref('stg_source_105') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_21 as (
    select * from {{ ref('stg_source_30') }}
),

dep_22 as (
    select * from {{ ref('stg_source_66') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_27 as (
    select * from {{ ref('stg_source_8') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_30 as (
    select * from {{ ref('stg_source_70') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_32 as (
    select * from {{ ref('stg_source_73') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_34 as (
    select * from {{ ref('stg_source_95') }}
),

dep_35 as (
    select * from {{ ref('stg_source_36') }}
),

dep_36 as (
    select * from {{ ref('stg_source_39') }}
),

dep_37 as (
    select * from {{ ref('stg_source_23') }}
),

dep_38 as (
    select * from {{ ref('stg_source_4') }}
),

dep_39 as (
    select * from {{ ref('stg_source_3') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_41 as (
    select * from {{ ref('stg_source_2') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_43 as (
    select * from {{ ref('stg_source_120') }}
),

dep_44 as (
    select * from {{ ref('stg_source_55') }}
),

dep_45 as (
    select * from {{ ref('stg_source_61') }}
),

dep_46 as (
    select * from {{ ref('stg_source_91') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_49 as (
    select * from {{ ref('stg_source_149') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_139') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_135_id']) }} as surrogate_id,
        dep_1.int_transformed_135_id as dim_entity_18_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_135_id = dep_2.int_transformed_18_id
    left join dep_3 on dep_1.int_transformed_135_id = dep_3.stg_source_147_id
    left join dep_4 on dep_1.int_transformed_135_id = dep_4.stg_source_83_id
    left join dep_5 on dep_1.int_transformed_135_id = dep_5.int_transformed_83_id
    left join dep_6 on dep_1.int_transformed_135_id = dep_6.int_transformed_109_id
    left join dep_7 on dep_1.int_transformed_135_id = dep_7.stg_source_90_id
    left join dep_8 on dep_1.int_transformed_135_id = dep_8.stg_source_112_id
    left join dep_9 on dep_1.int_transformed_135_id = dep_9.stg_source_93_id
    left join dep_10 on dep_1.int_transformed_135_id = dep_10.stg_source_88_id
    left join dep_11 on dep_1.int_transformed_135_id = dep_11.stg_source_76_id
    left join dep_12 on dep_1.int_transformed_135_id = dep_12.stg_source_10_id
    left join dep_13 on dep_1.int_transformed_135_id = dep_13.int_transformed_151_id
    left join dep_14 on dep_1.int_transformed_135_id = dep_14.int_transformed_94_id
    left join dep_15 on dep_1.int_transformed_135_id = dep_15.stg_source_94_id
    left join dep_16 on dep_1.int_transformed_135_id = dep_16.int_transformed_104_id
    left join dep_17 on dep_1.int_transformed_135_id = dep_17.int_transformed_134_id
    left join dep_18 on dep_1.int_transformed_135_id = dep_18.stg_source_118_id
    left join dep_19 on dep_1.int_transformed_135_id = dep_19.stg_source_105_id
    left join dep_20 on dep_1.int_transformed_135_id = dep_20.int_transformed_110_id
    left join dep_21 on dep_1.int_transformed_135_id = dep_21.stg_source_30_id
    left join dep_22 on dep_1.int_transformed_135_id = dep_22.stg_source_66_id
    left join dep_23 on dep_1.int_transformed_135_id = dep_23.int_transformed_163_id
    left join dep_24 on dep_1.int_transformed_135_id = dep_24.int_transformed_17_id
    left join dep_25 on dep_1.int_transformed_135_id = dep_25.int_transformed_124_id
    left join dep_26 on dep_1.int_transformed_135_id = dep_26.int_transformed_45_id
    left join dep_27 on dep_1.int_transformed_135_id = dep_27.stg_source_8_id
    left join dep_28 on dep_1.int_transformed_135_id = dep_28.int_transformed_31_id
    left join dep_29 on dep_1.int_transformed_135_id = dep_29.int_transformed_23_id
    left join dep_30 on dep_1.int_transformed_135_id = dep_30.stg_source_70_id
    left join dep_31 on dep_1.int_transformed_135_id = dep_31.int_transformed_166_id
    left join dep_32 on dep_1.int_transformed_135_id = dep_32.stg_source_73_id
    left join dep_33 on dep_1.int_transformed_135_id = dep_33.int_transformed_158_id
    left join dep_34 on dep_1.int_transformed_135_id = dep_34.stg_source_95_id
    left join dep_35 on dep_1.int_transformed_135_id = dep_35.stg_source_36_id
    left join dep_36 on dep_1.int_transformed_135_id = dep_36.stg_source_39_id
    left join dep_37 on dep_1.int_transformed_135_id = dep_37.stg_source_23_id
    left join dep_38 on dep_1.int_transformed_135_id = dep_38.stg_source_4_id
    left join dep_39 on dep_1.int_transformed_135_id = dep_39.stg_source_3_id
    left join dep_40 on dep_1.int_transformed_135_id = dep_40.int_transformed_127_id
    left join dep_41 on dep_1.int_transformed_135_id = dep_41.stg_source_2_id
    left join dep_42 on dep_1.int_transformed_135_id = dep_42.int_transformed_97_id
    left join dep_43 on dep_1.int_transformed_135_id = dep_43.stg_source_120_id
    left join dep_44 on dep_1.int_transformed_135_id = dep_44.stg_source_55_id
    left join dep_45 on dep_1.int_transformed_135_id = dep_45.stg_source_61_id
    left join dep_46 on dep_1.int_transformed_135_id = dep_46.stg_source_91_id
    left join dep_47 on dep_1.int_transformed_135_id = dep_47.int_transformed_123_id
    left join dep_48 on dep_1.int_transformed_135_id = dep_48.int_transformed_36_id
    left join dep_49 on dep_1.int_transformed_135_id = dep_49.stg_source_149_id
    left join dep_50 on dep_1.int_transformed_135_id = dep_50.int_transformed_139_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
