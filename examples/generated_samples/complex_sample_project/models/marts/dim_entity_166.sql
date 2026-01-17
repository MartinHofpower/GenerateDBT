{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_2 as (
    select * from {{ ref('stg_source_115') }}
),

dep_3 as (
    select * from {{ ref('stg_source_63') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_7 as (
    select * from {{ ref('stg_source_83') }}
),

dep_8 as (
    select * from {{ ref('stg_source_124') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_10 as (
    select * from {{ ref('stg_source_119') }}
),

dep_11 as (
    select * from {{ ref('stg_source_166') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_13 as (
    select * from {{ ref('stg_source_153') }}
),

dep_14 as (
    select * from {{ ref('stg_source_56') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_17 as (
    select * from {{ ref('stg_source_137') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_19 as (
    select * from {{ ref('stg_source_128') }}
),

dep_20 as (
    select * from {{ ref('stg_source_98') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_25 as (
    select * from {{ ref('stg_source_66') }}
),

dep_26 as (
    select * from {{ ref('stg_source_162') }}
),

dep_27 as (
    select * from {{ ref('stg_source_120') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_30 as (
    select * from {{ ref('stg_source_19') }}
),

dep_31 as (
    select * from {{ ref('stg_source_122') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_33 as (
    select * from {{ ref('stg_source_1') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_36 as (
    select * from {{ ref('stg_source_9') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_38 as (
    select * from {{ ref('stg_source_127') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_40 as (
    select * from {{ ref('stg_source_97') }}
),

dep_41 as (
    select * from {{ ref('stg_source_145') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_45 as (
    select * from {{ ref('stg_source_51') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_47 as (
    select * from {{ ref('stg_source_123') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_93') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_27_id']) }} as surrogate_id,
        dep_1.int_transformed_27_id as dim_entity_166_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_27_id = dep_2.stg_source_115_id
    left join dep_3 on dep_1.int_transformed_27_id = dep_3.stg_source_63_id
    left join dep_4 on dep_1.int_transformed_27_id = dep_4.int_transformed_153_id
    left join dep_5 on dep_1.int_transformed_27_id = dep_5.int_transformed_103_id
    left join dep_6 on dep_1.int_transformed_27_id = dep_6.int_transformed_12_id
    left join dep_7 on dep_1.int_transformed_27_id = dep_7.stg_source_83_id
    left join dep_8 on dep_1.int_transformed_27_id = dep_8.stg_source_124_id
    left join dep_9 on dep_1.int_transformed_27_id = dep_9.int_transformed_121_id
    left join dep_10 on dep_1.int_transformed_27_id = dep_10.stg_source_119_id
    left join dep_11 on dep_1.int_transformed_27_id = dep_11.stg_source_166_id
    left join dep_12 on dep_1.int_transformed_27_id = dep_12.int_transformed_160_id
    left join dep_13 on dep_1.int_transformed_27_id = dep_13.stg_source_153_id
    left join dep_14 on dep_1.int_transformed_27_id = dep_14.stg_source_56_id
    left join dep_15 on dep_1.int_transformed_27_id = dep_15.int_transformed_19_id
    left join dep_16 on dep_1.int_transformed_27_id = dep_16.int_transformed_125_id
    left join dep_17 on dep_1.int_transformed_27_id = dep_17.stg_source_137_id
    left join dep_18 on dep_1.int_transformed_27_id = dep_18.int_transformed_164_id
    left join dep_19 on dep_1.int_transformed_27_id = dep_19.stg_source_128_id
    left join dep_20 on dep_1.int_transformed_27_id = dep_20.stg_source_98_id
    left join dep_21 on dep_1.int_transformed_27_id = dep_21.int_transformed_58_id
    left join dep_22 on dep_1.int_transformed_27_id = dep_22.int_transformed_118_id
    left join dep_23 on dep_1.int_transformed_27_id = dep_23.int_transformed_17_id
    left join dep_24 on dep_1.int_transformed_27_id = dep_24.int_transformed_114_id
    left join dep_25 on dep_1.int_transformed_27_id = dep_25.stg_source_66_id
    left join dep_26 on dep_1.int_transformed_27_id = dep_26.stg_source_162_id
    left join dep_27 on dep_1.int_transformed_27_id = dep_27.stg_source_120_id
    left join dep_28 on dep_1.int_transformed_27_id = dep_28.int_transformed_64_id
    left join dep_29 on dep_1.int_transformed_27_id = dep_29.int_transformed_157_id
    left join dep_30 on dep_1.int_transformed_27_id = dep_30.stg_source_19_id
    left join dep_31 on dep_1.int_transformed_27_id = dep_31.stg_source_122_id
    left join dep_32 on dep_1.int_transformed_27_id = dep_32.int_transformed_88_id
    left join dep_33 on dep_1.int_transformed_27_id = dep_33.stg_source_1_id
    left join dep_34 on dep_1.int_transformed_27_id = dep_34.int_transformed_54_id
    left join dep_35 on dep_1.int_transformed_27_id = dep_35.int_transformed_98_id
    left join dep_36 on dep_1.int_transformed_27_id = dep_36.stg_source_9_id
    left join dep_37 on dep_1.int_transformed_27_id = dep_37.int_transformed_135_id
    left join dep_38 on dep_1.int_transformed_27_id = dep_38.stg_source_127_id
    left join dep_39 on dep_1.int_transformed_27_id = dep_39.int_transformed_30_id
    left join dep_40 on dep_1.int_transformed_27_id = dep_40.stg_source_97_id
    left join dep_41 on dep_1.int_transformed_27_id = dep_41.stg_source_145_id
    left join dep_42 on dep_1.int_transformed_27_id = dep_42.int_transformed_89_id
    left join dep_43 on dep_1.int_transformed_27_id = dep_43.int_transformed_86_id
    left join dep_44 on dep_1.int_transformed_27_id = dep_44.int_transformed_2_id
    left join dep_45 on dep_1.int_transformed_27_id = dep_45.stg_source_51_id
    left join dep_46 on dep_1.int_transformed_27_id = dep_46.int_transformed_136_id
    left join dep_47 on dep_1.int_transformed_27_id = dep_47.stg_source_123_id
    left join dep_48 on dep_1.int_transformed_27_id = dep_48.int_transformed_70_id
    left join dep_49 on dep_1.int_transformed_27_id = dep_49.int_transformed_137_id
    left join dep_50 on dep_1.int_transformed_27_id = dep_50.int_transformed_93_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
