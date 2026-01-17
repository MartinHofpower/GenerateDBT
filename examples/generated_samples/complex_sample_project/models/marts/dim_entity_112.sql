{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_53') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_165') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_8 as (
    select * from {{ ref('stg_source_10') }}
),

dep_9 as (
    select * from {{ ref('stg_source_127') }}
),

dep_10 as (
    select * from {{ ref('stg_source_32') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_12 as (
    select * from {{ ref('stg_source_33') }}
),

dep_13 as (
    select * from {{ ref('stg_source_83') }}
),

dep_14 as (
    select * from {{ ref('stg_source_84') }}
),

dep_15 as (
    select * from {{ ref('stg_source_165') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_17 as (
    select * from {{ ref('stg_source_103') }}
),

dep_18 as (
    select * from {{ ref('stg_source_121') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_21 as (
    select * from {{ ref('stg_source_39') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_23 as (
    select * from {{ ref('stg_source_153') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_25 as (
    select * from {{ ref('stg_source_145') }}
),

dep_26 as (
    select * from {{ ref('stg_source_154') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_29 as (
    select * from {{ ref('stg_source_112') }}
),

dep_30 as (
    select * from {{ ref('stg_source_48') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_37 as (
    select * from {{ ref('stg_source_98') }}
),

dep_38 as (
    select * from {{ ref('stg_source_150') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_41 as (
    select * from {{ ref('stg_source_64') }}
),

dep_42 as (
    select * from {{ ref('stg_source_17') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_46 as (
    select * from {{ ref('stg_source_102') }}
),

dep_47 as (
    select * from {{ ref('stg_source_62') }}
),

dep_48 as (
    select * from {{ ref('stg_source_159') }}
),

dep_49 as (
    select * from {{ ref('stg_source_42') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_137') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_53_id']) }} as surrogate_id,
        dep_1.stg_source_53_id as dim_entity_112_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_53_id = dep_2.int_transformed_119_id
    left join dep_3 on dep_1.stg_source_53_id = dep_3.int_transformed_138_id
    left join dep_4 on dep_1.stg_source_53_id = dep_4.int_transformed_165_id
    left join dep_5 on dep_1.stg_source_53_id = dep_5.int_transformed_62_id
    left join dep_6 on dep_1.stg_source_53_id = dep_6.int_transformed_11_id
    left join dep_7 on dep_1.stg_source_53_id = dep_7.int_transformed_9_id
    left join dep_8 on dep_1.stg_source_53_id = dep_8.stg_source_10_id
    left join dep_9 on dep_1.stg_source_53_id = dep_9.stg_source_127_id
    left join dep_10 on dep_1.stg_source_53_id = dep_10.stg_source_32_id
    left join dep_11 on dep_1.stg_source_53_id = dep_11.int_transformed_6_id
    left join dep_12 on dep_1.stg_source_53_id = dep_12.stg_source_33_id
    left join dep_13 on dep_1.stg_source_53_id = dep_13.stg_source_83_id
    left join dep_14 on dep_1.stg_source_53_id = dep_14.stg_source_84_id
    left join dep_15 on dep_1.stg_source_53_id = dep_15.stg_source_165_id
    left join dep_16 on dep_1.stg_source_53_id = dep_16.int_transformed_120_id
    left join dep_17 on dep_1.stg_source_53_id = dep_17.stg_source_103_id
    left join dep_18 on dep_1.stg_source_53_id = dep_18.stg_source_121_id
    left join dep_19 on dep_1.stg_source_53_id = dep_19.int_transformed_132_id
    left join dep_20 on dep_1.stg_source_53_id = dep_20.int_transformed_2_id
    left join dep_21 on dep_1.stg_source_53_id = dep_21.stg_source_39_id
    left join dep_22 on dep_1.stg_source_53_id = dep_22.int_transformed_74_id
    left join dep_23 on dep_1.stg_source_53_id = dep_23.stg_source_153_id
    left join dep_24 on dep_1.stg_source_53_id = dep_24.int_transformed_133_id
    left join dep_25 on dep_1.stg_source_53_id = dep_25.stg_source_145_id
    left join dep_26 on dep_1.stg_source_53_id = dep_26.stg_source_154_id
    left join dep_27 on dep_1.stg_source_53_id = dep_27.int_transformed_51_id
    left join dep_28 on dep_1.stg_source_53_id = dep_28.int_transformed_1_id
    left join dep_29 on dep_1.stg_source_53_id = dep_29.stg_source_112_id
    left join dep_30 on dep_1.stg_source_53_id = dep_30.stg_source_48_id
    left join dep_31 on dep_1.stg_source_53_id = dep_31.int_transformed_118_id
    left join dep_32 on dep_1.stg_source_53_id = dep_32.int_transformed_70_id
    left join dep_33 on dep_1.stg_source_53_id = dep_33.int_transformed_153_id
    left join dep_34 on dep_1.stg_source_53_id = dep_34.int_transformed_83_id
    left join dep_35 on dep_1.stg_source_53_id = dep_35.int_transformed_154_id
    left join dep_36 on dep_1.stg_source_53_id = dep_36.int_transformed_98_id
    left join dep_37 on dep_1.stg_source_53_id = dep_37.stg_source_98_id
    left join dep_38 on dep_1.stg_source_53_id = dep_38.stg_source_150_id
    left join dep_39 on dep_1.stg_source_53_id = dep_39.int_transformed_85_id
    left join dep_40 on dep_1.stg_source_53_id = dep_40.int_transformed_93_id
    left join dep_41 on dep_1.stg_source_53_id = dep_41.stg_source_64_id
    left join dep_42 on dep_1.stg_source_53_id = dep_42.stg_source_17_id
    left join dep_43 on dep_1.stg_source_53_id = dep_43.int_transformed_125_id
    left join dep_44 on dep_1.stg_source_53_id = dep_44.int_transformed_158_id
    left join dep_45 on dep_1.stg_source_53_id = dep_45.int_transformed_17_id
    left join dep_46 on dep_1.stg_source_53_id = dep_46.stg_source_102_id
    left join dep_47 on dep_1.stg_source_53_id = dep_47.stg_source_62_id
    left join dep_48 on dep_1.stg_source_53_id = dep_48.stg_source_159_id
    left join dep_49 on dep_1.stg_source_53_id = dep_49.stg_source_42_id
    left join dep_50 on dep_1.stg_source_53_id = dep_50.int_transformed_137_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
