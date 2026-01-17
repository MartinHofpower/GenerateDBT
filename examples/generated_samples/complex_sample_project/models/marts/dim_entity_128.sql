{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_4 as (
    select * from {{ ref('stg_source_153') }}
),

dep_5 as (
    select * from {{ ref('stg_source_12') }}
),

dep_6 as (
    select * from {{ ref('stg_source_135') }}
),

dep_7 as (
    select * from {{ ref('stg_source_53') }}
),

dep_8 as (
    select * from {{ ref('stg_source_133') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_12 as (
    select * from {{ ref('stg_source_46') }}
),

dep_13 as (
    select * from {{ ref('stg_source_25') }}
),

dep_14 as (
    select * from {{ ref('stg_source_1') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_18 as (
    select * from {{ ref('stg_source_130') }}
),

dep_19 as (
    select * from {{ ref('stg_source_164') }}
),

dep_20 as (
    select * from {{ ref('stg_source_24') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_22 as (
    select * from {{ ref('stg_source_6') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_25 as (
    select * from {{ ref('stg_source_113') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_27 as (
    select * from {{ ref('stg_source_23') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_29 as (
    select * from {{ ref('stg_source_18') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_31 as (
    select * from {{ ref('stg_source_74') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_33 as (
    select * from {{ ref('stg_source_159') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_37 as (
    select * from {{ ref('stg_source_99') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_40 as (
    select * from {{ ref('stg_source_71') }}
),

dep_41 as (
    select * from {{ ref('stg_source_44') }}
),

dep_42 as (
    select * from {{ ref('stg_source_117') }}
),

dep_43 as (
    select * from {{ ref('stg_source_47') }}
),

dep_44 as (
    select * from {{ ref('stg_source_73') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_46 as (
    select * from {{ ref('stg_source_88') }}
),

dep_47 as (
    select * from {{ ref('stg_source_84') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_49 as (
    select * from {{ ref('stg_source_166') }}
),

dep_50 as (
    select * from {{ ref('stg_source_128') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_33_id']) }} as surrogate_id,
        dep_1.int_transformed_33_id as dim_entity_128_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_33_id = dep_2.int_transformed_112_id
    left join dep_3 on dep_1.int_transformed_33_id = dep_3.int_transformed_162_id
    left join dep_4 on dep_1.int_transformed_33_id = dep_4.stg_source_153_id
    left join dep_5 on dep_1.int_transformed_33_id = dep_5.stg_source_12_id
    left join dep_6 on dep_1.int_transformed_33_id = dep_6.stg_source_135_id
    left join dep_7 on dep_1.int_transformed_33_id = dep_7.stg_source_53_id
    left join dep_8 on dep_1.int_transformed_33_id = dep_8.stg_source_133_id
    left join dep_9 on dep_1.int_transformed_33_id = dep_9.int_transformed_81_id
    left join dep_10 on dep_1.int_transformed_33_id = dep_10.int_transformed_93_id
    left join dep_11 on dep_1.int_transformed_33_id = dep_11.int_transformed_101_id
    left join dep_12 on dep_1.int_transformed_33_id = dep_12.stg_source_46_id
    left join dep_13 on dep_1.int_transformed_33_id = dep_13.stg_source_25_id
    left join dep_14 on dep_1.int_transformed_33_id = dep_14.stg_source_1_id
    left join dep_15 on dep_1.int_transformed_33_id = dep_15.int_transformed_63_id
    left join dep_16 on dep_1.int_transformed_33_id = dep_16.int_transformed_83_id
    left join dep_17 on dep_1.int_transformed_33_id = dep_17.int_transformed_110_id
    left join dep_18 on dep_1.int_transformed_33_id = dep_18.stg_source_130_id
    left join dep_19 on dep_1.int_transformed_33_id = dep_19.stg_source_164_id
    left join dep_20 on dep_1.int_transformed_33_id = dep_20.stg_source_24_id
    left join dep_21 on dep_1.int_transformed_33_id = dep_21.int_transformed_127_id
    left join dep_22 on dep_1.int_transformed_33_id = dep_22.stg_source_6_id
    left join dep_23 on dep_1.int_transformed_33_id = dep_23.int_transformed_157_id
    left join dep_24 on dep_1.int_transformed_33_id = dep_24.int_transformed_4_id
    left join dep_25 on dep_1.int_transformed_33_id = dep_25.stg_source_113_id
    left join dep_26 on dep_1.int_transformed_33_id = dep_26.int_transformed_148_id
    left join dep_27 on dep_1.int_transformed_33_id = dep_27.stg_source_23_id
    left join dep_28 on dep_1.int_transformed_33_id = dep_28.int_transformed_95_id
    left join dep_29 on dep_1.int_transformed_33_id = dep_29.stg_source_18_id
    left join dep_30 on dep_1.int_transformed_33_id = dep_30.int_transformed_121_id
    left join dep_31 on dep_1.int_transformed_33_id = dep_31.stg_source_74_id
    left join dep_32 on dep_1.int_transformed_33_id = dep_32.int_transformed_29_id
    left join dep_33 on dep_1.int_transformed_33_id = dep_33.stg_source_159_id
    left join dep_34 on dep_1.int_transformed_33_id = dep_34.int_transformed_105_id
    left join dep_35 on dep_1.int_transformed_33_id = dep_35.int_transformed_1_id
    left join dep_36 on dep_1.int_transformed_33_id = dep_36.int_transformed_73_id
    left join dep_37 on dep_1.int_transformed_33_id = dep_37.stg_source_99_id
    left join dep_38 on dep_1.int_transformed_33_id = dep_38.int_transformed_43_id
    left join dep_39 on dep_1.int_transformed_33_id = dep_39.int_transformed_6_id
    left join dep_40 on dep_1.int_transformed_33_id = dep_40.stg_source_71_id
    left join dep_41 on dep_1.int_transformed_33_id = dep_41.stg_source_44_id
    left join dep_42 on dep_1.int_transformed_33_id = dep_42.stg_source_117_id
    left join dep_43 on dep_1.int_transformed_33_id = dep_43.stg_source_47_id
    left join dep_44 on dep_1.int_transformed_33_id = dep_44.stg_source_73_id
    left join dep_45 on dep_1.int_transformed_33_id = dep_45.int_transformed_10_id
    left join dep_46 on dep_1.int_transformed_33_id = dep_46.stg_source_88_id
    left join dep_47 on dep_1.int_transformed_33_id = dep_47.stg_source_84_id
    left join dep_48 on dep_1.int_transformed_33_id = dep_48.int_transformed_141_id
    left join dep_49 on dep_1.int_transformed_33_id = dep_49.stg_source_166_id
    left join dep_50 on dep_1.int_transformed_33_id = dep_50.stg_source_128_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
