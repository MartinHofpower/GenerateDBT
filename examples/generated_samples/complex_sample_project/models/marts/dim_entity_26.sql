{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_2 as (
    select * from {{ ref('stg_source_34') }}
),

dep_3 as (
    select * from {{ ref('stg_source_122') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_8 as (
    select * from {{ ref('stg_source_149') }}
),

dep_9 as (
    select * from {{ ref('stg_source_55') }}
),

dep_10 as (
    select * from {{ ref('stg_source_66') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_13 as (
    select * from {{ ref('stg_source_84') }}
),

dep_14 as (
    select * from {{ ref('stg_source_3') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_16 as (
    select * from {{ ref('stg_source_64') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_20 as (
    select * from {{ ref('stg_source_50') }}
),

dep_21 as (
    select * from {{ ref('stg_source_19') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_25 as (
    select * from {{ ref('stg_source_81') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_28 as (
    select * from {{ ref('stg_source_53') }}
),

dep_29 as (
    select * from {{ ref('stg_source_79') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_32 as (
    select * from {{ ref('stg_source_43') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_34 as (
    select * from {{ ref('stg_source_127') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_36 as (
    select * from {{ ref('stg_source_80') }}
),

dep_37 as (
    select * from {{ ref('stg_source_35') }}
),

dep_38 as (
    select * from {{ ref('stg_source_143') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_40 as (
    select * from {{ ref('stg_source_141') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_43 as (
    select * from {{ ref('stg_source_99') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_45 as (
    select * from {{ ref('stg_source_112') }}
),

dep_46 as (
    select * from {{ ref('stg_source_135') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_50 as (
    select * from {{ ref('stg_source_165') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_51_id']) }} as surrogate_id,
        dep_1.int_transformed_51_id as dim_entity_26_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_51_id = dep_2.stg_source_34_id
    left join dep_3 on dep_1.int_transformed_51_id = dep_3.stg_source_122_id
    left join dep_4 on dep_1.int_transformed_51_id = dep_4.int_transformed_153_id
    left join dep_5 on dep_1.int_transformed_51_id = dep_5.int_transformed_49_id
    left join dep_6 on dep_1.int_transformed_51_id = dep_6.int_transformed_26_id
    left join dep_7 on dep_1.int_transformed_51_id = dep_7.int_transformed_159_id
    left join dep_8 on dep_1.int_transformed_51_id = dep_8.stg_source_149_id
    left join dep_9 on dep_1.int_transformed_51_id = dep_9.stg_source_55_id
    left join dep_10 on dep_1.int_transformed_51_id = dep_10.stg_source_66_id
    left join dep_11 on dep_1.int_transformed_51_id = dep_11.int_transformed_147_id
    left join dep_12 on dep_1.int_transformed_51_id = dep_12.int_transformed_135_id
    left join dep_13 on dep_1.int_transformed_51_id = dep_13.stg_source_84_id
    left join dep_14 on dep_1.int_transformed_51_id = dep_14.stg_source_3_id
    left join dep_15 on dep_1.int_transformed_51_id = dep_15.int_transformed_134_id
    left join dep_16 on dep_1.int_transformed_51_id = dep_16.stg_source_64_id
    left join dep_17 on dep_1.int_transformed_51_id = dep_17.int_transformed_25_id
    left join dep_18 on dep_1.int_transformed_51_id = dep_18.int_transformed_162_id
    left join dep_19 on dep_1.int_transformed_51_id = dep_19.int_transformed_6_id
    left join dep_20 on dep_1.int_transformed_51_id = dep_20.stg_source_50_id
    left join dep_21 on dep_1.int_transformed_51_id = dep_21.stg_source_19_id
    left join dep_22 on dep_1.int_transformed_51_id = dep_22.int_transformed_103_id
    left join dep_23 on dep_1.int_transformed_51_id = dep_23.int_transformed_16_id
    left join dep_24 on dep_1.int_transformed_51_id = dep_24.int_transformed_38_id
    left join dep_25 on dep_1.int_transformed_51_id = dep_25.stg_source_81_id
    left join dep_26 on dep_1.int_transformed_51_id = dep_26.int_transformed_12_id
    left join dep_27 on dep_1.int_transformed_51_id = dep_27.int_transformed_128_id
    left join dep_28 on dep_1.int_transformed_51_id = dep_28.stg_source_53_id
    left join dep_29 on dep_1.int_transformed_51_id = dep_29.stg_source_79_id
    left join dep_30 on dep_1.int_transformed_51_id = dep_30.int_transformed_106_id
    left join dep_31 on dep_1.int_transformed_51_id = dep_31.int_transformed_40_id
    left join dep_32 on dep_1.int_transformed_51_id = dep_32.stg_source_43_id
    left join dep_33 on dep_1.int_transformed_51_id = dep_33.int_transformed_158_id
    left join dep_34 on dep_1.int_transformed_51_id = dep_34.stg_source_127_id
    left join dep_35 on dep_1.int_transformed_51_id = dep_35.int_transformed_102_id
    left join dep_36 on dep_1.int_transformed_51_id = dep_36.stg_source_80_id
    left join dep_37 on dep_1.int_transformed_51_id = dep_37.stg_source_35_id
    left join dep_38 on dep_1.int_transformed_51_id = dep_38.stg_source_143_id
    left join dep_39 on dep_1.int_transformed_51_id = dep_39.int_transformed_13_id
    left join dep_40 on dep_1.int_transformed_51_id = dep_40.stg_source_141_id
    left join dep_41 on dep_1.int_transformed_51_id = dep_41.int_transformed_87_id
    left join dep_42 on dep_1.int_transformed_51_id = dep_42.int_transformed_97_id
    left join dep_43 on dep_1.int_transformed_51_id = dep_43.stg_source_99_id
    left join dep_44 on dep_1.int_transformed_51_id = dep_44.int_transformed_85_id
    left join dep_45 on dep_1.int_transformed_51_id = dep_45.stg_source_112_id
    left join dep_46 on dep_1.int_transformed_51_id = dep_46.stg_source_135_id
    left join dep_47 on dep_1.int_transformed_51_id = dep_47.int_transformed_76_id
    left join dep_48 on dep_1.int_transformed_51_id = dep_48.int_transformed_157_id
    left join dep_49 on dep_1.int_transformed_51_id = dep_49.int_transformed_118_id
    left join dep_50 on dep_1.int_transformed_51_id = dep_50.stg_source_165_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
