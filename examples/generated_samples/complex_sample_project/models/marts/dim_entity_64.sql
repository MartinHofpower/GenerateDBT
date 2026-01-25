{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_157') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_4 as (
    select * from {{ ref('stg_source_116') }}
),

dep_5 as (
    select * from {{ ref('stg_source_79') }}
),

dep_6 as (
    select * from {{ ref('stg_source_48') }}
),

dep_7 as (
    select * from {{ ref('stg_source_154') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_9 as (
    select * from {{ ref('stg_source_14') }}
),

dep_10 as (
    select * from {{ ref('stg_source_166') }}
),

dep_11 as (
    select * from {{ ref('stg_source_141') }}
),

dep_12 as (
    select * from {{ ref('stg_source_95') }}
),

dep_13 as (
    select * from {{ ref('stg_source_39') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_15 as (
    select * from {{ ref('stg_source_139') }}
),

dep_16 as (
    select * from {{ ref('stg_source_52') }}
),

dep_17 as (
    select * from {{ ref('stg_source_80') }}
),

dep_18 as (
    select * from {{ ref('stg_source_153') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_165') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_24 as (
    select * from {{ ref('stg_source_21') }}
),

dep_25 as (
    select * from {{ ref('stg_source_110') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_27 as (
    select * from {{ ref('stg_source_34') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_29 as (
    select * from {{ ref('stg_source_32') }}
),

dep_30 as (
    select * from {{ ref('stg_source_114') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_32 as (
    select * from {{ ref('stg_source_29') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_34 as (
    select * from {{ ref('stg_source_126') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_37 as (
    select * from {{ ref('stg_source_6') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_40 as (
    select * from {{ ref('stg_source_84') }}
),

dep_41 as (
    select * from {{ ref('stg_source_86') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_43 as (
    select * from {{ ref('stg_source_56') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_46 as (
    select * from {{ ref('stg_source_127') }}
),

dep_47 as (
    select * from {{ ref('stg_source_36') }}
),

dep_48 as (
    select * from {{ ref('stg_source_26') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_4') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_157_id']) }} as surrogate_id,
        dep_1.stg_source_157_id as dim_entity_64_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_157_id = dep_2.int_transformed_31_id
    left join dep_3 on dep_1.stg_source_157_id = dep_3.int_transformed_146_id
    left join dep_4 on dep_1.stg_source_157_id = dep_4.stg_source_116_id
    left join dep_5 on dep_1.stg_source_157_id = dep_5.stg_source_79_id
    left join dep_6 on dep_1.stg_source_157_id = dep_6.stg_source_48_id
    left join dep_7 on dep_1.stg_source_157_id = dep_7.stg_source_154_id
    left join dep_8 on dep_1.stg_source_157_id = dep_8.int_transformed_56_id
    left join dep_9 on dep_1.stg_source_157_id = dep_9.stg_source_14_id
    left join dep_10 on dep_1.stg_source_157_id = dep_10.stg_source_166_id
    left join dep_11 on dep_1.stg_source_157_id = dep_11.stg_source_141_id
    left join dep_12 on dep_1.stg_source_157_id = dep_12.stg_source_95_id
    left join dep_13 on dep_1.stg_source_157_id = dep_13.stg_source_39_id
    left join dep_14 on dep_1.stg_source_157_id = dep_14.int_transformed_7_id
    left join dep_15 on dep_1.stg_source_157_id = dep_15.stg_source_139_id
    left join dep_16 on dep_1.stg_source_157_id = dep_16.stg_source_52_id
    left join dep_17 on dep_1.stg_source_157_id = dep_17.stg_source_80_id
    left join dep_18 on dep_1.stg_source_157_id = dep_18.stg_source_153_id
    left join dep_19 on dep_1.stg_source_157_id = dep_19.int_transformed_165_id
    left join dep_20 on dep_1.stg_source_157_id = dep_20.int_transformed_8_id
    left join dep_21 on dep_1.stg_source_157_id = dep_21.int_transformed_159_id
    left join dep_22 on dep_1.stg_source_157_id = dep_22.int_transformed_46_id
    left join dep_23 on dep_1.stg_source_157_id = dep_23.int_transformed_155_id
    left join dep_24 on dep_1.stg_source_157_id = dep_24.stg_source_21_id
    left join dep_25 on dep_1.stg_source_157_id = dep_25.stg_source_110_id
    left join dep_26 on dep_1.stg_source_157_id = dep_26.int_transformed_76_id
    left join dep_27 on dep_1.stg_source_157_id = dep_27.stg_source_34_id
    left join dep_28 on dep_1.stg_source_157_id = dep_28.int_transformed_110_id
    left join dep_29 on dep_1.stg_source_157_id = dep_29.stg_source_32_id
    left join dep_30 on dep_1.stg_source_157_id = dep_30.stg_source_114_id
    left join dep_31 on dep_1.stg_source_157_id = dep_31.int_transformed_108_id
    left join dep_32 on dep_1.stg_source_157_id = dep_32.stg_source_29_id
    left join dep_33 on dep_1.stg_source_157_id = dep_33.int_transformed_147_id
    left join dep_34 on dep_1.stg_source_157_id = dep_34.stg_source_126_id
    left join dep_35 on dep_1.stg_source_157_id = dep_35.int_transformed_6_id
    left join dep_36 on dep_1.stg_source_157_id = dep_36.int_transformed_13_id
    left join dep_37 on dep_1.stg_source_157_id = dep_37.stg_source_6_id
    left join dep_38 on dep_1.stg_source_157_id = dep_38.int_transformed_11_id
    left join dep_39 on dep_1.stg_source_157_id = dep_39.int_transformed_18_id
    left join dep_40 on dep_1.stg_source_157_id = dep_40.stg_source_84_id
    left join dep_41 on dep_1.stg_source_157_id = dep_41.stg_source_86_id
    left join dep_42 on dep_1.stg_source_157_id = dep_42.int_transformed_135_id
    left join dep_43 on dep_1.stg_source_157_id = dep_43.stg_source_56_id
    left join dep_44 on dep_1.stg_source_157_id = dep_44.int_transformed_152_id
    left join dep_45 on dep_1.stg_source_157_id = dep_45.int_transformed_95_id
    left join dep_46 on dep_1.stg_source_157_id = dep_46.stg_source_127_id
    left join dep_47 on dep_1.stg_source_157_id = dep_47.stg_source_36_id
    left join dep_48 on dep_1.stg_source_157_id = dep_48.stg_source_26_id
    left join dep_49 on dep_1.stg_source_157_id = dep_49.int_transformed_96_id
    left join dep_50 on dep_1.stg_source_157_id = dep_50.int_transformed_4_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
