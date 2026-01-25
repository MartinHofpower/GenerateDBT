{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_4 as (
    select * from {{ ref('stg_source_121') }}
),

dep_5 as (
    select * from {{ ref('stg_source_76') }}
),

dep_6 as (
    select * from {{ ref('stg_source_62') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_8 as (
    select * from {{ ref('stg_source_68') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_13 as (
    select * from {{ ref('stg_source_146') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_15 as (
    select * from {{ ref('stg_source_97') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_17 as (
    select * from {{ ref('stg_source_109') }}
),

dep_18 as (
    select * from {{ ref('stg_source_28') }}
),

dep_19 as (
    select * from {{ ref('stg_source_101') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_21 as (
    select * from {{ ref('stg_source_78') }}
),

dep_22 as (
    select * from {{ ref('stg_source_43') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_25 as (
    select * from {{ ref('stg_source_90') }}
),

dep_26 as (
    select * from {{ ref('stg_source_73') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_28 as (
    select * from {{ ref('stg_source_163') }}
),

dep_29 as (
    select * from {{ ref('stg_source_26') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_32 as (
    select * from {{ ref('stg_source_112') }}
),

dep_33 as (
    select * from {{ ref('stg_source_161') }}
),

dep_34 as (
    select * from {{ ref('stg_source_59') }}
),

dep_35 as (
    select * from {{ ref('stg_source_36') }}
),

dep_36 as (
    select * from {{ ref('stg_source_144') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_39 as (
    select * from {{ ref('stg_source_18') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_42 as (
    select * from {{ ref('stg_source_33') }}
),

dep_43 as (
    select * from {{ ref('stg_source_50') }}
),

dep_44 as (
    select * from {{ ref('stg_source_117') }}
),

dep_45 as (
    select * from {{ ref('stg_source_66') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_47 as (
    select * from {{ ref('stg_source_6') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_49 as (
    select * from {{ ref('stg_source_77') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_141') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_102_id']) }} as surrogate_id,
        dep_1.int_transformed_102_id as dim_entity_142_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_102_id = dep_2.int_transformed_114_id
    left join dep_3 on dep_1.int_transformed_102_id = dep_3.int_transformed_23_id
    left join dep_4 on dep_1.int_transformed_102_id = dep_4.stg_source_121_id
    left join dep_5 on dep_1.int_transformed_102_id = dep_5.stg_source_76_id
    left join dep_6 on dep_1.int_transformed_102_id = dep_6.stg_source_62_id
    left join dep_7 on dep_1.int_transformed_102_id = dep_7.int_transformed_162_id
    left join dep_8 on dep_1.int_transformed_102_id = dep_8.stg_source_68_id
    left join dep_9 on dep_1.int_transformed_102_id = dep_9.int_transformed_154_id
    left join dep_10 on dep_1.int_transformed_102_id = dep_10.int_transformed_110_id
    left join dep_11 on dep_1.int_transformed_102_id = dep_11.int_transformed_99_id
    left join dep_12 on dep_1.int_transformed_102_id = dep_12.int_transformed_79_id
    left join dep_13 on dep_1.int_transformed_102_id = dep_13.stg_source_146_id
    left join dep_14 on dep_1.int_transformed_102_id = dep_14.int_transformed_106_id
    left join dep_15 on dep_1.int_transformed_102_id = dep_15.stg_source_97_id
    left join dep_16 on dep_1.int_transformed_102_id = dep_16.int_transformed_18_id
    left join dep_17 on dep_1.int_transformed_102_id = dep_17.stg_source_109_id
    left join dep_18 on dep_1.int_transformed_102_id = dep_18.stg_source_28_id
    left join dep_19 on dep_1.int_transformed_102_id = dep_19.stg_source_101_id
    left join dep_20 on dep_1.int_transformed_102_id = dep_20.int_transformed_100_id
    left join dep_21 on dep_1.int_transformed_102_id = dep_21.stg_source_78_id
    left join dep_22 on dep_1.int_transformed_102_id = dep_22.stg_source_43_id
    left join dep_23 on dep_1.int_transformed_102_id = dep_23.int_transformed_46_id
    left join dep_24 on dep_1.int_transformed_102_id = dep_24.int_transformed_84_id
    left join dep_25 on dep_1.int_transformed_102_id = dep_25.stg_source_90_id
    left join dep_26 on dep_1.int_transformed_102_id = dep_26.stg_source_73_id
    left join dep_27 on dep_1.int_transformed_102_id = dep_27.int_transformed_124_id
    left join dep_28 on dep_1.int_transformed_102_id = dep_28.stg_source_163_id
    left join dep_29 on dep_1.int_transformed_102_id = dep_29.stg_source_26_id
    left join dep_30 on dep_1.int_transformed_102_id = dep_30.int_transformed_51_id
    left join dep_31 on dep_1.int_transformed_102_id = dep_31.int_transformed_40_id
    left join dep_32 on dep_1.int_transformed_102_id = dep_32.stg_source_112_id
    left join dep_33 on dep_1.int_transformed_102_id = dep_33.stg_source_161_id
    left join dep_34 on dep_1.int_transformed_102_id = dep_34.stg_source_59_id
    left join dep_35 on dep_1.int_transformed_102_id = dep_35.stg_source_36_id
    left join dep_36 on dep_1.int_transformed_102_id = dep_36.stg_source_144_id
    left join dep_37 on dep_1.int_transformed_102_id = dep_37.int_transformed_26_id
    left join dep_38 on dep_1.int_transformed_102_id = dep_38.int_transformed_4_id
    left join dep_39 on dep_1.int_transformed_102_id = dep_39.stg_source_18_id
    left join dep_40 on dep_1.int_transformed_102_id = dep_40.int_transformed_19_id
    left join dep_41 on dep_1.int_transformed_102_id = dep_41.int_transformed_37_id
    left join dep_42 on dep_1.int_transformed_102_id = dep_42.stg_source_33_id
    left join dep_43 on dep_1.int_transformed_102_id = dep_43.stg_source_50_id
    left join dep_44 on dep_1.int_transformed_102_id = dep_44.stg_source_117_id
    left join dep_45 on dep_1.int_transformed_102_id = dep_45.stg_source_66_id
    left join dep_46 on dep_1.int_transformed_102_id = dep_46.int_transformed_65_id
    left join dep_47 on dep_1.int_transformed_102_id = dep_47.stg_source_6_id
    left join dep_48 on dep_1.int_transformed_102_id = dep_48.int_transformed_83_id
    left join dep_49 on dep_1.int_transformed_102_id = dep_49.stg_source_77_id
    left join dep_50 on dep_1.int_transformed_102_id = dep_50.int_transformed_141_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
