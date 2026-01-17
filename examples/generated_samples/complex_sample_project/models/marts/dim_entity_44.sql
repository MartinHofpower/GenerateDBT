{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_30') }}
),

dep_2 as (
    select * from {{ ref('stg_source_152') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_4 as (
    select * from {{ ref('stg_source_142') }}
),

dep_5 as (
    select * from {{ ref('stg_source_166') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_7 as (
    select * from {{ ref('stg_source_122') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_9 as (
    select * from {{ ref('stg_source_146') }}
),

dep_10 as (
    select * from {{ ref('stg_source_130') }}
),

dep_11 as (
    select * from {{ ref('stg_source_48') }}
),

dep_12 as (
    select * from {{ ref('stg_source_136') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_17 as (
    select * from {{ ref('stg_source_151') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_21 as (
    select * from {{ ref('stg_source_11') }}
),

dep_22 as (
    select * from {{ ref('stg_source_81') }}
),

dep_23 as (
    select * from {{ ref('stg_source_3') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_25 as (
    select * from {{ ref('stg_source_44') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_27 as (
    select * from {{ ref('stg_source_159') }}
),

dep_28 as (
    select * from {{ ref('stg_source_24') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_31 as (
    select * from {{ ref('stg_source_135') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_34 as (
    select * from {{ ref('stg_source_94') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_36 as (
    select * from {{ ref('stg_source_17') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_40 as (
    select * from {{ ref('stg_source_157') }}
),

dep_41 as (
    select * from {{ ref('stg_source_88') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_43 as (
    select * from {{ ref('stg_source_28') }}
),

dep_44 as (
    select * from {{ ref('stg_source_15') }}
),

dep_45 as (
    select * from {{ ref('stg_source_133') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_47 as (
    select * from {{ ref('stg_source_7') }}
),

dep_48 as (
    select * from {{ ref('stg_source_147') }}
),

dep_49 as (
    select * from {{ ref('stg_source_52') }}
),

dep_50 as (
    select * from {{ ref('stg_source_65') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_30_id']) }} as surrogate_id,
        dep_1.stg_source_30_id as dim_entity_44_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_30_id = dep_2.stg_source_152_id
    left join dep_3 on dep_1.stg_source_30_id = dep_3.int_transformed_111_id
    left join dep_4 on dep_1.stg_source_30_id = dep_4.stg_source_142_id
    left join dep_5 on dep_1.stg_source_30_id = dep_5.stg_source_166_id
    left join dep_6 on dep_1.stg_source_30_id = dep_6.int_transformed_105_id
    left join dep_7 on dep_1.stg_source_30_id = dep_7.stg_source_122_id
    left join dep_8 on dep_1.stg_source_30_id = dep_8.int_transformed_141_id
    left join dep_9 on dep_1.stg_source_30_id = dep_9.stg_source_146_id
    left join dep_10 on dep_1.stg_source_30_id = dep_10.stg_source_130_id
    left join dep_11 on dep_1.stg_source_30_id = dep_11.stg_source_48_id
    left join dep_12 on dep_1.stg_source_30_id = dep_12.stg_source_136_id
    left join dep_13 on dep_1.stg_source_30_id = dep_13.int_transformed_31_id
    left join dep_14 on dep_1.stg_source_30_id = dep_14.int_transformed_113_id
    left join dep_15 on dep_1.stg_source_30_id = dep_15.int_transformed_3_id
    left join dep_16 on dep_1.stg_source_30_id = dep_16.int_transformed_109_id
    left join dep_17 on dep_1.stg_source_30_id = dep_17.stg_source_151_id
    left join dep_18 on dep_1.stg_source_30_id = dep_18.int_transformed_112_id
    left join dep_19 on dep_1.stg_source_30_id = dep_19.int_transformed_66_id
    left join dep_20 on dep_1.stg_source_30_id = dep_20.int_transformed_47_id
    left join dep_21 on dep_1.stg_source_30_id = dep_21.stg_source_11_id
    left join dep_22 on dep_1.stg_source_30_id = dep_22.stg_source_81_id
    left join dep_23 on dep_1.stg_source_30_id = dep_23.stg_source_3_id
    left join dep_24 on dep_1.stg_source_30_id = dep_24.int_transformed_95_id
    left join dep_25 on dep_1.stg_source_30_id = dep_25.stg_source_44_id
    left join dep_26 on dep_1.stg_source_30_id = dep_26.int_transformed_85_id
    left join dep_27 on dep_1.stg_source_30_id = dep_27.stg_source_159_id
    left join dep_28 on dep_1.stg_source_30_id = dep_28.stg_source_24_id
    left join dep_29 on dep_1.stg_source_30_id = dep_29.int_transformed_27_id
    left join dep_30 on dep_1.stg_source_30_id = dep_30.int_transformed_144_id
    left join dep_31 on dep_1.stg_source_30_id = dep_31.stg_source_135_id
    left join dep_32 on dep_1.stg_source_30_id = dep_32.int_transformed_142_id
    left join dep_33 on dep_1.stg_source_30_id = dep_33.int_transformed_2_id
    left join dep_34 on dep_1.stg_source_30_id = dep_34.stg_source_94_id
    left join dep_35 on dep_1.stg_source_30_id = dep_35.int_transformed_139_id
    left join dep_36 on dep_1.stg_source_30_id = dep_36.stg_source_17_id
    left join dep_37 on dep_1.stg_source_30_id = dep_37.int_transformed_97_id
    left join dep_38 on dep_1.stg_source_30_id = dep_38.int_transformed_131_id
    left join dep_39 on dep_1.stg_source_30_id = dep_39.int_transformed_116_id
    left join dep_40 on dep_1.stg_source_30_id = dep_40.stg_source_157_id
    left join dep_41 on dep_1.stg_source_30_id = dep_41.stg_source_88_id
    left join dep_42 on dep_1.stg_source_30_id = dep_42.int_transformed_148_id
    left join dep_43 on dep_1.stg_source_30_id = dep_43.stg_source_28_id
    left join dep_44 on dep_1.stg_source_30_id = dep_44.stg_source_15_id
    left join dep_45 on dep_1.stg_source_30_id = dep_45.stg_source_133_id
    left join dep_46 on dep_1.stg_source_30_id = dep_46.int_transformed_33_id
    left join dep_47 on dep_1.stg_source_30_id = dep_47.stg_source_7_id
    left join dep_48 on dep_1.stg_source_30_id = dep_48.stg_source_147_id
    left join dep_49 on dep_1.stg_source_30_id = dep_49.stg_source_52_id
    left join dep_50 on dep_1.stg_source_30_id = dep_50.stg_source_65_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
