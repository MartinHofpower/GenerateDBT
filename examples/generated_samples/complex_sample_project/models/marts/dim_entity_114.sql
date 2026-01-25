{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_6 as (
    select * from {{ ref('stg_source_159') }}
),

dep_7 as (
    select * from {{ ref('stg_source_40') }}
),

dep_8 as (
    select * from {{ ref('stg_source_100') }}
),

dep_9 as (
    select * from {{ ref('stg_source_139') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_12 as (
    select * from {{ ref('stg_source_79') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_15 as (
    select * from {{ ref('stg_source_20') }}
),

dep_16 as (
    select * from {{ ref('stg_source_9') }}
),

dep_17 as (
    select * from {{ ref('stg_source_148') }}
),

dep_18 as (
    select * from {{ ref('stg_source_34') }}
),

dep_19 as (
    select * from {{ ref('stg_source_13') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_21 as (
    select * from {{ ref('stg_source_110') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_23 as (
    select * from {{ ref('stg_source_97') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_25 as (
    select * from {{ ref('stg_source_87') }}
),

dep_26 as (
    select * from {{ ref('stg_source_156') }}
),

dep_27 as (
    select * from {{ ref('stg_source_48') }}
),

dep_28 as (
    select * from {{ ref('stg_source_8') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_30 as (
    select * from {{ ref('stg_source_150') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_33 as (
    select * from {{ ref('stg_source_135') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_35 as (
    select * from {{ ref('stg_source_115') }}
),

dep_36 as (
    select * from {{ ref('stg_source_52') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_38 as (
    select * from {{ ref('stg_source_82') }}
),

dep_39 as (
    select * from {{ ref('stg_source_127') }}
),

dep_40 as (
    select * from {{ ref('stg_source_157') }}
),

dep_41 as (
    select * from {{ ref('stg_source_146') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_43 as (
    select * from {{ ref('stg_source_142') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_45 as (
    select * from {{ ref('stg_source_45') }}
),

dep_46 as (
    select * from {{ ref('stg_source_43') }}
),

dep_47 as (
    select * from {{ ref('stg_source_59') }}
),

dep_48 as (
    select * from {{ ref('stg_source_67') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_50 as (
    select * from {{ ref('stg_source_58') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_71_id']) }} as surrogate_id,
        dep_1.int_transformed_71_id as dim_entity_114_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_71_id = dep_2.int_transformed_103_id
    left join dep_3 on dep_1.int_transformed_71_id = dep_3.int_transformed_154_id
    left join dep_4 on dep_1.int_transformed_71_id = dep_4.int_transformed_94_id
    left join dep_5 on dep_1.int_transformed_71_id = dep_5.int_transformed_9_id
    left join dep_6 on dep_1.int_transformed_71_id = dep_6.stg_source_159_id
    left join dep_7 on dep_1.int_transformed_71_id = dep_7.stg_source_40_id
    left join dep_8 on dep_1.int_transformed_71_id = dep_8.stg_source_100_id
    left join dep_9 on dep_1.int_transformed_71_id = dep_9.stg_source_139_id
    left join dep_10 on dep_1.int_transformed_71_id = dep_10.int_transformed_144_id
    left join dep_11 on dep_1.int_transformed_71_id = dep_11.int_transformed_102_id
    left join dep_12 on dep_1.int_transformed_71_id = dep_12.stg_source_79_id
    left join dep_13 on dep_1.int_transformed_71_id = dep_13.int_transformed_113_id
    left join dep_14 on dep_1.int_transformed_71_id = dep_14.int_transformed_114_id
    left join dep_15 on dep_1.int_transformed_71_id = dep_15.stg_source_20_id
    left join dep_16 on dep_1.int_transformed_71_id = dep_16.stg_source_9_id
    left join dep_17 on dep_1.int_transformed_71_id = dep_17.stg_source_148_id
    left join dep_18 on dep_1.int_transformed_71_id = dep_18.stg_source_34_id
    left join dep_19 on dep_1.int_transformed_71_id = dep_19.stg_source_13_id
    left join dep_20 on dep_1.int_transformed_71_id = dep_20.int_transformed_19_id
    left join dep_21 on dep_1.int_transformed_71_id = dep_21.stg_source_110_id
    left join dep_22 on dep_1.int_transformed_71_id = dep_22.int_transformed_101_id
    left join dep_23 on dep_1.int_transformed_71_id = dep_23.stg_source_97_id
    left join dep_24 on dep_1.int_transformed_71_id = dep_24.int_transformed_3_id
    left join dep_25 on dep_1.int_transformed_71_id = dep_25.stg_source_87_id
    left join dep_26 on dep_1.int_transformed_71_id = dep_26.stg_source_156_id
    left join dep_27 on dep_1.int_transformed_71_id = dep_27.stg_source_48_id
    left join dep_28 on dep_1.int_transformed_71_id = dep_28.stg_source_8_id
    left join dep_29 on dep_1.int_transformed_71_id = dep_29.int_transformed_68_id
    left join dep_30 on dep_1.int_transformed_71_id = dep_30.stg_source_150_id
    left join dep_31 on dep_1.int_transformed_71_id = dep_31.int_transformed_79_id
    left join dep_32 on dep_1.int_transformed_71_id = dep_32.int_transformed_40_id
    left join dep_33 on dep_1.int_transformed_71_id = dep_33.stg_source_135_id
    left join dep_34 on dep_1.int_transformed_71_id = dep_34.int_transformed_81_id
    left join dep_35 on dep_1.int_transformed_71_id = dep_35.stg_source_115_id
    left join dep_36 on dep_1.int_transformed_71_id = dep_36.stg_source_52_id
    left join dep_37 on dep_1.int_transformed_71_id = dep_37.int_transformed_88_id
    left join dep_38 on dep_1.int_transformed_71_id = dep_38.stg_source_82_id
    left join dep_39 on dep_1.int_transformed_71_id = dep_39.stg_source_127_id
    left join dep_40 on dep_1.int_transformed_71_id = dep_40.stg_source_157_id
    left join dep_41 on dep_1.int_transformed_71_id = dep_41.stg_source_146_id
    left join dep_42 on dep_1.int_transformed_71_id = dep_42.int_transformed_82_id
    left join dep_43 on dep_1.int_transformed_71_id = dep_43.stg_source_142_id
    left join dep_44 on dep_1.int_transformed_71_id = dep_44.int_transformed_18_id
    left join dep_45 on dep_1.int_transformed_71_id = dep_45.stg_source_45_id
    left join dep_46 on dep_1.int_transformed_71_id = dep_46.stg_source_43_id
    left join dep_47 on dep_1.int_transformed_71_id = dep_47.stg_source_59_id
    left join dep_48 on dep_1.int_transformed_71_id = dep_48.stg_source_67_id
    left join dep_49 on dep_1.int_transformed_71_id = dep_49.int_transformed_17_id
    left join dep_50 on dep_1.int_transformed_71_id = dep_50.stg_source_58_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
