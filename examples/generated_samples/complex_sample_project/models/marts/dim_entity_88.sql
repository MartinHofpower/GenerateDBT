{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_130') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_4 as (
    select * from {{ ref('stg_source_8') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_8 as (
    select * from {{ ref('stg_source_36') }}
),

dep_9 as (
    select * from {{ ref('stg_source_14') }}
),

dep_10 as (
    select * from {{ ref('stg_source_52') }}
),

dep_11 as (
    select * from {{ ref('stg_source_91') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_14 as (
    select * from {{ ref('stg_source_110') }}
),

dep_15 as (
    select * from {{ ref('stg_source_123') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_18 as (
    select * from {{ ref('stg_source_61') }}
),

dep_19 as (
    select * from {{ ref('stg_source_9') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_21 as (
    select * from {{ ref('stg_source_69') }}
),

dep_22 as (
    select * from {{ ref('stg_source_83') }}
),

dep_23 as (
    select * from {{ ref('stg_source_46') }}
),

dep_24 as (
    select * from {{ ref('stg_source_20') }}
),

dep_25 as (
    select * from {{ ref('stg_source_120') }}
),

dep_26 as (
    select * from {{ ref('stg_source_165') }}
),

dep_27 as (
    select * from {{ ref('stg_source_145') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_29 as (
    select * from {{ ref('stg_source_136') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_31 as (
    select * from {{ ref('stg_source_64') }}
),

dep_32 as (
    select * from {{ ref('stg_source_103') }}
),

dep_33 as (
    select * from {{ ref('stg_source_57') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_36 as (
    select * from {{ ref('stg_source_66') }}
),

dep_37 as (
    select * from {{ ref('stg_source_135') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_40 as (
    select * from {{ ref('stg_source_6') }}
),

dep_41 as (
    select * from {{ ref('stg_source_143') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_47 as (
    select * from {{ ref('stg_source_163') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_50 as (
    select * from {{ ref('stg_source_75') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_130_id']) }} as surrogate_id,
        dep_1.stg_source_130_id as dim_entity_88_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_130_id = dep_2.int_transformed_50_id
    left join dep_3 on dep_1.stg_source_130_id = dep_3.int_transformed_94_id
    left join dep_4 on dep_1.stg_source_130_id = dep_4.stg_source_8_id
    left join dep_5 on dep_1.stg_source_130_id = dep_5.int_transformed_25_id
    left join dep_6 on dep_1.stg_source_130_id = dep_6.int_transformed_1_id
    left join dep_7 on dep_1.stg_source_130_id = dep_7.int_transformed_83_id
    left join dep_8 on dep_1.stg_source_130_id = dep_8.stg_source_36_id
    left join dep_9 on dep_1.stg_source_130_id = dep_9.stg_source_14_id
    left join dep_10 on dep_1.stg_source_130_id = dep_10.stg_source_52_id
    left join dep_11 on dep_1.stg_source_130_id = dep_11.stg_source_91_id
    left join dep_12 on dep_1.stg_source_130_id = dep_12.int_transformed_39_id
    left join dep_13 on dep_1.stg_source_130_id = dep_13.int_transformed_7_id
    left join dep_14 on dep_1.stg_source_130_id = dep_14.stg_source_110_id
    left join dep_15 on dep_1.stg_source_130_id = dep_15.stg_source_123_id
    left join dep_16 on dep_1.stg_source_130_id = dep_16.int_transformed_77_id
    left join dep_17 on dep_1.stg_source_130_id = dep_17.int_transformed_150_id
    left join dep_18 on dep_1.stg_source_130_id = dep_18.stg_source_61_id
    left join dep_19 on dep_1.stg_source_130_id = dep_19.stg_source_9_id
    left join dep_20 on dep_1.stg_source_130_id = dep_20.int_transformed_116_id
    left join dep_21 on dep_1.stg_source_130_id = dep_21.stg_source_69_id
    left join dep_22 on dep_1.stg_source_130_id = dep_22.stg_source_83_id
    left join dep_23 on dep_1.stg_source_130_id = dep_23.stg_source_46_id
    left join dep_24 on dep_1.stg_source_130_id = dep_24.stg_source_20_id
    left join dep_25 on dep_1.stg_source_130_id = dep_25.stg_source_120_id
    left join dep_26 on dep_1.stg_source_130_id = dep_26.stg_source_165_id
    left join dep_27 on dep_1.stg_source_130_id = dep_27.stg_source_145_id
    left join dep_28 on dep_1.stg_source_130_id = dep_28.int_transformed_76_id
    left join dep_29 on dep_1.stg_source_130_id = dep_29.stg_source_136_id
    left join dep_30 on dep_1.stg_source_130_id = dep_30.int_transformed_30_id
    left join dep_31 on dep_1.stg_source_130_id = dep_31.stg_source_64_id
    left join dep_32 on dep_1.stg_source_130_id = dep_32.stg_source_103_id
    left join dep_33 on dep_1.stg_source_130_id = dep_33.stg_source_57_id
    left join dep_34 on dep_1.stg_source_130_id = dep_34.int_transformed_130_id
    left join dep_35 on dep_1.stg_source_130_id = dep_35.int_transformed_99_id
    left join dep_36 on dep_1.stg_source_130_id = dep_36.stg_source_66_id
    left join dep_37 on dep_1.stg_source_130_id = dep_37.stg_source_135_id
    left join dep_38 on dep_1.stg_source_130_id = dep_38.int_transformed_60_id
    left join dep_39 on dep_1.stg_source_130_id = dep_39.int_transformed_131_id
    left join dep_40 on dep_1.stg_source_130_id = dep_40.stg_source_6_id
    left join dep_41 on dep_1.stg_source_130_id = dep_41.stg_source_143_id
    left join dep_42 on dep_1.stg_source_130_id = dep_42.int_transformed_52_id
    left join dep_43 on dep_1.stg_source_130_id = dep_43.int_transformed_37_id
    left join dep_44 on dep_1.stg_source_130_id = dep_44.int_transformed_166_id
    left join dep_45 on dep_1.stg_source_130_id = dep_45.int_transformed_96_id
    left join dep_46 on dep_1.stg_source_130_id = dep_46.int_transformed_159_id
    left join dep_47 on dep_1.stg_source_130_id = dep_47.stg_source_163_id
    left join dep_48 on dep_1.stg_source_130_id = dep_48.int_transformed_13_id
    left join dep_49 on dep_1.stg_source_130_id = dep_49.int_transformed_56_id
    left join dep_50 on dep_1.stg_source_130_id = dep_50.stg_source_75_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
