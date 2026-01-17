{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_60') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_3 as (
    select * from {{ ref('stg_source_133') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_9 as (
    select * from {{ ref('stg_source_155') }}
),

dep_10 as (
    select * from {{ ref('stg_source_125') }}
),

dep_11 as (
    select * from {{ ref('stg_source_143') }}
),

dep_12 as (
    select * from {{ ref('stg_source_55') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_16 as (
    select * from {{ ref('stg_source_28') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_18 as (
    select * from {{ ref('stg_source_5') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_20 as (
    select * from {{ ref('stg_source_10') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_22 as (
    select * from {{ ref('stg_source_135') }}
),

dep_23 as (
    select * from {{ ref('stg_source_113') }}
),

dep_24 as (
    select * from {{ ref('stg_source_69') }}
),

dep_25 as (
    select * from {{ ref('stg_source_72') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_27 as (
    select * from {{ ref('stg_source_112') }}
),

dep_28 as (
    select * from {{ ref('stg_source_66') }}
),

dep_29 as (
    select * from {{ ref('stg_source_119') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_31 as (
    select * from {{ ref('stg_source_16') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_33 as (
    select * from {{ ref('stg_source_54') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_37 as (
    select * from {{ ref('stg_source_57') }}
),

dep_38 as (
    select * from {{ ref('stg_source_151') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_40 as (
    select * from {{ ref('stg_source_122') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_44 as (
    select * from {{ ref('stg_source_97') }}
),

dep_45 as (
    select * from {{ ref('stg_source_6') }}
),

dep_46 as (
    select * from {{ ref('stg_source_124') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_49 as (
    select * from {{ ref('stg_source_80') }}
),

dep_50 as (
    select * from {{ ref('stg_source_7') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_60_id']) }} as surrogate_id,
        dep_1.stg_source_60_id as dim_entity_104_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_60_id = dep_2.int_transformed_94_id
    left join dep_3 on dep_1.stg_source_60_id = dep_3.stg_source_133_id
    left join dep_4 on dep_1.stg_source_60_id = dep_4.int_transformed_145_id
    left join dep_5 on dep_1.stg_source_60_id = dep_5.int_transformed_60_id
    left join dep_6 on dep_1.stg_source_60_id = dep_6.int_transformed_58_id
    left join dep_7 on dep_1.stg_source_60_id = dep_7.int_transformed_7_id
    left join dep_8 on dep_1.stg_source_60_id = dep_8.int_transformed_143_id
    left join dep_9 on dep_1.stg_source_60_id = dep_9.stg_source_155_id
    left join dep_10 on dep_1.stg_source_60_id = dep_10.stg_source_125_id
    left join dep_11 on dep_1.stg_source_60_id = dep_11.stg_source_143_id
    left join dep_12 on dep_1.stg_source_60_id = dep_12.stg_source_55_id
    left join dep_13 on dep_1.stg_source_60_id = dep_13.int_transformed_71_id
    left join dep_14 on dep_1.stg_source_60_id = dep_14.int_transformed_55_id
    left join dep_15 on dep_1.stg_source_60_id = dep_15.int_transformed_65_id
    left join dep_16 on dep_1.stg_source_60_id = dep_16.stg_source_28_id
    left join dep_17 on dep_1.stg_source_60_id = dep_17.int_transformed_136_id
    left join dep_18 on dep_1.stg_source_60_id = dep_18.stg_source_5_id
    left join dep_19 on dep_1.stg_source_60_id = dep_19.int_transformed_34_id
    left join dep_20 on dep_1.stg_source_60_id = dep_20.stg_source_10_id
    left join dep_21 on dep_1.stg_source_60_id = dep_21.int_transformed_53_id
    left join dep_22 on dep_1.stg_source_60_id = dep_22.stg_source_135_id
    left join dep_23 on dep_1.stg_source_60_id = dep_23.stg_source_113_id
    left join dep_24 on dep_1.stg_source_60_id = dep_24.stg_source_69_id
    left join dep_25 on dep_1.stg_source_60_id = dep_25.stg_source_72_id
    left join dep_26 on dep_1.stg_source_60_id = dep_26.int_transformed_149_id
    left join dep_27 on dep_1.stg_source_60_id = dep_27.stg_source_112_id
    left join dep_28 on dep_1.stg_source_60_id = dep_28.stg_source_66_id
    left join dep_29 on dep_1.stg_source_60_id = dep_29.stg_source_119_id
    left join dep_30 on dep_1.stg_source_60_id = dep_30.int_transformed_152_id
    left join dep_31 on dep_1.stg_source_60_id = dep_31.stg_source_16_id
    left join dep_32 on dep_1.stg_source_60_id = dep_32.int_transformed_125_id
    left join dep_33 on dep_1.stg_source_60_id = dep_33.stg_source_54_id
    left join dep_34 on dep_1.stg_source_60_id = dep_34.int_transformed_46_id
    left join dep_35 on dep_1.stg_source_60_id = dep_35.int_transformed_99_id
    left join dep_36 on dep_1.stg_source_60_id = dep_36.int_transformed_124_id
    left join dep_37 on dep_1.stg_source_60_id = dep_37.stg_source_57_id
    left join dep_38 on dep_1.stg_source_60_id = dep_38.stg_source_151_id
    left join dep_39 on dep_1.stg_source_60_id = dep_39.int_transformed_159_id
    left join dep_40 on dep_1.stg_source_60_id = dep_40.stg_source_122_id
    left join dep_41 on dep_1.stg_source_60_id = dep_41.int_transformed_140_id
    left join dep_42 on dep_1.stg_source_60_id = dep_42.int_transformed_50_id
    left join dep_43 on dep_1.stg_source_60_id = dep_43.int_transformed_137_id
    left join dep_44 on dep_1.stg_source_60_id = dep_44.stg_source_97_id
    left join dep_45 on dep_1.stg_source_60_id = dep_45.stg_source_6_id
    left join dep_46 on dep_1.stg_source_60_id = dep_46.stg_source_124_id
    left join dep_47 on dep_1.stg_source_60_id = dep_47.int_transformed_107_id
    left join dep_48 on dep_1.stg_source_60_id = dep_48.int_transformed_10_id
    left join dep_49 on dep_1.stg_source_60_id = dep_49.stg_source_80_id
    left join dep_50 on dep_1.stg_source_60_id = dep_50.stg_source_7_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
