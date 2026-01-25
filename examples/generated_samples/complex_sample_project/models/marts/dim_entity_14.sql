{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_162') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_3 as (
    select * from {{ ref('stg_source_101') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_5 as (
    select * from {{ ref('stg_source_34') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_7 as (
    select * from {{ ref('stg_source_18') }}
),

dep_8 as (
    select * from {{ ref('stg_source_154') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_10 as (
    select * from {{ ref('stg_source_31') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_12 as (
    select * from {{ ref('stg_source_5') }}
),

dep_13 as (
    select * from {{ ref('stg_source_35') }}
),

dep_14 as (
    select * from {{ ref('stg_source_164') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_17 as (
    select * from {{ ref('stg_source_30') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_20 as (
    select * from {{ ref('stg_source_144') }}
),

dep_21 as (
    select * from {{ ref('stg_source_157') }}
),

dep_22 as (
    select * from {{ ref('stg_source_86') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_24 as (
    select * from {{ ref('stg_source_111') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_29 as (
    select * from {{ ref('stg_source_57') }}
),

dep_30 as (
    select * from {{ ref('stg_source_74') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_37 as (
    select * from {{ ref('stg_source_166') }}
),

dep_38 as (
    select * from {{ ref('stg_source_14') }}
),

dep_39 as (
    select * from {{ ref('stg_source_11') }}
),

dep_40 as (
    select * from {{ ref('stg_source_68') }}
),

dep_41 as (
    select * from {{ ref('stg_source_72') }}
),

dep_42 as (
    select * from {{ ref('stg_source_148') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_45 as (
    select * from {{ ref('stg_source_17') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_47 as (
    select * from {{ ref('stg_source_59') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_49 as (
    select * from {{ ref('stg_source_91') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_43') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_162_id']) }} as surrogate_id,
        dep_1.stg_source_162_id as dim_entity_14_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_162_id = dep_2.int_transformed_78_id
    left join dep_3 on dep_1.stg_source_162_id = dep_3.stg_source_101_id
    left join dep_4 on dep_1.stg_source_162_id = dep_4.int_transformed_68_id
    left join dep_5 on dep_1.stg_source_162_id = dep_5.stg_source_34_id
    left join dep_6 on dep_1.stg_source_162_id = dep_6.int_transformed_138_id
    left join dep_7 on dep_1.stg_source_162_id = dep_7.stg_source_18_id
    left join dep_8 on dep_1.stg_source_162_id = dep_8.stg_source_154_id
    left join dep_9 on dep_1.stg_source_162_id = dep_9.int_transformed_58_id
    left join dep_10 on dep_1.stg_source_162_id = dep_10.stg_source_31_id
    left join dep_11 on dep_1.stg_source_162_id = dep_11.int_transformed_79_id
    left join dep_12 on dep_1.stg_source_162_id = dep_12.stg_source_5_id
    left join dep_13 on dep_1.stg_source_162_id = dep_13.stg_source_35_id
    left join dep_14 on dep_1.stg_source_162_id = dep_14.stg_source_164_id
    left join dep_15 on dep_1.stg_source_162_id = dep_15.int_transformed_108_id
    left join dep_16 on dep_1.stg_source_162_id = dep_16.int_transformed_27_id
    left join dep_17 on dep_1.stg_source_162_id = dep_17.stg_source_30_id
    left join dep_18 on dep_1.stg_source_162_id = dep_18.int_transformed_7_id
    left join dep_19 on dep_1.stg_source_162_id = dep_19.int_transformed_111_id
    left join dep_20 on dep_1.stg_source_162_id = dep_20.stg_source_144_id
    left join dep_21 on dep_1.stg_source_162_id = dep_21.stg_source_157_id
    left join dep_22 on dep_1.stg_source_162_id = dep_22.stg_source_86_id
    left join dep_23 on dep_1.stg_source_162_id = dep_23.int_transformed_159_id
    left join dep_24 on dep_1.stg_source_162_id = dep_24.stg_source_111_id
    left join dep_25 on dep_1.stg_source_162_id = dep_25.int_transformed_114_id
    left join dep_26 on dep_1.stg_source_162_id = dep_26.int_transformed_144_id
    left join dep_27 on dep_1.stg_source_162_id = dep_27.int_transformed_83_id
    left join dep_28 on dep_1.stg_source_162_id = dep_28.int_transformed_142_id
    left join dep_29 on dep_1.stg_source_162_id = dep_29.stg_source_57_id
    left join dep_30 on dep_1.stg_source_162_id = dep_30.stg_source_74_id
    left join dep_31 on dep_1.stg_source_162_id = dep_31.int_transformed_74_id
    left join dep_32 on dep_1.stg_source_162_id = dep_32.int_transformed_103_id
    left join dep_33 on dep_1.stg_source_162_id = dep_33.int_transformed_1_id
    left join dep_34 on dep_1.stg_source_162_id = dep_34.int_transformed_25_id
    left join dep_35 on dep_1.stg_source_162_id = dep_35.int_transformed_46_id
    left join dep_36 on dep_1.stg_source_162_id = dep_36.int_transformed_31_id
    left join dep_37 on dep_1.stg_source_162_id = dep_37.stg_source_166_id
    left join dep_38 on dep_1.stg_source_162_id = dep_38.stg_source_14_id
    left join dep_39 on dep_1.stg_source_162_id = dep_39.stg_source_11_id
    left join dep_40 on dep_1.stg_source_162_id = dep_40.stg_source_68_id
    left join dep_41 on dep_1.stg_source_162_id = dep_41.stg_source_72_id
    left join dep_42 on dep_1.stg_source_162_id = dep_42.stg_source_148_id
    left join dep_43 on dep_1.stg_source_162_id = dep_43.int_transformed_124_id
    left join dep_44 on dep_1.stg_source_162_id = dep_44.int_transformed_96_id
    left join dep_45 on dep_1.stg_source_162_id = dep_45.stg_source_17_id
    left join dep_46 on dep_1.stg_source_162_id = dep_46.int_transformed_73_id
    left join dep_47 on dep_1.stg_source_162_id = dep_47.stg_source_59_id
    left join dep_48 on dep_1.stg_source_162_id = dep_48.int_transformed_72_id
    left join dep_49 on dep_1.stg_source_162_id = dep_49.stg_source_91_id
    left join dep_50 on dep_1.stg_source_162_id = dep_50.int_transformed_43_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
