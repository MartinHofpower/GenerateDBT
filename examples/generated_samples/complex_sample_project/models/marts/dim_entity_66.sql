{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_4') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_3 as (
    select * from {{ ref('stg_source_15') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_6 as (
    select * from {{ ref('stg_source_161') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_10 as (
    select * from {{ ref('stg_source_86') }}
),

dep_11 as (
    select * from {{ ref('stg_source_110') }}
),

dep_12 as (
    select * from {{ ref('stg_source_135') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_14 as (
    select * from {{ ref('stg_source_21') }}
),

dep_15 as (
    select * from {{ ref('stg_source_70') }}
),

dep_16 as (
    select * from {{ ref('stg_source_133') }}
),

dep_17 as (
    select * from {{ ref('stg_source_89') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_19 as (
    select * from {{ ref('stg_source_148') }}
),

dep_20 as (
    select * from {{ ref('stg_source_65') }}
),

dep_21 as (
    select * from {{ ref('stg_source_109') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_24 as (
    select * from {{ ref('stg_source_1') }}
),

dep_25 as (
    select * from {{ ref('stg_source_3') }}
),

dep_26 as (
    select * from {{ ref('stg_source_31') }}
),

dep_27 as (
    select * from {{ ref('stg_source_59') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_30 as (
    select * from {{ ref('stg_source_87') }}
),

dep_31 as (
    select * from {{ ref('stg_source_115') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_33 as (
    select * from {{ ref('stg_source_80') }}
),

dep_34 as (
    select * from {{ ref('stg_source_14') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_37 as (
    select * from {{ ref('stg_source_77') }}
),

dep_38 as (
    select * from {{ ref('stg_source_26') }}
),

dep_39 as (
    select * from {{ ref('stg_source_93') }}
),

dep_40 as (
    select * from {{ ref('stg_source_150') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_42 as (
    select * from {{ ref('stg_source_48') }}
),

dep_43 as (
    select * from {{ ref('stg_source_43') }}
),

dep_44 as (
    select * from {{ ref('stg_source_72') }}
),

dep_45 as (
    select * from {{ ref('stg_source_141') }}
),

dep_46 as (
    select * from {{ ref('stg_source_76') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_48 as (
    select * from {{ ref('stg_source_57') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_88') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_4_id']) }} as surrogate_id,
        dep_1.stg_source_4_id as dim_entity_66_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_4_id = dep_2.int_transformed_9_id
    left join dep_3 on dep_1.stg_source_4_id = dep_3.stg_source_15_id
    left join dep_4 on dep_1.stg_source_4_id = dep_4.int_transformed_11_id
    left join dep_5 on dep_1.stg_source_4_id = dep_5.int_transformed_106_id
    left join dep_6 on dep_1.stg_source_4_id = dep_6.stg_source_161_id
    left join dep_7 on dep_1.stg_source_4_id = dep_7.int_transformed_104_id
    left join dep_8 on dep_1.stg_source_4_id = dep_8.int_transformed_63_id
    left join dep_9 on dep_1.stg_source_4_id = dep_9.int_transformed_33_id
    left join dep_10 on dep_1.stg_source_4_id = dep_10.stg_source_86_id
    left join dep_11 on dep_1.stg_source_4_id = dep_11.stg_source_110_id
    left join dep_12 on dep_1.stg_source_4_id = dep_12.stg_source_135_id
    left join dep_13 on dep_1.stg_source_4_id = dep_13.int_transformed_71_id
    left join dep_14 on dep_1.stg_source_4_id = dep_14.stg_source_21_id
    left join dep_15 on dep_1.stg_source_4_id = dep_15.stg_source_70_id
    left join dep_16 on dep_1.stg_source_4_id = dep_16.stg_source_133_id
    left join dep_17 on dep_1.stg_source_4_id = dep_17.stg_source_89_id
    left join dep_18 on dep_1.stg_source_4_id = dep_18.int_transformed_125_id
    left join dep_19 on dep_1.stg_source_4_id = dep_19.stg_source_148_id
    left join dep_20 on dep_1.stg_source_4_id = dep_20.stg_source_65_id
    left join dep_21 on dep_1.stg_source_4_id = dep_21.stg_source_109_id
    left join dep_22 on dep_1.stg_source_4_id = dep_22.int_transformed_78_id
    left join dep_23 on dep_1.stg_source_4_id = dep_23.int_transformed_157_id
    left join dep_24 on dep_1.stg_source_4_id = dep_24.stg_source_1_id
    left join dep_25 on dep_1.stg_source_4_id = dep_25.stg_source_3_id
    left join dep_26 on dep_1.stg_source_4_id = dep_26.stg_source_31_id
    left join dep_27 on dep_1.stg_source_4_id = dep_27.stg_source_59_id
    left join dep_28 on dep_1.stg_source_4_id = dep_28.int_transformed_16_id
    left join dep_29 on dep_1.stg_source_4_id = dep_29.int_transformed_156_id
    left join dep_30 on dep_1.stg_source_4_id = dep_30.stg_source_87_id
    left join dep_31 on dep_1.stg_source_4_id = dep_31.stg_source_115_id
    left join dep_32 on dep_1.stg_source_4_id = dep_32.int_transformed_127_id
    left join dep_33 on dep_1.stg_source_4_id = dep_33.stg_source_80_id
    left join dep_34 on dep_1.stg_source_4_id = dep_34.stg_source_14_id
    left join dep_35 on dep_1.stg_source_4_id = dep_35.int_transformed_152_id
    left join dep_36 on dep_1.stg_source_4_id = dep_36.int_transformed_27_id
    left join dep_37 on dep_1.stg_source_4_id = dep_37.stg_source_77_id
    left join dep_38 on dep_1.stg_source_4_id = dep_38.stg_source_26_id
    left join dep_39 on dep_1.stg_source_4_id = dep_39.stg_source_93_id
    left join dep_40 on dep_1.stg_source_4_id = dep_40.stg_source_150_id
    left join dep_41 on dep_1.stg_source_4_id = dep_41.int_transformed_159_id
    left join dep_42 on dep_1.stg_source_4_id = dep_42.stg_source_48_id
    left join dep_43 on dep_1.stg_source_4_id = dep_43.stg_source_43_id
    left join dep_44 on dep_1.stg_source_4_id = dep_44.stg_source_72_id
    left join dep_45 on dep_1.stg_source_4_id = dep_45.stg_source_141_id
    left join dep_46 on dep_1.stg_source_4_id = dep_46.stg_source_76_id
    left join dep_47 on dep_1.stg_source_4_id = dep_47.int_transformed_34_id
    left join dep_48 on dep_1.stg_source_4_id = dep_48.stg_source_57_id
    left join dep_49 on dep_1.stg_source_4_id = dep_49.int_transformed_81_id
    left join dep_50 on dep_1.stg_source_4_id = dep_50.int_transformed_88_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
