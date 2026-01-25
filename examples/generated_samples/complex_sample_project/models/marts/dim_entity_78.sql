{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_3 as (
    select * from {{ ref('stg_source_106') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_5 as (
    select * from {{ ref('stg_source_60') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_7 as (
    select * from {{ ref('stg_source_42') }}
),

dep_8 as (
    select * from {{ ref('stg_source_55') }}
),

dep_9 as (
    select * from {{ ref('stg_source_54') }}
),

dep_10 as (
    select * from {{ ref('stg_source_142') }}
),

dep_11 as (
    select * from {{ ref('stg_source_126') }}
),

dep_12 as (
    select * from {{ ref('stg_source_37') }}
),

dep_13 as (
    select * from {{ ref('stg_source_47') }}
),

dep_14 as (
    select * from {{ ref('stg_source_61') }}
),

dep_15 as (
    select * from {{ ref('stg_source_34') }}
),

dep_16 as (
    select * from {{ ref('stg_source_138') }}
),

dep_17 as (
    select * from {{ ref('stg_source_86') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_19 as (
    select * from {{ ref('stg_source_153') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_22 as (
    select * from {{ ref('stg_source_107') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_24 as (
    select * from {{ ref('stg_source_152') }}
),

dep_25 as (
    select * from {{ ref('stg_source_114') }}
),

dep_26 as (
    select * from {{ ref('stg_source_36') }}
),

dep_27 as (
    select * from {{ ref('stg_source_53') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_33 as (
    select * from {{ ref('stg_source_49') }}
),

dep_34 as (
    select * from {{ ref('stg_source_160') }}
),

dep_35 as (
    select * from {{ ref('stg_source_73') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_40 as (
    select * from {{ ref('stg_source_10') }}
),

dep_41 as (
    select * from {{ ref('stg_source_68') }}
),

dep_42 as (
    select * from {{ ref('stg_source_165') }}
),

dep_43 as (
    select * from {{ ref('stg_source_136') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_47 as (
    select * from {{ ref('stg_source_112') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_72') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_73_id']) }} as surrogate_id,
        dep_1.int_transformed_73_id as dim_entity_78_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_73_id = dep_2.int_transformed_89_id
    left join dep_3 on dep_1.int_transformed_73_id = dep_3.stg_source_106_id
    left join dep_4 on dep_1.int_transformed_73_id = dep_4.int_transformed_65_id
    left join dep_5 on dep_1.int_transformed_73_id = dep_5.stg_source_60_id
    left join dep_6 on dep_1.int_transformed_73_id = dep_6.int_transformed_162_id
    left join dep_7 on dep_1.int_transformed_73_id = dep_7.stg_source_42_id
    left join dep_8 on dep_1.int_transformed_73_id = dep_8.stg_source_55_id
    left join dep_9 on dep_1.int_transformed_73_id = dep_9.stg_source_54_id
    left join dep_10 on dep_1.int_transformed_73_id = dep_10.stg_source_142_id
    left join dep_11 on dep_1.int_transformed_73_id = dep_11.stg_source_126_id
    left join dep_12 on dep_1.int_transformed_73_id = dep_12.stg_source_37_id
    left join dep_13 on dep_1.int_transformed_73_id = dep_13.stg_source_47_id
    left join dep_14 on dep_1.int_transformed_73_id = dep_14.stg_source_61_id
    left join dep_15 on dep_1.int_transformed_73_id = dep_15.stg_source_34_id
    left join dep_16 on dep_1.int_transformed_73_id = dep_16.stg_source_138_id
    left join dep_17 on dep_1.int_transformed_73_id = dep_17.stg_source_86_id
    left join dep_18 on dep_1.int_transformed_73_id = dep_18.int_transformed_35_id
    left join dep_19 on dep_1.int_transformed_73_id = dep_19.stg_source_153_id
    left join dep_20 on dep_1.int_transformed_73_id = dep_20.int_transformed_61_id
    left join dep_21 on dep_1.int_transformed_73_id = dep_21.int_transformed_77_id
    left join dep_22 on dep_1.int_transformed_73_id = dep_22.stg_source_107_id
    left join dep_23 on dep_1.int_transformed_73_id = dep_23.int_transformed_44_id
    left join dep_24 on dep_1.int_transformed_73_id = dep_24.stg_source_152_id
    left join dep_25 on dep_1.int_transformed_73_id = dep_25.stg_source_114_id
    left join dep_26 on dep_1.int_transformed_73_id = dep_26.stg_source_36_id
    left join dep_27 on dep_1.int_transformed_73_id = dep_27.stg_source_53_id
    left join dep_28 on dep_1.int_transformed_73_id = dep_28.int_transformed_60_id
    left join dep_29 on dep_1.int_transformed_73_id = dep_29.int_transformed_137_id
    left join dep_30 on dep_1.int_transformed_73_id = dep_30.int_transformed_163_id
    left join dep_31 on dep_1.int_transformed_73_id = dep_31.int_transformed_151_id
    left join dep_32 on dep_1.int_transformed_73_id = dep_32.int_transformed_81_id
    left join dep_33 on dep_1.int_transformed_73_id = dep_33.stg_source_49_id
    left join dep_34 on dep_1.int_transformed_73_id = dep_34.stg_source_160_id
    left join dep_35 on dep_1.int_transformed_73_id = dep_35.stg_source_73_id
    left join dep_36 on dep_1.int_transformed_73_id = dep_36.int_transformed_22_id
    left join dep_37 on dep_1.int_transformed_73_id = dep_37.int_transformed_122_id
    left join dep_38 on dep_1.int_transformed_73_id = dep_38.int_transformed_34_id
    left join dep_39 on dep_1.int_transformed_73_id = dep_39.int_transformed_96_id
    left join dep_40 on dep_1.int_transformed_73_id = dep_40.stg_source_10_id
    left join dep_41 on dep_1.int_transformed_73_id = dep_41.stg_source_68_id
    left join dep_42 on dep_1.int_transformed_73_id = dep_42.stg_source_165_id
    left join dep_43 on dep_1.int_transformed_73_id = dep_43.stg_source_136_id
    left join dep_44 on dep_1.int_transformed_73_id = dep_44.int_transformed_90_id
    left join dep_45 on dep_1.int_transformed_73_id = dep_45.int_transformed_114_id
    left join dep_46 on dep_1.int_transformed_73_id = dep_46.int_transformed_50_id
    left join dep_47 on dep_1.int_transformed_73_id = dep_47.stg_source_112_id
    left join dep_48 on dep_1.int_transformed_73_id = dep_48.int_transformed_4_id
    left join dep_49 on dep_1.int_transformed_73_id = dep_49.int_transformed_116_id
    left join dep_50 on dep_1.int_transformed_73_id = dep_50.int_transformed_72_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
