{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_2 as (
    select * from {{ ref('stg_source_46') }}
),

dep_3 as (
    select * from {{ ref('stg_source_73') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_8 as (
    select * from {{ ref('stg_source_114') }}
),

dep_9 as (
    select * from {{ ref('stg_source_18') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_165') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_13 as (
    select * from {{ ref('stg_source_47') }}
),

dep_14 as (
    select * from {{ ref('stg_source_126') }}
),

dep_15 as (
    select * from {{ ref('stg_source_81') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_18 as (
    select * from {{ ref('stg_source_161') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_21 as (
    select * from {{ ref('stg_source_154') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_24 as (
    select * from {{ ref('stg_source_1') }}
),

dep_25 as (
    select * from {{ ref('stg_source_40') }}
),

dep_26 as (
    select * from {{ ref('stg_source_93') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_28 as (
    select * from {{ ref('stg_source_27') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_30 as (
    select * from {{ ref('stg_source_129') }}
),

dep_31 as (
    select * from {{ ref('stg_source_138') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_33 as (
    select * from {{ ref('stg_source_25') }}
),

dep_34 as (
    select * from {{ ref('stg_source_5') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_36 as (
    select * from {{ ref('stg_source_26') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_38 as (
    select * from {{ ref('stg_source_43') }}
),

dep_39 as (
    select * from {{ ref('stg_source_90') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_41 as (
    select * from {{ ref('stg_source_80') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_43 as (
    select * from {{ ref('stg_source_141') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_45 as (
    select * from {{ ref('stg_source_76') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_50 as (
    select * from {{ ref('stg_source_110') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_126_id']) }} as surrogate_id,
        dep_1.int_transformed_126_id as dim_entity_30_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_126_id = dep_2.stg_source_46_id
    left join dep_3 on dep_1.int_transformed_126_id = dep_3.stg_source_73_id
    left join dep_4 on dep_1.int_transformed_126_id = dep_4.int_transformed_91_id
    left join dep_5 on dep_1.int_transformed_126_id = dep_5.int_transformed_145_id
    left join dep_6 on dep_1.int_transformed_126_id = dep_6.int_transformed_79_id
    left join dep_7 on dep_1.int_transformed_126_id = dep_7.int_transformed_87_id
    left join dep_8 on dep_1.int_transformed_126_id = dep_8.stg_source_114_id
    left join dep_9 on dep_1.int_transformed_126_id = dep_9.stg_source_18_id
    left join dep_10 on dep_1.int_transformed_126_id = dep_10.int_transformed_165_id
    left join dep_11 on dep_1.int_transformed_126_id = dep_11.int_transformed_134_id
    left join dep_12 on dep_1.int_transformed_126_id = dep_12.int_transformed_50_id
    left join dep_13 on dep_1.int_transformed_126_id = dep_13.stg_source_47_id
    left join dep_14 on dep_1.int_transformed_126_id = dep_14.stg_source_126_id
    left join dep_15 on dep_1.int_transformed_126_id = dep_15.stg_source_81_id
    left join dep_16 on dep_1.int_transformed_126_id = dep_16.int_transformed_64_id
    left join dep_17 on dep_1.int_transformed_126_id = dep_17.int_transformed_110_id
    left join dep_18 on dep_1.int_transformed_126_id = dep_18.stg_source_161_id
    left join dep_19 on dep_1.int_transformed_126_id = dep_19.int_transformed_132_id
    left join dep_20 on dep_1.int_transformed_126_id = dep_20.int_transformed_146_id
    left join dep_21 on dep_1.int_transformed_126_id = dep_21.stg_source_154_id
    left join dep_22 on dep_1.int_transformed_126_id = dep_22.int_transformed_86_id
    left join dep_23 on dep_1.int_transformed_126_id = dep_23.int_transformed_88_id
    left join dep_24 on dep_1.int_transformed_126_id = dep_24.stg_source_1_id
    left join dep_25 on dep_1.int_transformed_126_id = dep_25.stg_source_40_id
    left join dep_26 on dep_1.int_transformed_126_id = dep_26.stg_source_93_id
    left join dep_27 on dep_1.int_transformed_126_id = dep_27.int_transformed_35_id
    left join dep_28 on dep_1.int_transformed_126_id = dep_28.stg_source_27_id
    left join dep_29 on dep_1.int_transformed_126_id = dep_29.int_transformed_115_id
    left join dep_30 on dep_1.int_transformed_126_id = dep_30.stg_source_129_id
    left join dep_31 on dep_1.int_transformed_126_id = dep_31.stg_source_138_id
    left join dep_32 on dep_1.int_transformed_126_id = dep_32.int_transformed_117_id
    left join dep_33 on dep_1.int_transformed_126_id = dep_33.stg_source_25_id
    left join dep_34 on dep_1.int_transformed_126_id = dep_34.stg_source_5_id
    left join dep_35 on dep_1.int_transformed_126_id = dep_35.int_transformed_65_id
    left join dep_36 on dep_1.int_transformed_126_id = dep_36.stg_source_26_id
    left join dep_37 on dep_1.int_transformed_126_id = dep_37.int_transformed_89_id
    left join dep_38 on dep_1.int_transformed_126_id = dep_38.stg_source_43_id
    left join dep_39 on dep_1.int_transformed_126_id = dep_39.stg_source_90_id
    left join dep_40 on dep_1.int_transformed_126_id = dep_40.int_transformed_156_id
    left join dep_41 on dep_1.int_transformed_126_id = dep_41.stg_source_80_id
    left join dep_42 on dep_1.int_transformed_126_id = dep_42.int_transformed_54_id
    left join dep_43 on dep_1.int_transformed_126_id = dep_43.stg_source_141_id
    left join dep_44 on dep_1.int_transformed_126_id = dep_44.int_transformed_80_id
    left join dep_45 on dep_1.int_transformed_126_id = dep_45.stg_source_76_id
    left join dep_46 on dep_1.int_transformed_126_id = dep_46.int_transformed_13_id
    left join dep_47 on dep_1.int_transformed_126_id = dep_47.int_transformed_1_id
    left join dep_48 on dep_1.int_transformed_126_id = dep_48.int_transformed_63_id
    left join dep_49 on dep_1.int_transformed_126_id = dep_49.int_transformed_73_id
    left join dep_50 on dep_1.int_transformed_126_id = dep_50.stg_source_110_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
