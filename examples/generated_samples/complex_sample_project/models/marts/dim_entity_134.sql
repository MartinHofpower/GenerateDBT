{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_154') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_4 as (
    select * from {{ ref('stg_source_10') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_6 as (
    select * from {{ ref('stg_source_30') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_8 as (
    select * from {{ ref('stg_source_16') }}
),

dep_9 as (
    select * from {{ ref('stg_source_90') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_11 as (
    select * from {{ ref('stg_source_109') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_15 as (
    select * from {{ ref('stg_source_39') }}
),

dep_16 as (
    select * from {{ ref('stg_source_60') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_20 as (
    select * from {{ ref('stg_source_43') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_22 as (
    select * from {{ ref('stg_source_143') }}
),

dep_23 as (
    select * from {{ ref('stg_source_126') }}
),

dep_24 as (
    select * from {{ ref('stg_source_91') }}
),

dep_25 as (
    select * from {{ ref('stg_source_122') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_27 as (
    select * from {{ ref('stg_source_3') }}
),

dep_28 as (
    select * from {{ ref('stg_source_131') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_30 as (
    select * from {{ ref('stg_source_40') }}
),

dep_31 as (
    select * from {{ ref('stg_source_94') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_21') }}
),

dep_33 as (
    select * from {{ ref('stg_source_68') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_35 as (
    select * from {{ ref('stg_source_161') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_38 as (
    select * from {{ ref('stg_source_102') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_41 as (
    select * from {{ ref('stg_source_92') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_43 as (
    select * from {{ ref('stg_source_124') }}
),

dep_44 as (
    select * from {{ ref('stg_source_7') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_48 as (
    select * from {{ ref('stg_source_9') }}
),

dep_49 as (
    select * from {{ ref('stg_source_136') }}
),

dep_50 as (
    select * from {{ ref('stg_source_50') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_154_id']) }} as surrogate_id,
        dep_1.stg_source_154_id as dim_entity_134_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_154_id = dep_2.int_transformed_13_id
    left join dep_3 on dep_1.stg_source_154_id = dep_3.int_transformed_92_id
    left join dep_4 on dep_1.stg_source_154_id = dep_4.stg_source_10_id
    left join dep_5 on dep_1.stg_source_154_id = dep_5.int_transformed_45_id
    left join dep_6 on dep_1.stg_source_154_id = dep_6.stg_source_30_id
    left join dep_7 on dep_1.stg_source_154_id = dep_7.int_transformed_5_id
    left join dep_8 on dep_1.stg_source_154_id = dep_8.stg_source_16_id
    left join dep_9 on dep_1.stg_source_154_id = dep_9.stg_source_90_id
    left join dep_10 on dep_1.stg_source_154_id = dep_10.int_transformed_4_id
    left join dep_11 on dep_1.stg_source_154_id = dep_11.stg_source_109_id
    left join dep_12 on dep_1.stg_source_154_id = dep_12.int_transformed_144_id
    left join dep_13 on dep_1.stg_source_154_id = dep_13.int_transformed_95_id
    left join dep_14 on dep_1.stg_source_154_id = dep_14.int_transformed_115_id
    left join dep_15 on dep_1.stg_source_154_id = dep_15.stg_source_39_id
    left join dep_16 on dep_1.stg_source_154_id = dep_16.stg_source_60_id
    left join dep_17 on dep_1.stg_source_154_id = dep_17.int_transformed_48_id
    left join dep_18 on dep_1.stg_source_154_id = dep_18.int_transformed_18_id
    left join dep_19 on dep_1.stg_source_154_id = dep_19.int_transformed_152_id
    left join dep_20 on dep_1.stg_source_154_id = dep_20.stg_source_43_id
    left join dep_21 on dep_1.stg_source_154_id = dep_21.int_transformed_148_id
    left join dep_22 on dep_1.stg_source_154_id = dep_22.stg_source_143_id
    left join dep_23 on dep_1.stg_source_154_id = dep_23.stg_source_126_id
    left join dep_24 on dep_1.stg_source_154_id = dep_24.stg_source_91_id
    left join dep_25 on dep_1.stg_source_154_id = dep_25.stg_source_122_id
    left join dep_26 on dep_1.stg_source_154_id = dep_26.int_transformed_31_id
    left join dep_27 on dep_1.stg_source_154_id = dep_27.stg_source_3_id
    left join dep_28 on dep_1.stg_source_154_id = dep_28.stg_source_131_id
    left join dep_29 on dep_1.stg_source_154_id = dep_29.int_transformed_65_id
    left join dep_30 on dep_1.stg_source_154_id = dep_30.stg_source_40_id
    left join dep_31 on dep_1.stg_source_154_id = dep_31.stg_source_94_id
    left join dep_32 on dep_1.stg_source_154_id = dep_32.int_transformed_21_id
    left join dep_33 on dep_1.stg_source_154_id = dep_33.stg_source_68_id
    left join dep_34 on dep_1.stg_source_154_id = dep_34.int_transformed_142_id
    left join dep_35 on dep_1.stg_source_154_id = dep_35.stg_source_161_id
    left join dep_36 on dep_1.stg_source_154_id = dep_36.int_transformed_156_id
    left join dep_37 on dep_1.stg_source_154_id = dep_37.int_transformed_104_id
    left join dep_38 on dep_1.stg_source_154_id = dep_38.stg_source_102_id
    left join dep_39 on dep_1.stg_source_154_id = dep_39.int_transformed_143_id
    left join dep_40 on dep_1.stg_source_154_id = dep_40.int_transformed_42_id
    left join dep_41 on dep_1.stg_source_154_id = dep_41.stg_source_92_id
    left join dep_42 on dep_1.stg_source_154_id = dep_42.int_transformed_79_id
    left join dep_43 on dep_1.stg_source_154_id = dep_43.stg_source_124_id
    left join dep_44 on dep_1.stg_source_154_id = dep_44.stg_source_7_id
    left join dep_45 on dep_1.stg_source_154_id = dep_45.int_transformed_25_id
    left join dep_46 on dep_1.stg_source_154_id = dep_46.int_transformed_81_id
    left join dep_47 on dep_1.stg_source_154_id = dep_47.int_transformed_120_id
    left join dep_48 on dep_1.stg_source_154_id = dep_48.stg_source_9_id
    left join dep_49 on dep_1.stg_source_154_id = dep_49.stg_source_136_id
    left join dep_50 on dep_1.stg_source_154_id = dep_50.stg_source_50_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
