{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_4 as (
    select * from {{ ref('stg_source_164') }}
),

dep_5 as (
    select * from {{ ref('stg_source_120') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_11 as (
    select * from {{ ref('stg_source_13') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_13 as (
    select * from {{ ref('stg_source_129') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_15 as (
    select * from {{ ref('stg_source_119') }}
),

dep_16 as (
    select * from {{ ref('stg_source_148') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_23 as (
    select * from {{ ref('stg_source_157') }}
),

dep_24 as (
    select * from {{ ref('stg_source_138') }}
),

dep_25 as (
    select * from {{ ref('stg_source_17') }}
),

dep_26 as (
    select * from {{ ref('stg_source_107') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_28 as (
    select * from {{ ref('stg_source_6') }}
),

dep_29 as (
    select * from {{ ref('stg_source_9') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_32 as (
    select * from {{ ref('stg_source_108') }}
),

dep_33 as (
    select * from {{ ref('stg_source_90') }}
),

dep_34 as (
    select * from {{ ref('stg_source_8') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_36 as (
    select * from {{ ref('stg_source_154') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_40 as (
    select * from {{ ref('stg_source_14') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_44 as (
    select * from {{ ref('stg_source_59') }}
),

dep_45 as (
    select * from {{ ref('stg_source_114') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_65') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_7_id']) }} as surrogate_id,
        dep_1.int_transformed_7_id as fct_business_event_127_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_7_id = dep_2.int_transformed_111_id
    left join dep_3 on dep_1.int_transformed_7_id = dep_3.int_transformed_152_id
    left join dep_4 on dep_1.int_transformed_7_id = dep_4.stg_source_164_id
    left join dep_5 on dep_1.int_transformed_7_id = dep_5.stg_source_120_id
    left join dep_6 on dep_1.int_transformed_7_id = dep_6.int_transformed_99_id
    left join dep_7 on dep_1.int_transformed_7_id = dep_7.int_transformed_22_id
    left join dep_8 on dep_1.int_transformed_7_id = dep_8.int_transformed_107_id
    left join dep_9 on dep_1.int_transformed_7_id = dep_9.int_transformed_72_id
    left join dep_10 on dep_1.int_transformed_7_id = dep_10.int_transformed_114_id
    left join dep_11 on dep_1.int_transformed_7_id = dep_11.stg_source_13_id
    left join dep_12 on dep_1.int_transformed_7_id = dep_12.int_transformed_123_id
    left join dep_13 on dep_1.int_transformed_7_id = dep_13.stg_source_129_id
    left join dep_14 on dep_1.int_transformed_7_id = dep_14.int_transformed_80_id
    left join dep_15 on dep_1.int_transformed_7_id = dep_15.stg_source_119_id
    left join dep_16 on dep_1.int_transformed_7_id = dep_16.stg_source_148_id
    left join dep_17 on dep_1.int_transformed_7_id = dep_17.int_transformed_45_id
    left join dep_18 on dep_1.int_transformed_7_id = dep_18.int_transformed_144_id
    left join dep_19 on dep_1.int_transformed_7_id = dep_19.int_transformed_115_id
    left join dep_20 on dep_1.int_transformed_7_id = dep_20.int_transformed_2_id
    left join dep_21 on dep_1.int_transformed_7_id = dep_21.int_transformed_84_id
    left join dep_22 on dep_1.int_transformed_7_id = dep_22.int_transformed_129_id
    left join dep_23 on dep_1.int_transformed_7_id = dep_23.stg_source_157_id
    left join dep_24 on dep_1.int_transformed_7_id = dep_24.stg_source_138_id
    left join dep_25 on dep_1.int_transformed_7_id = dep_25.stg_source_17_id
    left join dep_26 on dep_1.int_transformed_7_id = dep_26.stg_source_107_id
    left join dep_27 on dep_1.int_transformed_7_id = dep_27.int_transformed_164_id
    left join dep_28 on dep_1.int_transformed_7_id = dep_28.stg_source_6_id
    left join dep_29 on dep_1.int_transformed_7_id = dep_29.stg_source_9_id
    left join dep_30 on dep_1.int_transformed_7_id = dep_30.int_transformed_90_id
    left join dep_31 on dep_1.int_transformed_7_id = dep_31.int_transformed_98_id
    left join dep_32 on dep_1.int_transformed_7_id = dep_32.stg_source_108_id
    left join dep_33 on dep_1.int_transformed_7_id = dep_33.stg_source_90_id
    left join dep_34 on dep_1.int_transformed_7_id = dep_34.stg_source_8_id
    left join dep_35 on dep_1.int_transformed_7_id = dep_35.int_transformed_157_id
    left join dep_36 on dep_1.int_transformed_7_id = dep_36.stg_source_154_id
    left join dep_37 on dep_1.int_transformed_7_id = dep_37.int_transformed_149_id
    left join dep_38 on dep_1.int_transformed_7_id = dep_38.int_transformed_40_id
    left join dep_39 on dep_1.int_transformed_7_id = dep_39.int_transformed_59_id
    left join dep_40 on dep_1.int_transformed_7_id = dep_40.stg_source_14_id
    left join dep_41 on dep_1.int_transformed_7_id = dep_41.int_transformed_121_id
    left join dep_42 on dep_1.int_transformed_7_id = dep_42.int_transformed_77_id
    left join dep_43 on dep_1.int_transformed_7_id = dep_43.int_transformed_148_id
    left join dep_44 on dep_1.int_transformed_7_id = dep_44.stg_source_59_id
    left join dep_45 on dep_1.int_transformed_7_id = dep_45.stg_source_114_id
    left join dep_46 on dep_1.int_transformed_7_id = dep_46.int_transformed_28_id
    left join dep_47 on dep_1.int_transformed_7_id = dep_47.int_transformed_79_id
    left join dep_48 on dep_1.int_transformed_7_id = dep_48.int_transformed_97_id
    left join dep_49 on dep_1.int_transformed_7_id = dep_49.int_transformed_36_id
    left join dep_50 on dep_1.int_transformed_7_id = dep_50.int_transformed_65_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
