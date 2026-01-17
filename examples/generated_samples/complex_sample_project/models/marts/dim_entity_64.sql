{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_29') }}
),

dep_2 as (
    select * from {{ ref('stg_source_95') }}
),

dep_3 as (
    select * from {{ ref('stg_source_33') }}
),

dep_4 as (
    select * from {{ ref('stg_source_15') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_7 as (
    select * from {{ ref('stg_source_108') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_11 as (
    select * from {{ ref('stg_source_104') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_14 as (
    select * from {{ ref('stg_source_53') }}
),

dep_15 as (
    select * from {{ ref('stg_source_79') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_19 as (
    select * from {{ ref('stg_source_114') }}
),

dep_20 as (
    select * from {{ ref('stg_source_140') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_26 as (
    select * from {{ ref('stg_source_19') }}
),

dep_27 as (
    select * from {{ ref('stg_source_131') }}
),

dep_28 as (
    select * from {{ ref('stg_source_68') }}
),

dep_29 as (
    select * from {{ ref('stg_source_130') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_31 as (
    select * from {{ ref('stg_source_23') }}
),

dep_32 as (
    select * from {{ ref('stg_source_67') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_34 as (
    select * from {{ ref('stg_source_4') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_37 as (
    select * from {{ ref('stg_source_46') }}
),

dep_38 as (
    select * from {{ ref('stg_source_147') }}
),

dep_39 as (
    select * from {{ ref('stg_source_90') }}
),

dep_40 as (
    select * from {{ ref('stg_source_110') }}
),

dep_41 as (
    select * from {{ ref('stg_source_103') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_44 as (
    select * from {{ ref('stg_source_14') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_49 as (
    select * from {{ ref('stg_source_17') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_159') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_29_id']) }} as surrogate_id,
        dep_1.stg_source_29_id as dim_entity_64_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_29_id = dep_2.stg_source_95_id
    left join dep_3 on dep_1.stg_source_29_id = dep_3.stg_source_33_id
    left join dep_4 on dep_1.stg_source_29_id = dep_4.stg_source_15_id
    left join dep_5 on dep_1.stg_source_29_id = dep_5.int_transformed_51_id
    left join dep_6 on dep_1.stg_source_29_id = dep_6.int_transformed_84_id
    left join dep_7 on dep_1.stg_source_29_id = dep_7.stg_source_108_id
    left join dep_8 on dep_1.stg_source_29_id = dep_8.int_transformed_108_id
    left join dep_9 on dep_1.stg_source_29_id = dep_9.int_transformed_121_id
    left join dep_10 on dep_1.stg_source_29_id = dep_10.int_transformed_20_id
    left join dep_11 on dep_1.stg_source_29_id = dep_11.stg_source_104_id
    left join dep_12 on dep_1.stg_source_29_id = dep_12.int_transformed_127_id
    left join dep_13 on dep_1.stg_source_29_id = dep_13.int_transformed_61_id
    left join dep_14 on dep_1.stg_source_29_id = dep_14.stg_source_53_id
    left join dep_15 on dep_1.stg_source_29_id = dep_15.stg_source_79_id
    left join dep_16 on dep_1.stg_source_29_id = dep_16.int_transformed_45_id
    left join dep_17 on dep_1.stg_source_29_id = dep_17.int_transformed_163_id
    left join dep_18 on dep_1.stg_source_29_id = dep_18.int_transformed_103_id
    left join dep_19 on dep_1.stg_source_29_id = dep_19.stg_source_114_id
    left join dep_20 on dep_1.stg_source_29_id = dep_20.stg_source_140_id
    left join dep_21 on dep_1.stg_source_29_id = dep_21.int_transformed_70_id
    left join dep_22 on dep_1.stg_source_29_id = dep_22.int_transformed_13_id
    left join dep_23 on dep_1.stg_source_29_id = dep_23.int_transformed_144_id
    left join dep_24 on dep_1.stg_source_29_id = dep_24.int_transformed_5_id
    left join dep_25 on dep_1.stg_source_29_id = dep_25.int_transformed_16_id
    left join dep_26 on dep_1.stg_source_29_id = dep_26.stg_source_19_id
    left join dep_27 on dep_1.stg_source_29_id = dep_27.stg_source_131_id
    left join dep_28 on dep_1.stg_source_29_id = dep_28.stg_source_68_id
    left join dep_29 on dep_1.stg_source_29_id = dep_29.stg_source_130_id
    left join dep_30 on dep_1.stg_source_29_id = dep_30.int_transformed_164_id
    left join dep_31 on dep_1.stg_source_29_id = dep_31.stg_source_23_id
    left join dep_32 on dep_1.stg_source_29_id = dep_32.stg_source_67_id
    left join dep_33 on dep_1.stg_source_29_id = dep_33.int_transformed_115_id
    left join dep_34 on dep_1.stg_source_29_id = dep_34.stg_source_4_id
    left join dep_35 on dep_1.stg_source_29_id = dep_35.int_transformed_32_id
    left join dep_36 on dep_1.stg_source_29_id = dep_36.int_transformed_60_id
    left join dep_37 on dep_1.stg_source_29_id = dep_37.stg_source_46_id
    left join dep_38 on dep_1.stg_source_29_id = dep_38.stg_source_147_id
    left join dep_39 on dep_1.stg_source_29_id = dep_39.stg_source_90_id
    left join dep_40 on dep_1.stg_source_29_id = dep_40.stg_source_110_id
    left join dep_41 on dep_1.stg_source_29_id = dep_41.stg_source_103_id
    left join dep_42 on dep_1.stg_source_29_id = dep_42.int_transformed_143_id
    left join dep_43 on dep_1.stg_source_29_id = dep_43.int_transformed_133_id
    left join dep_44 on dep_1.stg_source_29_id = dep_44.stg_source_14_id
    left join dep_45 on dep_1.stg_source_29_id = dep_45.int_transformed_25_id
    left join dep_46 on dep_1.stg_source_29_id = dep_46.int_transformed_43_id
    left join dep_47 on dep_1.stg_source_29_id = dep_47.int_transformed_97_id
    left join dep_48 on dep_1.stg_source_29_id = dep_48.int_transformed_142_id
    left join dep_49 on dep_1.stg_source_29_id = dep_49.stg_source_17_id
    left join dep_50 on dep_1.stg_source_29_id = dep_50.int_transformed_159_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
