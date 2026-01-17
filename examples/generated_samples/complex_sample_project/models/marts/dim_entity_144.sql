{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_2 as (
    select * from {{ ref('stg_source_97') }}
),

dep_3 as (
    select * from {{ ref('stg_source_87') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_6 as (
    select * from {{ ref('stg_source_160') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_9 as (
    select * from {{ ref('stg_source_140') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_11 as (
    select * from {{ ref('stg_source_102') }}
),

dep_12 as (
    select * from {{ ref('stg_source_152') }}
),

dep_13 as (
    select * from {{ ref('stg_source_13') }}
),

dep_14 as (
    select * from {{ ref('stg_source_62') }}
),

dep_15 as (
    select * from {{ ref('stg_source_69') }}
),

dep_16 as (
    select * from {{ ref('stg_source_40') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_18 as (
    select * from {{ ref('stg_source_31') }}
),

dep_19 as (
    select * from {{ ref('stg_source_46') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_25 as (
    select * from {{ ref('stg_source_145') }}
),

dep_26 as (
    select * from {{ ref('stg_source_54') }}
),

dep_27 as (
    select * from {{ ref('stg_source_149') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_29 as (
    select * from {{ ref('stg_source_45') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_33 as (
    select * from {{ ref('stg_source_88') }}
),

dep_34 as (
    select * from {{ ref('stg_source_109') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_40 as (
    select * from {{ ref('stg_source_153') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_43 as (
    select * from {{ ref('stg_source_129') }}
),

dep_44 as (
    select * from {{ ref('stg_source_136') }}
),

dep_45 as (
    select * from {{ ref('stg_source_19') }}
),

dep_46 as (
    select * from {{ ref('stg_source_1') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_48 as (
    select * from {{ ref('stg_source_86') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_50 as (
    select * from {{ ref('stg_source_51') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_104_id']) }} as surrogate_id,
        dep_1.int_transformed_104_id as dim_entity_144_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_104_id = dep_2.stg_source_97_id
    left join dep_3 on dep_1.int_transformed_104_id = dep_3.stg_source_87_id
    left join dep_4 on dep_1.int_transformed_104_id = dep_4.int_transformed_142_id
    left join dep_5 on dep_1.int_transformed_104_id = dep_5.int_transformed_61_id
    left join dep_6 on dep_1.int_transformed_104_id = dep_6.stg_source_160_id
    left join dep_7 on dep_1.int_transformed_104_id = dep_7.int_transformed_126_id
    left join dep_8 on dep_1.int_transformed_104_id = dep_8.int_transformed_151_id
    left join dep_9 on dep_1.int_transformed_104_id = dep_9.stg_source_140_id
    left join dep_10 on dep_1.int_transformed_104_id = dep_10.int_transformed_84_id
    left join dep_11 on dep_1.int_transformed_104_id = dep_11.stg_source_102_id
    left join dep_12 on dep_1.int_transformed_104_id = dep_12.stg_source_152_id
    left join dep_13 on dep_1.int_transformed_104_id = dep_13.stg_source_13_id
    left join dep_14 on dep_1.int_transformed_104_id = dep_14.stg_source_62_id
    left join dep_15 on dep_1.int_transformed_104_id = dep_15.stg_source_69_id
    left join dep_16 on dep_1.int_transformed_104_id = dep_16.stg_source_40_id
    left join dep_17 on dep_1.int_transformed_104_id = dep_17.int_transformed_65_id
    left join dep_18 on dep_1.int_transformed_104_id = dep_18.stg_source_31_id
    left join dep_19 on dep_1.int_transformed_104_id = dep_19.stg_source_46_id
    left join dep_20 on dep_1.int_transformed_104_id = dep_20.int_transformed_50_id
    left join dep_21 on dep_1.int_transformed_104_id = dep_21.int_transformed_59_id
    left join dep_22 on dep_1.int_transformed_104_id = dep_22.int_transformed_95_id
    left join dep_23 on dep_1.int_transformed_104_id = dep_23.int_transformed_73_id
    left join dep_24 on dep_1.int_transformed_104_id = dep_24.int_transformed_36_id
    left join dep_25 on dep_1.int_transformed_104_id = dep_25.stg_source_145_id
    left join dep_26 on dep_1.int_transformed_104_id = dep_26.stg_source_54_id
    left join dep_27 on dep_1.int_transformed_104_id = dep_27.stg_source_149_id
    left join dep_28 on dep_1.int_transformed_104_id = dep_28.int_transformed_88_id
    left join dep_29 on dep_1.int_transformed_104_id = dep_29.stg_source_45_id
    left join dep_30 on dep_1.int_transformed_104_id = dep_30.int_transformed_132_id
    left join dep_31 on dep_1.int_transformed_104_id = dep_31.int_transformed_7_id
    left join dep_32 on dep_1.int_transformed_104_id = dep_32.int_transformed_109_id
    left join dep_33 on dep_1.int_transformed_104_id = dep_33.stg_source_88_id
    left join dep_34 on dep_1.int_transformed_104_id = dep_34.stg_source_109_id
    left join dep_35 on dep_1.int_transformed_104_id = dep_35.int_transformed_98_id
    left join dep_36 on dep_1.int_transformed_104_id = dep_36.int_transformed_63_id
    left join dep_37 on dep_1.int_transformed_104_id = dep_37.int_transformed_106_id
    left join dep_38 on dep_1.int_transformed_104_id = dep_38.int_transformed_42_id
    left join dep_39 on dep_1.int_transformed_104_id = dep_39.int_transformed_93_id
    left join dep_40 on dep_1.int_transformed_104_id = dep_40.stg_source_153_id
    left join dep_41 on dep_1.int_transformed_104_id = dep_41.int_transformed_4_id
    left join dep_42 on dep_1.int_transformed_104_id = dep_42.int_transformed_162_id
    left join dep_43 on dep_1.int_transformed_104_id = dep_43.stg_source_129_id
    left join dep_44 on dep_1.int_transformed_104_id = dep_44.stg_source_136_id
    left join dep_45 on dep_1.int_transformed_104_id = dep_45.stg_source_19_id
    left join dep_46 on dep_1.int_transformed_104_id = dep_46.stg_source_1_id
    left join dep_47 on dep_1.int_transformed_104_id = dep_47.int_transformed_28_id
    left join dep_48 on dep_1.int_transformed_104_id = dep_48.stg_source_86_id
    left join dep_49 on dep_1.int_transformed_104_id = dep_49.int_transformed_129_id
    left join dep_50 on dep_1.int_transformed_104_id = dep_50.stg_source_51_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
