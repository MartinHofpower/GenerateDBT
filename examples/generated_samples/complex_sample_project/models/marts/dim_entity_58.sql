{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_2 as (
    select * from {{ ref('stg_source_98') }}
),

dep_3 as (
    select * from {{ ref('stg_source_1') }}
),

dep_4 as (
    select * from {{ ref('stg_source_139') }}
),

dep_5 as (
    select * from {{ ref('stg_source_54') }}
),

dep_6 as (
    select * from {{ ref('stg_source_25') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_8 as (
    select * from {{ ref('stg_source_13') }}
),

dep_9 as (
    select * from {{ ref('stg_source_100') }}
),

dep_10 as (
    select * from {{ ref('stg_source_36') }}
),

dep_11 as (
    select * from {{ ref('stg_source_166') }}
),

dep_12 as (
    select * from {{ ref('stg_source_106') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_14 as (
    select * from {{ ref('stg_source_97') }}
),

dep_15 as (
    select * from {{ ref('stg_source_115') }}
),

dep_16 as (
    select * from {{ ref('stg_source_127') }}
),

dep_17 as (
    select * from {{ ref('stg_source_108') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_20 as (
    select * from {{ ref('stg_source_155') }}
),

dep_21 as (
    select * from {{ ref('stg_source_51') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_25 as (
    select * from {{ ref('stg_source_87') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_28 as (
    select * from {{ ref('stg_source_99') }}
),

dep_29 as (
    select * from {{ ref('stg_source_146') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_33 as (
    select * from {{ ref('stg_source_59') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_36 as (
    select * from {{ ref('stg_source_119') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_21') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_47 as (
    select * from {{ ref('stg_source_165') }}
),

dep_48 as (
    select * from {{ ref('stg_source_88') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_50 as (
    select * from {{ ref('stg_source_94') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_132_id']) }} as surrogate_id,
        dep_1.int_transformed_132_id as dim_entity_58_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_132_id = dep_2.stg_source_98_id
    left join dep_3 on dep_1.int_transformed_132_id = dep_3.stg_source_1_id
    left join dep_4 on dep_1.int_transformed_132_id = dep_4.stg_source_139_id
    left join dep_5 on dep_1.int_transformed_132_id = dep_5.stg_source_54_id
    left join dep_6 on dep_1.int_transformed_132_id = dep_6.stg_source_25_id
    left join dep_7 on dep_1.int_transformed_132_id = dep_7.int_transformed_15_id
    left join dep_8 on dep_1.int_transformed_132_id = dep_8.stg_source_13_id
    left join dep_9 on dep_1.int_transformed_132_id = dep_9.stg_source_100_id
    left join dep_10 on dep_1.int_transformed_132_id = dep_10.stg_source_36_id
    left join dep_11 on dep_1.int_transformed_132_id = dep_11.stg_source_166_id
    left join dep_12 on dep_1.int_transformed_132_id = dep_12.stg_source_106_id
    left join dep_13 on dep_1.int_transformed_132_id = dep_13.int_transformed_29_id
    left join dep_14 on dep_1.int_transformed_132_id = dep_14.stg_source_97_id
    left join dep_15 on dep_1.int_transformed_132_id = dep_15.stg_source_115_id
    left join dep_16 on dep_1.int_transformed_132_id = dep_16.stg_source_127_id
    left join dep_17 on dep_1.int_transformed_132_id = dep_17.stg_source_108_id
    left join dep_18 on dep_1.int_transformed_132_id = dep_18.int_transformed_98_id
    left join dep_19 on dep_1.int_transformed_132_id = dep_19.int_transformed_71_id
    left join dep_20 on dep_1.int_transformed_132_id = dep_20.stg_source_155_id
    left join dep_21 on dep_1.int_transformed_132_id = dep_21.stg_source_51_id
    left join dep_22 on dep_1.int_transformed_132_id = dep_22.int_transformed_58_id
    left join dep_23 on dep_1.int_transformed_132_id = dep_23.int_transformed_22_id
    left join dep_24 on dep_1.int_transformed_132_id = dep_24.int_transformed_24_id
    left join dep_25 on dep_1.int_transformed_132_id = dep_25.stg_source_87_id
    left join dep_26 on dep_1.int_transformed_132_id = dep_26.int_transformed_110_id
    left join dep_27 on dep_1.int_transformed_132_id = dep_27.int_transformed_142_id
    left join dep_28 on dep_1.int_transformed_132_id = dep_28.stg_source_99_id
    left join dep_29 on dep_1.int_transformed_132_id = dep_29.stg_source_146_id
    left join dep_30 on dep_1.int_transformed_132_id = dep_30.int_transformed_26_id
    left join dep_31 on dep_1.int_transformed_132_id = dep_31.int_transformed_144_id
    left join dep_32 on dep_1.int_transformed_132_id = dep_32.int_transformed_128_id
    left join dep_33 on dep_1.int_transformed_132_id = dep_33.stg_source_59_id
    left join dep_34 on dep_1.int_transformed_132_id = dep_34.int_transformed_106_id
    left join dep_35 on dep_1.int_transformed_132_id = dep_35.int_transformed_85_id
    left join dep_36 on dep_1.int_transformed_132_id = dep_36.stg_source_119_id
    left join dep_37 on dep_1.int_transformed_132_id = dep_37.int_transformed_134_id
    left join dep_38 on dep_1.int_transformed_132_id = dep_38.int_transformed_59_id
    left join dep_39 on dep_1.int_transformed_132_id = dep_39.int_transformed_7_id
    left join dep_40 on dep_1.int_transformed_132_id = dep_40.int_transformed_47_id
    left join dep_41 on dep_1.int_transformed_132_id = dep_41.int_transformed_21_id
    left join dep_42 on dep_1.int_transformed_132_id = dep_42.int_transformed_86_id
    left join dep_43 on dep_1.int_transformed_132_id = dep_43.int_transformed_164_id
    left join dep_44 on dep_1.int_transformed_132_id = dep_44.int_transformed_155_id
    left join dep_45 on dep_1.int_transformed_132_id = dep_45.int_transformed_96_id
    left join dep_46 on dep_1.int_transformed_132_id = dep_46.int_transformed_95_id
    left join dep_47 on dep_1.int_transformed_132_id = dep_47.stg_source_165_id
    left join dep_48 on dep_1.int_transformed_132_id = dep_48.stg_source_88_id
    left join dep_49 on dep_1.int_transformed_132_id = dep_49.int_transformed_126_id
    left join dep_50 on dep_1.int_transformed_132_id = dep_50.stg_source_94_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
