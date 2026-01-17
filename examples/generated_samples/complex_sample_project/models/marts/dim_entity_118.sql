{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_157') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_5 as (
    select * from {{ ref('stg_source_78') }}
),

dep_6 as (
    select * from {{ ref('stg_source_24') }}
),

dep_7 as (
    select * from {{ ref('stg_source_50') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_9 as (
    select * from {{ ref('stg_source_83') }}
),

dep_10 as (
    select * from {{ ref('stg_source_32') }}
),

dep_11 as (
    select * from {{ ref('stg_source_29') }}
),

dep_12 as (
    select * from {{ ref('stg_source_122') }}
),

dep_13 as (
    select * from {{ ref('stg_source_117') }}
),

dep_14 as (
    select * from {{ ref('stg_source_66') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_19 as (
    select * from {{ ref('stg_source_82') }}
),

dep_20 as (
    select * from {{ ref('stg_source_59') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_25 as (
    select * from {{ ref('stg_source_25') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_30 as (
    select * from {{ ref('stg_source_27') }}
),

dep_31 as (
    select * from {{ ref('stg_source_22') }}
),

dep_32 as (
    select * from {{ ref('stg_source_125') }}
),

dep_33 as (
    select * from {{ ref('stg_source_52') }}
),

dep_34 as (
    select * from {{ ref('stg_source_113') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_38 as (
    select * from {{ ref('stg_source_94') }}
),

dep_39 as (
    select * from {{ ref('stg_source_143') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_5') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_157_id']) }} as surrogate_id,
        dep_1.stg_source_157_id as dim_entity_118_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_157_id = dep_2.int_transformed_52_id
    left join dep_3 on dep_1.stg_source_157_id = dep_3.int_transformed_47_id
    left join dep_4 on dep_1.stg_source_157_id = dep_4.int_transformed_163_id
    left join dep_5 on dep_1.stg_source_157_id = dep_5.stg_source_78_id
    left join dep_6 on dep_1.stg_source_157_id = dep_6.stg_source_24_id
    left join dep_7 on dep_1.stg_source_157_id = dep_7.stg_source_50_id
    left join dep_8 on dep_1.stg_source_157_id = dep_8.int_transformed_73_id
    left join dep_9 on dep_1.stg_source_157_id = dep_9.stg_source_83_id
    left join dep_10 on dep_1.stg_source_157_id = dep_10.stg_source_32_id
    left join dep_11 on dep_1.stg_source_157_id = dep_11.stg_source_29_id
    left join dep_12 on dep_1.stg_source_157_id = dep_12.stg_source_122_id
    left join dep_13 on dep_1.stg_source_157_id = dep_13.stg_source_117_id
    left join dep_14 on dep_1.stg_source_157_id = dep_14.stg_source_66_id
    left join dep_15 on dep_1.stg_source_157_id = dep_15.int_transformed_84_id
    left join dep_16 on dep_1.stg_source_157_id = dep_16.int_transformed_164_id
    left join dep_17 on dep_1.stg_source_157_id = dep_17.int_transformed_148_id
    left join dep_18 on dep_1.stg_source_157_id = dep_18.int_transformed_127_id
    left join dep_19 on dep_1.stg_source_157_id = dep_19.stg_source_82_id
    left join dep_20 on dep_1.stg_source_157_id = dep_20.stg_source_59_id
    left join dep_21 on dep_1.stg_source_157_id = dep_21.int_transformed_153_id
    left join dep_22 on dep_1.stg_source_157_id = dep_22.int_transformed_90_id
    left join dep_23 on dep_1.stg_source_157_id = dep_23.int_transformed_131_id
    left join dep_24 on dep_1.stg_source_157_id = dep_24.int_transformed_162_id
    left join dep_25 on dep_1.stg_source_157_id = dep_25.stg_source_25_id
    left join dep_26 on dep_1.stg_source_157_id = dep_26.int_transformed_88_id
    left join dep_27 on dep_1.stg_source_157_id = dep_27.int_transformed_94_id
    left join dep_28 on dep_1.stg_source_157_id = dep_28.int_transformed_111_id
    left join dep_29 on dep_1.stg_source_157_id = dep_29.int_transformed_106_id
    left join dep_30 on dep_1.stg_source_157_id = dep_30.stg_source_27_id
    left join dep_31 on dep_1.stg_source_157_id = dep_31.stg_source_22_id
    left join dep_32 on dep_1.stg_source_157_id = dep_32.stg_source_125_id
    left join dep_33 on dep_1.stg_source_157_id = dep_33.stg_source_52_id
    left join dep_34 on dep_1.stg_source_157_id = dep_34.stg_source_113_id
    left join dep_35 on dep_1.stg_source_157_id = dep_35.int_transformed_117_id
    left join dep_36 on dep_1.stg_source_157_id = dep_36.int_transformed_157_id
    left join dep_37 on dep_1.stg_source_157_id = dep_37.int_transformed_160_id
    left join dep_38 on dep_1.stg_source_157_id = dep_38.stg_source_94_id
    left join dep_39 on dep_1.stg_source_157_id = dep_39.stg_source_143_id
    left join dep_40 on dep_1.stg_source_157_id = dep_40.int_transformed_124_id
    left join dep_41 on dep_1.stg_source_157_id = dep_41.int_transformed_49_id
    left join dep_42 on dep_1.stg_source_157_id = dep_42.int_transformed_77_id
    left join dep_43 on dep_1.stg_source_157_id = dep_43.int_transformed_91_id
    left join dep_44 on dep_1.stg_source_157_id = dep_44.int_transformed_93_id
    left join dep_45 on dep_1.stg_source_157_id = dep_45.int_transformed_67_id
    left join dep_46 on dep_1.stg_source_157_id = dep_46.int_transformed_138_id
    left join dep_47 on dep_1.stg_source_157_id = dep_47.int_transformed_64_id
    left join dep_48 on dep_1.stg_source_157_id = dep_48.int_transformed_92_id
    left join dep_49 on dep_1.stg_source_157_id = dep_49.int_transformed_116_id
    left join dep_50 on dep_1.stg_source_157_id = dep_50.int_transformed_5_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
