{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_7') }}
),

dep_2 as (
    select * from {{ ref('stg_source_101') }}
),

dep_3 as (
    select * from {{ ref('stg_source_134') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_5 as (
    select * from {{ ref('stg_source_143') }}
),

dep_6 as (
    select * from {{ ref('stg_source_18') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_8 as (
    select * from {{ ref('stg_source_9') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_10 as (
    select * from {{ ref('stg_source_125') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_18 as (
    select * from {{ ref('stg_source_99') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_20 as (
    select * from {{ ref('stg_source_31') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_22 as (
    select * from {{ ref('stg_source_98') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_24 as (
    select * from {{ ref('stg_source_56') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_28 as (
    select * from {{ ref('stg_source_60') }}
),

dep_29 as (
    select * from {{ ref('stg_source_52') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_32 as (
    select * from {{ ref('stg_source_80') }}
),

dep_33 as (
    select * from {{ ref('stg_source_102') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_35 as (
    select * from {{ ref('stg_source_122') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_38 as (
    select * from {{ ref('stg_source_146') }}
),

dep_39 as (
    select * from {{ ref('stg_source_77') }}
),

dep_40 as (
    select * from {{ ref('stg_source_1') }}
),

dep_41 as (
    select * from {{ ref('stg_source_36') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_43 as (
    select * from {{ ref('stg_source_110') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_46 as (
    select * from {{ ref('stg_source_159') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_49 as (
    select * from {{ ref('stg_source_67') }}
),

dep_50 as (
    select * from {{ ref('stg_source_53') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_7_id']) }} as surrogate_id,
        dep_1.stg_source_7_id as dim_entity_74_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_7_id = dep_2.stg_source_101_id
    left join dep_3 on dep_1.stg_source_7_id = dep_3.stg_source_134_id
    left join dep_4 on dep_1.stg_source_7_id = dep_4.int_transformed_133_id
    left join dep_5 on dep_1.stg_source_7_id = dep_5.stg_source_143_id
    left join dep_6 on dep_1.stg_source_7_id = dep_6.stg_source_18_id
    left join dep_7 on dep_1.stg_source_7_id = dep_7.int_transformed_164_id
    left join dep_8 on dep_1.stg_source_7_id = dep_8.stg_source_9_id
    left join dep_9 on dep_1.stg_source_7_id = dep_9.int_transformed_98_id
    left join dep_10 on dep_1.stg_source_7_id = dep_10.stg_source_125_id
    left join dep_11 on dep_1.stg_source_7_id = dep_11.int_transformed_42_id
    left join dep_12 on dep_1.stg_source_7_id = dep_12.int_transformed_152_id
    left join dep_13 on dep_1.stg_source_7_id = dep_13.int_transformed_147_id
    left join dep_14 on dep_1.stg_source_7_id = dep_14.int_transformed_124_id
    left join dep_15 on dep_1.stg_source_7_id = dep_15.int_transformed_138_id
    left join dep_16 on dep_1.stg_source_7_id = dep_16.int_transformed_142_id
    left join dep_17 on dep_1.stg_source_7_id = dep_17.int_transformed_163_id
    left join dep_18 on dep_1.stg_source_7_id = dep_18.stg_source_99_id
    left join dep_19 on dep_1.stg_source_7_id = dep_19.int_transformed_135_id
    left join dep_20 on dep_1.stg_source_7_id = dep_20.stg_source_31_id
    left join dep_21 on dep_1.stg_source_7_id = dep_21.int_transformed_15_id
    left join dep_22 on dep_1.stg_source_7_id = dep_22.stg_source_98_id
    left join dep_23 on dep_1.stg_source_7_id = dep_23.int_transformed_56_id
    left join dep_24 on dep_1.stg_source_7_id = dep_24.stg_source_56_id
    left join dep_25 on dep_1.stg_source_7_id = dep_25.int_transformed_140_id
    left join dep_26 on dep_1.stg_source_7_id = dep_26.int_transformed_74_id
    left join dep_27 on dep_1.stg_source_7_id = dep_27.int_transformed_38_id
    left join dep_28 on dep_1.stg_source_7_id = dep_28.stg_source_60_id
    left join dep_29 on dep_1.stg_source_7_id = dep_29.stg_source_52_id
    left join dep_30 on dep_1.stg_source_7_id = dep_30.int_transformed_2_id
    left join dep_31 on dep_1.stg_source_7_id = dep_31.int_transformed_69_id
    left join dep_32 on dep_1.stg_source_7_id = dep_32.stg_source_80_id
    left join dep_33 on dep_1.stg_source_7_id = dep_33.stg_source_102_id
    left join dep_34 on dep_1.stg_source_7_id = dep_34.int_transformed_61_id
    left join dep_35 on dep_1.stg_source_7_id = dep_35.stg_source_122_id
    left join dep_36 on dep_1.stg_source_7_id = dep_36.int_transformed_83_id
    left join dep_37 on dep_1.stg_source_7_id = dep_37.int_transformed_52_id
    left join dep_38 on dep_1.stg_source_7_id = dep_38.stg_source_146_id
    left join dep_39 on dep_1.stg_source_7_id = dep_39.stg_source_77_id
    left join dep_40 on dep_1.stg_source_7_id = dep_40.stg_source_1_id
    left join dep_41 on dep_1.stg_source_7_id = dep_41.stg_source_36_id
    left join dep_42 on dep_1.stg_source_7_id = dep_42.int_transformed_8_id
    left join dep_43 on dep_1.stg_source_7_id = dep_43.stg_source_110_id
    left join dep_44 on dep_1.stg_source_7_id = dep_44.int_transformed_76_id
    left join dep_45 on dep_1.stg_source_7_id = dep_45.int_transformed_33_id
    left join dep_46 on dep_1.stg_source_7_id = dep_46.stg_source_159_id
    left join dep_47 on dep_1.stg_source_7_id = dep_47.int_transformed_97_id
    left join dep_48 on dep_1.stg_source_7_id = dep_48.int_transformed_158_id
    left join dep_49 on dep_1.stg_source_7_id = dep_49.stg_source_67_id
    left join dep_50 on dep_1.stg_source_7_id = dep_50.stg_source_53_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
