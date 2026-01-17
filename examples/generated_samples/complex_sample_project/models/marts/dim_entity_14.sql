{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_130') }}
),

dep_2 as (
    select * from {{ ref('stg_source_146') }}
),

dep_3 as (
    select * from {{ ref('stg_source_148') }}
),

dep_4 as (
    select * from {{ ref('stg_source_111') }}
),

dep_5 as (
    select * from {{ ref('stg_source_59') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_7 as (
    select * from {{ ref('stg_source_123') }}
),

dep_8 as (
    select * from {{ ref('stg_source_40') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_11 as (
    select * from {{ ref('stg_source_110') }}
),

dep_12 as (
    select * from {{ ref('stg_source_151') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_15 as (
    select * from {{ ref('stg_source_122') }}
),

dep_16 as (
    select * from {{ ref('stg_source_71') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_19 as (
    select * from {{ ref('stg_source_128') }}
),

dep_20 as (
    select * from {{ ref('stg_source_119') }}
),

dep_21 as (
    select * from {{ ref('stg_source_32') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_23 as (
    select * from {{ ref('stg_source_152') }}
),

dep_24 as (
    select * from {{ ref('stg_source_125') }}
),

dep_25 as (
    select * from {{ ref('stg_source_21') }}
),

dep_26 as (
    select * from {{ ref('stg_source_31') }}
),

dep_27 as (
    select * from {{ ref('stg_source_124') }}
),

dep_28 as (
    select * from {{ ref('stg_source_64') }}
),

dep_29 as (
    select * from {{ ref('stg_source_98') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_31 as (
    select * from {{ ref('stg_source_38') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_33 as (
    select * from {{ ref('stg_source_20') }}
),

dep_34 as (
    select * from {{ ref('stg_source_1') }}
),

dep_35 as (
    select * from {{ ref('stg_source_7') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_37 as (
    select * from {{ ref('stg_source_79') }}
),

dep_38 as (
    select * from {{ ref('stg_source_150') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_41 as (
    select * from {{ ref('stg_source_30') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_43 as (
    select * from {{ ref('stg_source_87') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_48 as (
    select * from {{ ref('stg_source_35') }}
),

dep_49 as (
    select * from {{ ref('stg_source_56') }}
),

dep_50 as (
    select * from {{ ref('stg_source_67') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_130_id']) }} as surrogate_id,
        dep_1.stg_source_130_id as dim_entity_14_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_130_id = dep_2.stg_source_146_id
    left join dep_3 on dep_1.stg_source_130_id = dep_3.stg_source_148_id
    left join dep_4 on dep_1.stg_source_130_id = dep_4.stg_source_111_id
    left join dep_5 on dep_1.stg_source_130_id = dep_5.stg_source_59_id
    left join dep_6 on dep_1.stg_source_130_id = dep_6.int_transformed_84_id
    left join dep_7 on dep_1.stg_source_130_id = dep_7.stg_source_123_id
    left join dep_8 on dep_1.stg_source_130_id = dep_8.stg_source_40_id
    left join dep_9 on dep_1.stg_source_130_id = dep_9.int_transformed_6_id
    left join dep_10 on dep_1.stg_source_130_id = dep_10.int_transformed_99_id
    left join dep_11 on dep_1.stg_source_130_id = dep_11.stg_source_110_id
    left join dep_12 on dep_1.stg_source_130_id = dep_12.stg_source_151_id
    left join dep_13 on dep_1.stg_source_130_id = dep_13.int_transformed_158_id
    left join dep_14 on dep_1.stg_source_130_id = dep_14.int_transformed_71_id
    left join dep_15 on dep_1.stg_source_130_id = dep_15.stg_source_122_id
    left join dep_16 on dep_1.stg_source_130_id = dep_16.stg_source_71_id
    left join dep_17 on dep_1.stg_source_130_id = dep_17.int_transformed_108_id
    left join dep_18 on dep_1.stg_source_130_id = dep_18.int_transformed_47_id
    left join dep_19 on dep_1.stg_source_130_id = dep_19.stg_source_128_id
    left join dep_20 on dep_1.stg_source_130_id = dep_20.stg_source_119_id
    left join dep_21 on dep_1.stg_source_130_id = dep_21.stg_source_32_id
    left join dep_22 on dep_1.stg_source_130_id = dep_22.int_transformed_153_id
    left join dep_23 on dep_1.stg_source_130_id = dep_23.stg_source_152_id
    left join dep_24 on dep_1.stg_source_130_id = dep_24.stg_source_125_id
    left join dep_25 on dep_1.stg_source_130_id = dep_25.stg_source_21_id
    left join dep_26 on dep_1.stg_source_130_id = dep_26.stg_source_31_id
    left join dep_27 on dep_1.stg_source_130_id = dep_27.stg_source_124_id
    left join dep_28 on dep_1.stg_source_130_id = dep_28.stg_source_64_id
    left join dep_29 on dep_1.stg_source_130_id = dep_29.stg_source_98_id
    left join dep_30 on dep_1.stg_source_130_id = dep_30.int_transformed_114_id
    left join dep_31 on dep_1.stg_source_130_id = dep_31.stg_source_38_id
    left join dep_32 on dep_1.stg_source_130_id = dep_32.int_transformed_93_id
    left join dep_33 on dep_1.stg_source_130_id = dep_33.stg_source_20_id
    left join dep_34 on dep_1.stg_source_130_id = dep_34.stg_source_1_id
    left join dep_35 on dep_1.stg_source_130_id = dep_35.stg_source_7_id
    left join dep_36 on dep_1.stg_source_130_id = dep_36.int_transformed_54_id
    left join dep_37 on dep_1.stg_source_130_id = dep_37.stg_source_79_id
    left join dep_38 on dep_1.stg_source_130_id = dep_38.stg_source_150_id
    left join dep_39 on dep_1.stg_source_130_id = dep_39.int_transformed_138_id
    left join dep_40 on dep_1.stg_source_130_id = dep_40.int_transformed_3_id
    left join dep_41 on dep_1.stg_source_130_id = dep_41.stg_source_30_id
    left join dep_42 on dep_1.stg_source_130_id = dep_42.int_transformed_100_id
    left join dep_43 on dep_1.stg_source_130_id = dep_43.stg_source_87_id
    left join dep_44 on dep_1.stg_source_130_id = dep_44.int_transformed_25_id
    left join dep_45 on dep_1.stg_source_130_id = dep_45.int_transformed_68_id
    left join dep_46 on dep_1.stg_source_130_id = dep_46.int_transformed_136_id
    left join dep_47 on dep_1.stg_source_130_id = dep_47.int_transformed_57_id
    left join dep_48 on dep_1.stg_source_130_id = dep_48.stg_source_35_id
    left join dep_49 on dep_1.stg_source_130_id = dep_49.stg_source_56_id
    left join dep_50 on dep_1.stg_source_130_id = dep_50.stg_source_67_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
