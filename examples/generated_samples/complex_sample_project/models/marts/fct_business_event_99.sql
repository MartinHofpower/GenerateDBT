{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_149') }}
),

dep_2 as (
    select * from {{ ref('stg_source_31') }}
),

dep_3 as (
    select * from {{ ref('stg_source_33') }}
),

dep_4 as (
    select * from {{ ref('stg_source_90') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_6 as (
    select * from {{ ref('stg_source_60') }}
),

dep_7 as (
    select * from {{ ref('stg_source_119') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_10 as (
    select * from {{ ref('stg_source_59') }}
),

dep_11 as (
    select * from {{ ref('stg_source_41') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_13 as (
    select * from {{ ref('stg_source_67') }}
),

dep_14 as (
    select * from {{ ref('stg_source_99') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_19 as (
    select * from {{ ref('stg_source_13') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_21 as (
    select * from {{ ref('stg_source_10') }}
),

dep_22 as (
    select * from {{ ref('stg_source_101') }}
),

dep_23 as (
    select * from {{ ref('stg_source_105') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_25 as (
    select * from {{ ref('stg_source_19') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_27 as (
    select * from {{ ref('stg_source_129') }}
),

dep_28 as (
    select * from {{ ref('stg_source_111') }}
),

dep_29 as (
    select * from {{ ref('stg_source_1') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_31 as (
    select * from {{ ref('stg_source_77') }}
),

dep_32 as (
    select * from {{ ref('stg_source_110') }}
),

dep_33 as (
    select * from {{ ref('stg_source_164') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_35 as (
    select * from {{ ref('stg_source_157') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_37 as (
    select * from {{ ref('stg_source_147') }}
),

dep_38 as (
    select * from {{ ref('stg_source_114') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_40 as (
    select * from {{ ref('stg_source_96') }}
),

dep_41 as (
    select * from {{ ref('stg_source_83') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_44 as (
    select * from {{ ref('stg_source_158') }}
),

dep_45 as (
    select * from {{ ref('stg_source_130') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_47 as (
    select * from {{ ref('stg_source_142') }}
),

dep_48 as (
    select * from {{ ref('stg_source_65') }}
),

dep_49 as (
    select * from {{ ref('stg_source_134') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_42') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_149_id']) }} as surrogate_id,
        dep_1.stg_source_149_id as fct_business_event_99_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_149_id = dep_2.stg_source_31_id
    left join dep_3 on dep_1.stg_source_149_id = dep_3.stg_source_33_id
    left join dep_4 on dep_1.stg_source_149_id = dep_4.stg_source_90_id
    left join dep_5 on dep_1.stg_source_149_id = dep_5.int_transformed_70_id
    left join dep_6 on dep_1.stg_source_149_id = dep_6.stg_source_60_id
    left join dep_7 on dep_1.stg_source_149_id = dep_7.stg_source_119_id
    left join dep_8 on dep_1.stg_source_149_id = dep_8.int_transformed_118_id
    left join dep_9 on dep_1.stg_source_149_id = dep_9.int_transformed_14_id
    left join dep_10 on dep_1.stg_source_149_id = dep_10.stg_source_59_id
    left join dep_11 on dep_1.stg_source_149_id = dep_11.stg_source_41_id
    left join dep_12 on dep_1.stg_source_149_id = dep_12.int_transformed_57_id
    left join dep_13 on dep_1.stg_source_149_id = dep_13.stg_source_67_id
    left join dep_14 on dep_1.stg_source_149_id = dep_14.stg_source_99_id
    left join dep_15 on dep_1.stg_source_149_id = dep_15.int_transformed_82_id
    left join dep_16 on dep_1.stg_source_149_id = dep_16.int_transformed_19_id
    left join dep_17 on dep_1.stg_source_149_id = dep_17.int_transformed_164_id
    left join dep_18 on dep_1.stg_source_149_id = dep_18.int_transformed_147_id
    left join dep_19 on dep_1.stg_source_149_id = dep_19.stg_source_13_id
    left join dep_20 on dep_1.stg_source_149_id = dep_20.int_transformed_96_id
    left join dep_21 on dep_1.stg_source_149_id = dep_21.stg_source_10_id
    left join dep_22 on dep_1.stg_source_149_id = dep_22.stg_source_101_id
    left join dep_23 on dep_1.stg_source_149_id = dep_23.stg_source_105_id
    left join dep_24 on dep_1.stg_source_149_id = dep_24.int_transformed_126_id
    left join dep_25 on dep_1.stg_source_149_id = dep_25.stg_source_19_id
    left join dep_26 on dep_1.stg_source_149_id = dep_26.int_transformed_33_id
    left join dep_27 on dep_1.stg_source_149_id = dep_27.stg_source_129_id
    left join dep_28 on dep_1.stg_source_149_id = dep_28.stg_source_111_id
    left join dep_29 on dep_1.stg_source_149_id = dep_29.stg_source_1_id
    left join dep_30 on dep_1.stg_source_149_id = dep_30.int_transformed_122_id
    left join dep_31 on dep_1.stg_source_149_id = dep_31.stg_source_77_id
    left join dep_32 on dep_1.stg_source_149_id = dep_32.stg_source_110_id
    left join dep_33 on dep_1.stg_source_149_id = dep_33.stg_source_164_id
    left join dep_34 on dep_1.stg_source_149_id = dep_34.int_transformed_84_id
    left join dep_35 on dep_1.stg_source_149_id = dep_35.stg_source_157_id
    left join dep_36 on dep_1.stg_source_149_id = dep_36.int_transformed_59_id
    left join dep_37 on dep_1.stg_source_149_id = dep_37.stg_source_147_id
    left join dep_38 on dep_1.stg_source_149_id = dep_38.stg_source_114_id
    left join dep_39 on dep_1.stg_source_149_id = dep_39.int_transformed_79_id
    left join dep_40 on dep_1.stg_source_149_id = dep_40.stg_source_96_id
    left join dep_41 on dep_1.stg_source_149_id = dep_41.stg_source_83_id
    left join dep_42 on dep_1.stg_source_149_id = dep_42.int_transformed_97_id
    left join dep_43 on dep_1.stg_source_149_id = dep_43.int_transformed_166_id
    left join dep_44 on dep_1.stg_source_149_id = dep_44.stg_source_158_id
    left join dep_45 on dep_1.stg_source_149_id = dep_45.stg_source_130_id
    left join dep_46 on dep_1.stg_source_149_id = dep_46.int_transformed_72_id
    left join dep_47 on dep_1.stg_source_149_id = dep_47.stg_source_142_id
    left join dep_48 on dep_1.stg_source_149_id = dep_48.stg_source_65_id
    left join dep_49 on dep_1.stg_source_149_id = dep_49.stg_source_134_id
    left join dep_50 on dep_1.stg_source_149_id = dep_50.int_transformed_42_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
