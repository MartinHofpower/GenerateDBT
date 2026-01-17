{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_2 as (
    select * from {{ ref('stg_source_151') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_5 as (
    select * from {{ ref('stg_source_137') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_9 as (
    select * from {{ ref('stg_source_23') }}
),

dep_10 as (
    select * from {{ ref('stg_source_20') }}
),

dep_11 as (
    select * from {{ ref('stg_source_12') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_17 as (
    select * from {{ ref('stg_source_147') }}
),

dep_18 as (
    select * from {{ ref('stg_source_37') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_24 as (
    select * from {{ ref('stg_source_11') }}
),

dep_25 as (
    select * from {{ ref('stg_source_60') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_28 as (
    select * from {{ ref('stg_source_127') }}
),

dep_29 as (
    select * from {{ ref('stg_source_71') }}
),

dep_30 as (
    select * from {{ ref('stg_source_130') }}
),

dep_31 as (
    select * from {{ ref('stg_source_122') }}
),

dep_32 as (
    select * from {{ ref('stg_source_117') }}
),

dep_33 as (
    select * from {{ ref('stg_source_42') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_35 as (
    select * from {{ ref('stg_source_160') }}
),

dep_36 as (
    select * from {{ ref('stg_source_63') }}
),

dep_37 as (
    select * from {{ ref('stg_source_125') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_40 as (
    select * from {{ ref('stg_source_140') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_42 as (
    select * from {{ ref('stg_source_146') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_46 as (
    select * from {{ ref('stg_source_22') }}
),

dep_47 as (
    select * from {{ ref('stg_source_148') }}
),

dep_48 as (
    select * from {{ ref('stg_source_19') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_50 as (
    select * from {{ ref('stg_source_126') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_85_id']) }} as surrogate_id,
        dep_1.int_transformed_85_id as fct_business_event_85_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_85_id = dep_2.stg_source_151_id
    left join dep_3 on dep_1.int_transformed_85_id = dep_3.int_transformed_27_id
    left join dep_4 on dep_1.int_transformed_85_id = dep_4.int_transformed_115_id
    left join dep_5 on dep_1.int_transformed_85_id = dep_5.stg_source_137_id
    left join dep_6 on dep_1.int_transformed_85_id = dep_6.int_transformed_22_id
    left join dep_7 on dep_1.int_transformed_85_id = dep_7.int_transformed_105_id
    left join dep_8 on dep_1.int_transformed_85_id = dep_8.int_transformed_73_id
    left join dep_9 on dep_1.int_transformed_85_id = dep_9.stg_source_23_id
    left join dep_10 on dep_1.int_transformed_85_id = dep_10.stg_source_20_id
    left join dep_11 on dep_1.int_transformed_85_id = dep_11.stg_source_12_id
    left join dep_12 on dep_1.int_transformed_85_id = dep_12.int_transformed_94_id
    left join dep_13 on dep_1.int_transformed_85_id = dep_13.int_transformed_90_id
    left join dep_14 on dep_1.int_transformed_85_id = dep_14.int_transformed_110_id
    left join dep_15 on dep_1.int_transformed_85_id = dep_15.int_transformed_158_id
    left join dep_16 on dep_1.int_transformed_85_id = dep_16.int_transformed_17_id
    left join dep_17 on dep_1.int_transformed_85_id = dep_17.stg_source_147_id
    left join dep_18 on dep_1.int_transformed_85_id = dep_18.stg_source_37_id
    left join dep_19 on dep_1.int_transformed_85_id = dep_19.int_transformed_116_id
    left join dep_20 on dep_1.int_transformed_85_id = dep_20.int_transformed_26_id
    left join dep_21 on dep_1.int_transformed_85_id = dep_21.int_transformed_14_id
    left join dep_22 on dep_1.int_transformed_85_id = dep_22.int_transformed_104_id
    left join dep_23 on dep_1.int_transformed_85_id = dep_23.int_transformed_57_id
    left join dep_24 on dep_1.int_transformed_85_id = dep_24.stg_source_11_id
    left join dep_25 on dep_1.int_transformed_85_id = dep_25.stg_source_60_id
    left join dep_26 on dep_1.int_transformed_85_id = dep_26.int_transformed_59_id
    left join dep_27 on dep_1.int_transformed_85_id = dep_27.int_transformed_72_id
    left join dep_28 on dep_1.int_transformed_85_id = dep_28.stg_source_127_id
    left join dep_29 on dep_1.int_transformed_85_id = dep_29.stg_source_71_id
    left join dep_30 on dep_1.int_transformed_85_id = dep_30.stg_source_130_id
    left join dep_31 on dep_1.int_transformed_85_id = dep_31.stg_source_122_id
    left join dep_32 on dep_1.int_transformed_85_id = dep_32.stg_source_117_id
    left join dep_33 on dep_1.int_transformed_85_id = dep_33.stg_source_42_id
    left join dep_34 on dep_1.int_transformed_85_id = dep_34.int_transformed_142_id
    left join dep_35 on dep_1.int_transformed_85_id = dep_35.stg_source_160_id
    left join dep_36 on dep_1.int_transformed_85_id = dep_36.stg_source_63_id
    left join dep_37 on dep_1.int_transformed_85_id = dep_37.stg_source_125_id
    left join dep_38 on dep_1.int_transformed_85_id = dep_38.int_transformed_111_id
    left join dep_39 on dep_1.int_transformed_85_id = dep_39.int_transformed_121_id
    left join dep_40 on dep_1.int_transformed_85_id = dep_40.stg_source_140_id
    left join dep_41 on dep_1.int_transformed_85_id = dep_41.int_transformed_16_id
    left join dep_42 on dep_1.int_transformed_85_id = dep_42.stg_source_146_id
    left join dep_43 on dep_1.int_transformed_85_id = dep_43.int_transformed_8_id
    left join dep_44 on dep_1.int_transformed_85_id = dep_44.int_transformed_96_id
    left join dep_45 on dep_1.int_transformed_85_id = dep_45.int_transformed_138_id
    left join dep_46 on dep_1.int_transformed_85_id = dep_46.stg_source_22_id
    left join dep_47 on dep_1.int_transformed_85_id = dep_47.stg_source_148_id
    left join dep_48 on dep_1.int_transformed_85_id = dep_48.stg_source_19_id
    left join dep_49 on dep_1.int_transformed_85_id = dep_49.int_transformed_102_id
    left join dep_50 on dep_1.int_transformed_85_id = dep_50.stg_source_126_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
