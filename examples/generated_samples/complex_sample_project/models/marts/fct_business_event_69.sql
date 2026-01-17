{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_27') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_3 as (
    select * from {{ ref('stg_source_57') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_5 as (
    select * from {{ ref('stg_source_129') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_7 as (
    select * from {{ ref('stg_source_91') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_10 as (
    select * from {{ ref('stg_source_116') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_14 as (
    select * from {{ ref('stg_source_137') }}
),

dep_15 as (
    select * from {{ ref('stg_source_71') }}
),

dep_16 as (
    select * from {{ ref('stg_source_98') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_19 as (
    select * from {{ ref('stg_source_132') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_22 as (
    select * from {{ ref('stg_source_72') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_27 as (
    select * from {{ ref('stg_source_114') }}
),

dep_28 as (
    select * from {{ ref('stg_source_149') }}
),

dep_29 as (
    select * from {{ ref('stg_source_45') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_32 as (
    select * from {{ ref('stg_source_130') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_38 as (
    select * from {{ ref('stg_source_113') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_42 as (
    select * from {{ ref('stg_source_105') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_44 as (
    select * from {{ ref('stg_source_90') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_106') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_27_id']) }} as surrogate_id,
        dep_1.stg_source_27_id as fct_business_event_69_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_27_id = dep_2.int_transformed_72_id
    left join dep_3 on dep_1.stg_source_27_id = dep_3.stg_source_57_id
    left join dep_4 on dep_1.stg_source_27_id = dep_4.int_transformed_90_id
    left join dep_5 on dep_1.stg_source_27_id = dep_5.stg_source_129_id
    left join dep_6 on dep_1.stg_source_27_id = dep_6.int_transformed_5_id
    left join dep_7 on dep_1.stg_source_27_id = dep_7.stg_source_91_id
    left join dep_8 on dep_1.stg_source_27_id = dep_8.int_transformed_19_id
    left join dep_9 on dep_1.stg_source_27_id = dep_9.int_transformed_57_id
    left join dep_10 on dep_1.stg_source_27_id = dep_10.stg_source_116_id
    left join dep_11 on dep_1.stg_source_27_id = dep_11.int_transformed_113_id
    left join dep_12 on dep_1.stg_source_27_id = dep_12.int_transformed_145_id
    left join dep_13 on dep_1.stg_source_27_id = dep_13.int_transformed_75_id
    left join dep_14 on dep_1.stg_source_27_id = dep_14.stg_source_137_id
    left join dep_15 on dep_1.stg_source_27_id = dep_15.stg_source_71_id
    left join dep_16 on dep_1.stg_source_27_id = dep_16.stg_source_98_id
    left join dep_17 on dep_1.stg_source_27_id = dep_17.int_transformed_2_id
    left join dep_18 on dep_1.stg_source_27_id = dep_18.int_transformed_31_id
    left join dep_19 on dep_1.stg_source_27_id = dep_19.stg_source_132_id
    left join dep_20 on dep_1.stg_source_27_id = dep_20.int_transformed_38_id
    left join dep_21 on dep_1.stg_source_27_id = dep_21.int_transformed_9_id
    left join dep_22 on dep_1.stg_source_27_id = dep_22.stg_source_72_id
    left join dep_23 on dep_1.stg_source_27_id = dep_23.int_transformed_132_id
    left join dep_24 on dep_1.stg_source_27_id = dep_24.int_transformed_123_id
    left join dep_25 on dep_1.stg_source_27_id = dep_25.int_transformed_64_id
    left join dep_26 on dep_1.stg_source_27_id = dep_26.int_transformed_41_id
    left join dep_27 on dep_1.stg_source_27_id = dep_27.stg_source_114_id
    left join dep_28 on dep_1.stg_source_27_id = dep_28.stg_source_149_id
    left join dep_29 on dep_1.stg_source_27_id = dep_29.stg_source_45_id
    left join dep_30 on dep_1.stg_source_27_id = dep_30.int_transformed_152_id
    left join dep_31 on dep_1.stg_source_27_id = dep_31.int_transformed_162_id
    left join dep_32 on dep_1.stg_source_27_id = dep_32.stg_source_130_id
    left join dep_33 on dep_1.stg_source_27_id = dep_33.int_transformed_110_id
    left join dep_34 on dep_1.stg_source_27_id = dep_34.int_transformed_14_id
    left join dep_35 on dep_1.stg_source_27_id = dep_35.int_transformed_35_id
    left join dep_36 on dep_1.stg_source_27_id = dep_36.int_transformed_126_id
    left join dep_37 on dep_1.stg_source_27_id = dep_37.int_transformed_32_id
    left join dep_38 on dep_1.stg_source_27_id = dep_38.stg_source_113_id
    left join dep_39 on dep_1.stg_source_27_id = dep_39.int_transformed_36_id
    left join dep_40 on dep_1.stg_source_27_id = dep_40.int_transformed_11_id
    left join dep_41 on dep_1.stg_source_27_id = dep_41.int_transformed_147_id
    left join dep_42 on dep_1.stg_source_27_id = dep_42.stg_source_105_id
    left join dep_43 on dep_1.stg_source_27_id = dep_43.int_transformed_65_id
    left join dep_44 on dep_1.stg_source_27_id = dep_44.stg_source_90_id
    left join dep_45 on dep_1.stg_source_27_id = dep_45.int_transformed_133_id
    left join dep_46 on dep_1.stg_source_27_id = dep_46.int_transformed_23_id
    left join dep_47 on dep_1.stg_source_27_id = dep_47.int_transformed_54_id
    left join dep_48 on dep_1.stg_source_27_id = dep_48.int_transformed_78_id
    left join dep_49 on dep_1.stg_source_27_id = dep_49.int_transformed_99_id
    left join dep_50 on dep_1.stg_source_27_id = dep_50.int_transformed_106_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
