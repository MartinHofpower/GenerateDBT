{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_5 as (
    select * from {{ ref('stg_source_1') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_7 as (
    select * from {{ ref('stg_source_138') }}
),

dep_8 as (
    select * from {{ ref('stg_source_22') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_10 as (
    select * from {{ ref('stg_source_117') }}
),

dep_11 as (
    select * from {{ ref('stg_source_139') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_13 as (
    select * from {{ ref('stg_source_54') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_15 as (
    select * from {{ ref('stg_source_111') }}
),

dep_16 as (
    select * from {{ ref('stg_source_48') }}
),

dep_17 as (
    select * from {{ ref('stg_source_45') }}
),

dep_18 as (
    select * from {{ ref('stg_source_84') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_20 as (
    select * from {{ ref('stg_source_163') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_23 as (
    select * from {{ ref('stg_source_155') }}
),

dep_24 as (
    select * from {{ ref('stg_source_106') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_26 as (
    select * from {{ ref('stg_source_29') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_34 as (
    select * from {{ ref('stg_source_60') }}
),

dep_35 as (
    select * from {{ ref('stg_source_118') }}
),

dep_36 as (
    select * from {{ ref('stg_source_130') }}
),

dep_37 as (
    select * from {{ ref('stg_source_159') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_39 as (
    select * from {{ ref('stg_source_94') }}
),

dep_40 as (
    select * from {{ ref('stg_source_63') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_43 as (
    select * from {{ ref('stg_source_115') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_45 as (
    select * from {{ ref('stg_source_91') }}
),

dep_46 as (
    select * from {{ ref('stg_source_90') }}
),

dep_47 as (
    select * from {{ ref('stg_source_52') }}
),

dep_48 as (
    select * from {{ ref('stg_source_92') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_107') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_26_id']) }} as surrogate_id,
        dep_1.int_transformed_26_id as fct_business_event_137_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_26_id = dep_2.int_transformed_155_id
    left join dep_3 on dep_1.int_transformed_26_id = dep_3.int_transformed_99_id
    left join dep_4 on dep_1.int_transformed_26_id = dep_4.int_transformed_106_id
    left join dep_5 on dep_1.int_transformed_26_id = dep_5.stg_source_1_id
    left join dep_6 on dep_1.int_transformed_26_id = dep_6.int_transformed_2_id
    left join dep_7 on dep_1.int_transformed_26_id = dep_7.stg_source_138_id
    left join dep_8 on dep_1.int_transformed_26_id = dep_8.stg_source_22_id
    left join dep_9 on dep_1.int_transformed_26_id = dep_9.int_transformed_67_id
    left join dep_10 on dep_1.int_transformed_26_id = dep_10.stg_source_117_id
    left join dep_11 on dep_1.int_transformed_26_id = dep_11.stg_source_139_id
    left join dep_12 on dep_1.int_transformed_26_id = dep_12.int_transformed_113_id
    left join dep_13 on dep_1.int_transformed_26_id = dep_13.stg_source_54_id
    left join dep_14 on dep_1.int_transformed_26_id = dep_14.int_transformed_46_id
    left join dep_15 on dep_1.int_transformed_26_id = dep_15.stg_source_111_id
    left join dep_16 on dep_1.int_transformed_26_id = dep_16.stg_source_48_id
    left join dep_17 on dep_1.int_transformed_26_id = dep_17.stg_source_45_id
    left join dep_18 on dep_1.int_transformed_26_id = dep_18.stg_source_84_id
    left join dep_19 on dep_1.int_transformed_26_id = dep_19.int_transformed_116_id
    left join dep_20 on dep_1.int_transformed_26_id = dep_20.stg_source_163_id
    left join dep_21 on dep_1.int_transformed_26_id = dep_21.int_transformed_17_id
    left join dep_22 on dep_1.int_transformed_26_id = dep_22.int_transformed_117_id
    left join dep_23 on dep_1.int_transformed_26_id = dep_23.stg_source_155_id
    left join dep_24 on dep_1.int_transformed_26_id = dep_24.stg_source_106_id
    left join dep_25 on dep_1.int_transformed_26_id = dep_25.int_transformed_93_id
    left join dep_26 on dep_1.int_transformed_26_id = dep_26.stg_source_29_id
    left join dep_27 on dep_1.int_transformed_26_id = dep_27.int_transformed_28_id
    left join dep_28 on dep_1.int_transformed_26_id = dep_28.int_transformed_114_id
    left join dep_29 on dep_1.int_transformed_26_id = dep_29.int_transformed_139_id
    left join dep_30 on dep_1.int_transformed_26_id = dep_30.int_transformed_40_id
    left join dep_31 on dep_1.int_transformed_26_id = dep_31.int_transformed_52_id
    left join dep_32 on dep_1.int_transformed_26_id = dep_32.int_transformed_49_id
    left join dep_33 on dep_1.int_transformed_26_id = dep_33.int_transformed_11_id
    left join dep_34 on dep_1.int_transformed_26_id = dep_34.stg_source_60_id
    left join dep_35 on dep_1.int_transformed_26_id = dep_35.stg_source_118_id
    left join dep_36 on dep_1.int_transformed_26_id = dep_36.stg_source_130_id
    left join dep_37 on dep_1.int_transformed_26_id = dep_37.stg_source_159_id
    left join dep_38 on dep_1.int_transformed_26_id = dep_38.int_transformed_150_id
    left join dep_39 on dep_1.int_transformed_26_id = dep_39.stg_source_94_id
    left join dep_40 on dep_1.int_transformed_26_id = dep_40.stg_source_63_id
    left join dep_41 on dep_1.int_transformed_26_id = dep_41.int_transformed_137_id
    left join dep_42 on dep_1.int_transformed_26_id = dep_42.int_transformed_90_id
    left join dep_43 on dep_1.int_transformed_26_id = dep_43.stg_source_115_id
    left join dep_44 on dep_1.int_transformed_26_id = dep_44.int_transformed_43_id
    left join dep_45 on dep_1.int_transformed_26_id = dep_45.stg_source_91_id
    left join dep_46 on dep_1.int_transformed_26_id = dep_46.stg_source_90_id
    left join dep_47 on dep_1.int_transformed_26_id = dep_47.stg_source_52_id
    left join dep_48 on dep_1.int_transformed_26_id = dep_48.stg_source_92_id
    left join dep_49 on dep_1.int_transformed_26_id = dep_49.int_transformed_130_id
    left join dep_50 on dep_1.int_transformed_26_id = dep_50.int_transformed_107_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
