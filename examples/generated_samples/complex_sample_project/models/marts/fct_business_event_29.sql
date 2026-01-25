{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_49') }}
),

dep_2 as (
    select * from {{ ref('stg_source_61') }}
),

dep_3 as (
    select * from {{ ref('stg_source_115') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_5 as (
    select * from {{ ref('stg_source_23') }}
),

dep_6 as (
    select * from {{ ref('stg_source_68') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_9 as (
    select * from {{ ref('stg_source_113') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_13 as (
    select * from {{ ref('stg_source_73') }}
),

dep_14 as (
    select * from {{ ref('stg_source_44') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_16 as (
    select * from {{ ref('stg_source_101') }}
),

dep_17 as (
    select * from {{ ref('stg_source_36') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_20 as (
    select * from {{ ref('stg_source_120') }}
),

dep_21 as (
    select * from {{ ref('stg_source_87') }}
),

dep_22 as (
    select * from {{ ref('stg_source_30') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_24 as (
    select * from {{ ref('stg_source_147') }}
),

dep_25 as (
    select * from {{ ref('stg_source_84') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_27 as (
    select * from {{ ref('stg_source_72') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_30 as (
    select * from {{ ref('stg_source_126') }}
),

dep_31 as (
    select * from {{ ref('stg_source_7') }}
),

dep_32 as (
    select * from {{ ref('stg_source_18') }}
),

dep_33 as (
    select * from {{ ref('stg_source_131') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_36 as (
    select * from {{ ref('stg_source_9') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_38 as (
    select * from {{ ref('stg_source_130') }}
),

dep_39 as (
    select * from {{ ref('stg_source_164') }}
),

dep_40 as (
    select * from {{ ref('stg_source_132') }}
),

dep_41 as (
    select * from {{ ref('stg_source_16') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_43 as (
    select * from {{ ref('stg_source_81') }}
),

dep_44 as (
    select * from {{ ref('stg_source_88') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_46 as (
    select * from {{ ref('stg_source_107') }}
),

dep_47 as (
    select * from {{ ref('stg_source_125') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_50 as (
    select * from {{ ref('stg_source_27') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_49_id']) }} as surrogate_id,
        dep_1.stg_source_49_id as fct_business_event_29_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_49_id = dep_2.stg_source_61_id
    left join dep_3 on dep_1.stg_source_49_id = dep_3.stg_source_115_id
    left join dep_4 on dep_1.stg_source_49_id = dep_4.int_transformed_87_id
    left join dep_5 on dep_1.stg_source_49_id = dep_5.stg_source_23_id
    left join dep_6 on dep_1.stg_source_49_id = dep_6.stg_source_68_id
    left join dep_7 on dep_1.stg_source_49_id = dep_7.int_transformed_43_id
    left join dep_8 on dep_1.stg_source_49_id = dep_8.int_transformed_122_id
    left join dep_9 on dep_1.stg_source_49_id = dep_9.stg_source_113_id
    left join dep_10 on dep_1.stg_source_49_id = dep_10.int_transformed_160_id
    left join dep_11 on dep_1.stg_source_49_id = dep_11.int_transformed_15_id
    left join dep_12 on dep_1.stg_source_49_id = dep_12.int_transformed_63_id
    left join dep_13 on dep_1.stg_source_49_id = dep_13.stg_source_73_id
    left join dep_14 on dep_1.stg_source_49_id = dep_14.stg_source_44_id
    left join dep_15 on dep_1.stg_source_49_id = dep_15.int_transformed_41_id
    left join dep_16 on dep_1.stg_source_49_id = dep_16.stg_source_101_id
    left join dep_17 on dep_1.stg_source_49_id = dep_17.stg_source_36_id
    left join dep_18 on dep_1.stg_source_49_id = dep_18.int_transformed_163_id
    left join dep_19 on dep_1.stg_source_49_id = dep_19.int_transformed_147_id
    left join dep_20 on dep_1.stg_source_49_id = dep_20.stg_source_120_id
    left join dep_21 on dep_1.stg_source_49_id = dep_21.stg_source_87_id
    left join dep_22 on dep_1.stg_source_49_id = dep_22.stg_source_30_id
    left join dep_23 on dep_1.stg_source_49_id = dep_23.int_transformed_52_id
    left join dep_24 on dep_1.stg_source_49_id = dep_24.stg_source_147_id
    left join dep_25 on dep_1.stg_source_49_id = dep_25.stg_source_84_id
    left join dep_26 on dep_1.stg_source_49_id = dep_26.int_transformed_54_id
    left join dep_27 on dep_1.stg_source_49_id = dep_27.stg_source_72_id
    left join dep_28 on dep_1.stg_source_49_id = dep_28.int_transformed_14_id
    left join dep_29 on dep_1.stg_source_49_id = dep_29.int_transformed_46_id
    left join dep_30 on dep_1.stg_source_49_id = dep_30.stg_source_126_id
    left join dep_31 on dep_1.stg_source_49_id = dep_31.stg_source_7_id
    left join dep_32 on dep_1.stg_source_49_id = dep_32.stg_source_18_id
    left join dep_33 on dep_1.stg_source_49_id = dep_33.stg_source_131_id
    left join dep_34 on dep_1.stg_source_49_id = dep_34.int_transformed_82_id
    left join dep_35 on dep_1.stg_source_49_id = dep_35.int_transformed_108_id
    left join dep_36 on dep_1.stg_source_49_id = dep_36.stg_source_9_id
    left join dep_37 on dep_1.stg_source_49_id = dep_37.int_transformed_136_id
    left join dep_38 on dep_1.stg_source_49_id = dep_38.stg_source_130_id
    left join dep_39 on dep_1.stg_source_49_id = dep_39.stg_source_164_id
    left join dep_40 on dep_1.stg_source_49_id = dep_40.stg_source_132_id
    left join dep_41 on dep_1.stg_source_49_id = dep_41.stg_source_16_id
    left join dep_42 on dep_1.stg_source_49_id = dep_42.int_transformed_74_id
    left join dep_43 on dep_1.stg_source_49_id = dep_43.stg_source_81_id
    left join dep_44 on dep_1.stg_source_49_id = dep_44.stg_source_88_id
    left join dep_45 on dep_1.stg_source_49_id = dep_45.int_transformed_154_id
    left join dep_46 on dep_1.stg_source_49_id = dep_46.stg_source_107_id
    left join dep_47 on dep_1.stg_source_49_id = dep_47.stg_source_125_id
    left join dep_48 on dep_1.stg_source_49_id = dep_48.int_transformed_50_id
    left join dep_49 on dep_1.stg_source_49_id = dep_49.int_transformed_102_id
    left join dep_50 on dep_1.stg_source_49_id = dep_50.stg_source_27_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
