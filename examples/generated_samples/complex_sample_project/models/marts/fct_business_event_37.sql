{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_92') }}
),

dep_2 as (
    select * from {{ ref('stg_source_57') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_4 as (
    select * from {{ ref('stg_source_154') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_8 as (
    select * from {{ ref('stg_source_46') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_13 as (
    select * from {{ ref('stg_source_36') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_15 as (
    select * from {{ ref('stg_source_44') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_17 as (
    select * from {{ ref('stg_source_47') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_19 as (
    select * from {{ ref('stg_source_135') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_21 as (
    select * from {{ ref('stg_source_63') }}
),

dep_22 as (
    select * from {{ ref('stg_source_160') }}
),

dep_23 as (
    select * from {{ ref('stg_source_54') }}
),

dep_24 as (
    select * from {{ ref('stg_source_146') }}
),

dep_25 as (
    select * from {{ ref('stg_source_8') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_27 as (
    select * from {{ ref('stg_source_123') }}
),

dep_28 as (
    select * from {{ ref('stg_source_127') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_31 as (
    select * from {{ ref('stg_source_156') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_34 as (
    select * from {{ ref('stg_source_7') }}
),

dep_35 as (
    select * from {{ ref('stg_source_69') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_37 as (
    select * from {{ ref('stg_source_77') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_39 as (
    select * from {{ ref('stg_source_108') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_41 as (
    select * from {{ ref('stg_source_65') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_43 as (
    select * from {{ ref('stg_source_12') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_45 as (
    select * from {{ ref('stg_source_115') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_47 as (
    select * from {{ ref('stg_source_98') }}
),

dep_48 as (
    select * from {{ ref('stg_source_79') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_133') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_92_id']) }} as surrogate_id,
        dep_1.stg_source_92_id as fct_business_event_37_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_92_id = dep_2.stg_source_57_id
    left join dep_3 on dep_1.stg_source_92_id = dep_3.int_transformed_162_id
    left join dep_4 on dep_1.stg_source_92_id = dep_4.stg_source_154_id
    left join dep_5 on dep_1.stg_source_92_id = dep_5.int_transformed_101_id
    left join dep_6 on dep_1.stg_source_92_id = dep_6.int_transformed_22_id
    left join dep_7 on dep_1.stg_source_92_id = dep_7.int_transformed_105_id
    left join dep_8 on dep_1.stg_source_92_id = dep_8.stg_source_46_id
    left join dep_9 on dep_1.stg_source_92_id = dep_9.int_transformed_114_id
    left join dep_10 on dep_1.stg_source_92_id = dep_10.int_transformed_78_id
    left join dep_11 on dep_1.stg_source_92_id = dep_11.int_transformed_93_id
    left join dep_12 on dep_1.stg_source_92_id = dep_12.int_transformed_134_id
    left join dep_13 on dep_1.stg_source_92_id = dep_13.stg_source_36_id
    left join dep_14 on dep_1.stg_source_92_id = dep_14.int_transformed_3_id
    left join dep_15 on dep_1.stg_source_92_id = dep_15.stg_source_44_id
    left join dep_16 on dep_1.stg_source_92_id = dep_16.int_transformed_66_id
    left join dep_17 on dep_1.stg_source_92_id = dep_17.stg_source_47_id
    left join dep_18 on dep_1.stg_source_92_id = dep_18.int_transformed_84_id
    left join dep_19 on dep_1.stg_source_92_id = dep_19.stg_source_135_id
    left join dep_20 on dep_1.stg_source_92_id = dep_20.int_transformed_143_id
    left join dep_21 on dep_1.stg_source_92_id = dep_21.stg_source_63_id
    left join dep_22 on dep_1.stg_source_92_id = dep_22.stg_source_160_id
    left join dep_23 on dep_1.stg_source_92_id = dep_23.stg_source_54_id
    left join dep_24 on dep_1.stg_source_92_id = dep_24.stg_source_146_id
    left join dep_25 on dep_1.stg_source_92_id = dep_25.stg_source_8_id
    left join dep_26 on dep_1.stg_source_92_id = dep_26.int_transformed_58_id
    left join dep_27 on dep_1.stg_source_92_id = dep_27.stg_source_123_id
    left join dep_28 on dep_1.stg_source_92_id = dep_28.stg_source_127_id
    left join dep_29 on dep_1.stg_source_92_id = dep_29.int_transformed_122_id
    left join dep_30 on dep_1.stg_source_92_id = dep_30.int_transformed_124_id
    left join dep_31 on dep_1.stg_source_92_id = dep_31.stg_source_156_id
    left join dep_32 on dep_1.stg_source_92_id = dep_32.int_transformed_138_id
    left join dep_33 on dep_1.stg_source_92_id = dep_33.int_transformed_87_id
    left join dep_34 on dep_1.stg_source_92_id = dep_34.stg_source_7_id
    left join dep_35 on dep_1.stg_source_92_id = dep_35.stg_source_69_id
    left join dep_36 on dep_1.stg_source_92_id = dep_36.int_transformed_4_id
    left join dep_37 on dep_1.stg_source_92_id = dep_37.stg_source_77_id
    left join dep_38 on dep_1.stg_source_92_id = dep_38.int_transformed_44_id
    left join dep_39 on dep_1.stg_source_92_id = dep_39.stg_source_108_id
    left join dep_40 on dep_1.stg_source_92_id = dep_40.int_transformed_50_id
    left join dep_41 on dep_1.stg_source_92_id = dep_41.stg_source_65_id
    left join dep_42 on dep_1.stg_source_92_id = dep_42.int_transformed_63_id
    left join dep_43 on dep_1.stg_source_92_id = dep_43.stg_source_12_id
    left join dep_44 on dep_1.stg_source_92_id = dep_44.int_transformed_75_id
    left join dep_45 on dep_1.stg_source_92_id = dep_45.stg_source_115_id
    left join dep_46 on dep_1.stg_source_92_id = dep_46.int_transformed_94_id
    left join dep_47 on dep_1.stg_source_92_id = dep_47.stg_source_98_id
    left join dep_48 on dep_1.stg_source_92_id = dep_48.stg_source_79_id
    left join dep_49 on dep_1.stg_source_92_id = dep_49.int_transformed_116_id
    left join dep_50 on dep_1.stg_source_92_id = dep_50.int_transformed_133_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
