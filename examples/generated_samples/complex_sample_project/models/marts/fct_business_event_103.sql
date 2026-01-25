{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_151') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_3 as (
    select * from {{ ref('stg_source_137') }}
),

dep_4 as (
    select * from {{ ref('stg_source_76') }}
),

dep_5 as (
    select * from {{ ref('stg_source_103') }}
),

dep_6 as (
    select * from {{ ref('stg_source_4') }}
),

dep_7 as (
    select * from {{ ref('stg_source_134') }}
),

dep_8 as (
    select * from {{ ref('stg_source_158') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_10 as (
    select * from {{ ref('stg_source_123') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_14 as (
    select * from {{ ref('stg_source_15') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_16 as (
    select * from {{ ref('stg_source_70') }}
),

dep_17 as (
    select * from {{ ref('stg_source_144') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_19 as (
    select * from {{ ref('stg_source_33') }}
),

dep_20 as (
    select * from {{ ref('stg_source_73') }}
),

dep_21 as (
    select * from {{ ref('stg_source_121') }}
),

dep_22 as (
    select * from {{ ref('stg_source_82') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_26 as (
    select * from {{ ref('stg_source_136') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_28 as (
    select * from {{ ref('stg_source_17') }}
),

dep_29 as (
    select * from {{ ref('stg_source_102') }}
),

dep_30 as (
    select * from {{ ref('stg_source_11') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_33 as (
    select * from {{ ref('stg_source_133') }}
),

dep_34 as (
    select * from {{ ref('stg_source_16') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_38 as (
    select * from {{ ref('stg_source_23') }}
),

dep_39 as (
    select * from {{ ref('stg_source_2') }}
),

dep_40 as (
    select * from {{ ref('stg_source_14') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_42 as (
    select * from {{ ref('stg_source_92') }}
),

dep_43 as (
    select * from {{ ref('stg_source_108') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_49 as (
    select * from {{ ref('stg_source_93') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_82') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_151_id']) }} as surrogate_id,
        dep_1.stg_source_151_id as fct_business_event_103_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_151_id = dep_2.int_transformed_125_id
    left join dep_3 on dep_1.stg_source_151_id = dep_3.stg_source_137_id
    left join dep_4 on dep_1.stg_source_151_id = dep_4.stg_source_76_id
    left join dep_5 on dep_1.stg_source_151_id = dep_5.stg_source_103_id
    left join dep_6 on dep_1.stg_source_151_id = dep_6.stg_source_4_id
    left join dep_7 on dep_1.stg_source_151_id = dep_7.stg_source_134_id
    left join dep_8 on dep_1.stg_source_151_id = dep_8.stg_source_158_id
    left join dep_9 on dep_1.stg_source_151_id = dep_9.int_transformed_101_id
    left join dep_10 on dep_1.stg_source_151_id = dep_10.stg_source_123_id
    left join dep_11 on dep_1.stg_source_151_id = dep_11.int_transformed_127_id
    left join dep_12 on dep_1.stg_source_151_id = dep_12.int_transformed_123_id
    left join dep_13 on dep_1.stg_source_151_id = dep_13.int_transformed_114_id
    left join dep_14 on dep_1.stg_source_151_id = dep_14.stg_source_15_id
    left join dep_15 on dep_1.stg_source_151_id = dep_15.int_transformed_113_id
    left join dep_16 on dep_1.stg_source_151_id = dep_16.stg_source_70_id
    left join dep_17 on dep_1.stg_source_151_id = dep_17.stg_source_144_id
    left join dep_18 on dep_1.stg_source_151_id = dep_18.int_transformed_143_id
    left join dep_19 on dep_1.stg_source_151_id = dep_19.stg_source_33_id
    left join dep_20 on dep_1.stg_source_151_id = dep_20.stg_source_73_id
    left join dep_21 on dep_1.stg_source_151_id = dep_21.stg_source_121_id
    left join dep_22 on dep_1.stg_source_151_id = dep_22.stg_source_82_id
    left join dep_23 on dep_1.stg_source_151_id = dep_23.int_transformed_17_id
    left join dep_24 on dep_1.stg_source_151_id = dep_24.int_transformed_90_id
    left join dep_25 on dep_1.stg_source_151_id = dep_25.int_transformed_129_id
    left join dep_26 on dep_1.stg_source_151_id = dep_26.stg_source_136_id
    left join dep_27 on dep_1.stg_source_151_id = dep_27.int_transformed_144_id
    left join dep_28 on dep_1.stg_source_151_id = dep_28.stg_source_17_id
    left join dep_29 on dep_1.stg_source_151_id = dep_29.stg_source_102_id
    left join dep_30 on dep_1.stg_source_151_id = dep_30.stg_source_11_id
    left join dep_31 on dep_1.stg_source_151_id = dep_31.int_transformed_92_id
    left join dep_32 on dep_1.stg_source_151_id = dep_32.int_transformed_106_id
    left join dep_33 on dep_1.stg_source_151_id = dep_33.stg_source_133_id
    left join dep_34 on dep_1.stg_source_151_id = dep_34.stg_source_16_id
    left join dep_35 on dep_1.stg_source_151_id = dep_35.int_transformed_107_id
    left join dep_36 on dep_1.stg_source_151_id = dep_36.int_transformed_26_id
    left join dep_37 on dep_1.stg_source_151_id = dep_37.int_transformed_38_id
    left join dep_38 on dep_1.stg_source_151_id = dep_38.stg_source_23_id
    left join dep_39 on dep_1.stg_source_151_id = dep_39.stg_source_2_id
    left join dep_40 on dep_1.stg_source_151_id = dep_40.stg_source_14_id
    left join dep_41 on dep_1.stg_source_151_id = dep_41.int_transformed_99_id
    left join dep_42 on dep_1.stg_source_151_id = dep_42.stg_source_92_id
    left join dep_43 on dep_1.stg_source_151_id = dep_43.stg_source_108_id
    left join dep_44 on dep_1.stg_source_151_id = dep_44.int_transformed_73_id
    left join dep_45 on dep_1.stg_source_151_id = dep_45.int_transformed_124_id
    left join dep_46 on dep_1.stg_source_151_id = dep_46.int_transformed_120_id
    left join dep_47 on dep_1.stg_source_151_id = dep_47.int_transformed_88_id
    left join dep_48 on dep_1.stg_source_151_id = dep_48.int_transformed_20_id
    left join dep_49 on dep_1.stg_source_151_id = dep_49.stg_source_93_id
    left join dep_50 on dep_1.stg_source_151_id = dep_50.int_transformed_82_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
