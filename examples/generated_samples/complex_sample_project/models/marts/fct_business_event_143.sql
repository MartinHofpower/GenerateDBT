{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_3 as (
    select * from {{ ref('stg_source_118') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_5 as (
    select * from {{ ref('stg_source_103') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_8 as (
    select * from {{ ref('stg_source_42') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_10 as (
    select * from {{ ref('stg_source_142') }}
),

dep_11 as (
    select * from {{ ref('stg_source_146') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_16 as (
    select * from {{ ref('stg_source_39') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_18 as (
    select * from {{ ref('stg_source_65') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_20 as (
    select * from {{ ref('stg_source_159') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_24 as (
    select * from {{ ref('stg_source_35') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_83') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_28 as (
    select * from {{ ref('stg_source_122') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_31 as (
    select * from {{ ref('stg_source_10') }}
),

dep_32 as (
    select * from {{ ref('stg_source_147') }}
),

dep_33 as (
    select * from {{ ref('stg_source_8') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_36 as (
    select * from {{ ref('stg_source_2') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_165') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_3') }}
),

dep_45 as (
    select * from {{ ref('stg_source_48') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_47 as (
    select * from {{ ref('stg_source_83') }}
),

dep_48 as (
    select * from {{ ref('stg_source_1') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_50 as (
    select * from {{ ref('stg_source_121') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_159_id']) }} as surrogate_id,
        dep_1.int_transformed_159_id as fct_business_event_143_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_159_id = dep_2.int_transformed_39_id
    left join dep_3 on dep_1.int_transformed_159_id = dep_3.stg_source_118_id
    left join dep_4 on dep_1.int_transformed_159_id = dep_4.int_transformed_42_id
    left join dep_5 on dep_1.int_transformed_159_id = dep_5.stg_source_103_id
    left join dep_6 on dep_1.int_transformed_159_id = dep_6.int_transformed_114_id
    left join dep_7 on dep_1.int_transformed_159_id = dep_7.int_transformed_106_id
    left join dep_8 on dep_1.int_transformed_159_id = dep_8.stg_source_42_id
    left join dep_9 on dep_1.int_transformed_159_id = dep_9.int_transformed_27_id
    left join dep_10 on dep_1.int_transformed_159_id = dep_10.stg_source_142_id
    left join dep_11 on dep_1.int_transformed_159_id = dep_11.stg_source_146_id
    left join dep_12 on dep_1.int_transformed_159_id = dep_12.int_transformed_118_id
    left join dep_13 on dep_1.int_transformed_159_id = dep_13.int_transformed_138_id
    left join dep_14 on dep_1.int_transformed_159_id = dep_14.int_transformed_45_id
    left join dep_15 on dep_1.int_transformed_159_id = dep_15.int_transformed_139_id
    left join dep_16 on dep_1.int_transformed_159_id = dep_16.stg_source_39_id
    left join dep_17 on dep_1.int_transformed_159_id = dep_17.int_transformed_137_id
    left join dep_18 on dep_1.int_transformed_159_id = dep_18.stg_source_65_id
    left join dep_19 on dep_1.int_transformed_159_id = dep_19.int_transformed_150_id
    left join dep_20 on dep_1.int_transformed_159_id = dep_20.stg_source_159_id
    left join dep_21 on dep_1.int_transformed_159_id = dep_21.int_transformed_161_id
    left join dep_22 on dep_1.int_transformed_159_id = dep_22.int_transformed_82_id
    left join dep_23 on dep_1.int_transformed_159_id = dep_23.int_transformed_107_id
    left join dep_24 on dep_1.int_transformed_159_id = dep_24.stg_source_35_id
    left join dep_25 on dep_1.int_transformed_159_id = dep_25.int_transformed_83_id
    left join dep_26 on dep_1.int_transformed_159_id = dep_26.int_transformed_109_id
    left join dep_27 on dep_1.int_transformed_159_id = dep_27.int_transformed_56_id
    left join dep_28 on dep_1.int_transformed_159_id = dep_28.stg_source_122_id
    left join dep_29 on dep_1.int_transformed_159_id = dep_29.int_transformed_40_id
    left join dep_30 on dep_1.int_transformed_159_id = dep_30.int_transformed_20_id
    left join dep_31 on dep_1.int_transformed_159_id = dep_31.stg_source_10_id
    left join dep_32 on dep_1.int_transformed_159_id = dep_32.stg_source_147_id
    left join dep_33 on dep_1.int_transformed_159_id = dep_33.stg_source_8_id
    left join dep_34 on dep_1.int_transformed_159_id = dep_34.int_transformed_48_id
    left join dep_35 on dep_1.int_transformed_159_id = dep_35.int_transformed_149_id
    left join dep_36 on dep_1.int_transformed_159_id = dep_36.stg_source_2_id
    left join dep_37 on dep_1.int_transformed_159_id = dep_37.int_transformed_165_id
    left join dep_38 on dep_1.int_transformed_159_id = dep_38.int_transformed_130_id
    left join dep_39 on dep_1.int_transformed_159_id = dep_39.int_transformed_152_id
    left join dep_40 on dep_1.int_transformed_159_id = dep_40.int_transformed_30_id
    left join dep_41 on dep_1.int_transformed_159_id = dep_41.int_transformed_112_id
    left join dep_42 on dep_1.int_transformed_159_id = dep_42.int_transformed_100_id
    left join dep_43 on dep_1.int_transformed_159_id = dep_43.int_transformed_148_id
    left join dep_44 on dep_1.int_transformed_159_id = dep_44.int_transformed_3_id
    left join dep_45 on dep_1.int_transformed_159_id = dep_45.stg_source_48_id
    left join dep_46 on dep_1.int_transformed_159_id = dep_46.int_transformed_12_id
    left join dep_47 on dep_1.int_transformed_159_id = dep_47.stg_source_83_id
    left join dep_48 on dep_1.int_transformed_159_id = dep_48.stg_source_1_id
    left join dep_49 on dep_1.int_transformed_159_id = dep_49.int_transformed_37_id
    left join dep_50 on dep_1.int_transformed_159_id = dep_50.stg_source_121_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
