{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_2 as (
    select * from {{ ref('stg_source_48') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_4 as (
    select * from {{ ref('stg_source_159') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_6 as (
    select * from {{ ref('stg_source_112') }}
),

dep_7 as (
    select * from {{ ref('stg_source_44') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_10 as (
    select * from {{ ref('stg_source_13') }}
),

dep_11 as (
    select * from {{ ref('stg_source_113') }}
),

dep_12 as (
    select * from {{ ref('stg_source_69') }}
),

dep_13 as (
    select * from {{ ref('stg_source_107') }}
),

dep_14 as (
    select * from {{ ref('stg_source_3') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_16 as (
    select * from {{ ref('stg_source_103') }}
),

dep_17 as (
    select * from {{ ref('stg_source_155') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_20 as (
    select * from {{ ref('stg_source_58') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_23 as (
    select * from {{ ref('stg_source_153') }}
),

dep_24 as (
    select * from {{ ref('stg_source_111') }}
),

dep_25 as (
    select * from {{ ref('stg_source_151') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_27 as (
    select * from {{ ref('stg_source_11') }}
),

dep_28 as (
    select * from {{ ref('stg_source_98') }}
),

dep_29 as (
    select * from {{ ref('stg_source_161') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_32 as (
    select * from {{ ref('stg_source_73') }}
),

dep_33 as (
    select * from {{ ref('stg_source_137') }}
),

dep_34 as (
    select * from {{ ref('stg_source_138') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_37 as (
    select * from {{ ref('stg_source_33') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_41 as (
    select * from {{ ref('stg_source_41') }}
),

dep_42 as (
    select * from {{ ref('stg_source_30') }}
),

dep_43 as (
    select * from {{ ref('stg_source_106') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_49 as (
    select * from {{ ref('stg_source_87') }}
),

dep_50 as (
    select * from {{ ref('stg_source_149') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_123_id']) }} as surrogate_id,
        dep_1.int_transformed_123_id as dim_entity_28_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_123_id = dep_2.stg_source_48_id
    left join dep_3 on dep_1.int_transformed_123_id = dep_3.int_transformed_48_id
    left join dep_4 on dep_1.int_transformed_123_id = dep_4.stg_source_159_id
    left join dep_5 on dep_1.int_transformed_123_id = dep_5.int_transformed_39_id
    left join dep_6 on dep_1.int_transformed_123_id = dep_6.stg_source_112_id
    left join dep_7 on dep_1.int_transformed_123_id = dep_7.stg_source_44_id
    left join dep_8 on dep_1.int_transformed_123_id = dep_8.int_transformed_109_id
    left join dep_9 on dep_1.int_transformed_123_id = dep_9.int_transformed_151_id
    left join dep_10 on dep_1.int_transformed_123_id = dep_10.stg_source_13_id
    left join dep_11 on dep_1.int_transformed_123_id = dep_11.stg_source_113_id
    left join dep_12 on dep_1.int_transformed_123_id = dep_12.stg_source_69_id
    left join dep_13 on dep_1.int_transformed_123_id = dep_13.stg_source_107_id
    left join dep_14 on dep_1.int_transformed_123_id = dep_14.stg_source_3_id
    left join dep_15 on dep_1.int_transformed_123_id = dep_15.int_transformed_53_id
    left join dep_16 on dep_1.int_transformed_123_id = dep_16.stg_source_103_id
    left join dep_17 on dep_1.int_transformed_123_id = dep_17.stg_source_155_id
    left join dep_18 on dep_1.int_transformed_123_id = dep_18.int_transformed_62_id
    left join dep_19 on dep_1.int_transformed_123_id = dep_19.int_transformed_155_id
    left join dep_20 on dep_1.int_transformed_123_id = dep_20.stg_source_58_id
    left join dep_21 on dep_1.int_transformed_123_id = dep_21.int_transformed_58_id
    left join dep_22 on dep_1.int_transformed_123_id = dep_22.int_transformed_59_id
    left join dep_23 on dep_1.int_transformed_123_id = dep_23.stg_source_153_id
    left join dep_24 on dep_1.int_transformed_123_id = dep_24.stg_source_111_id
    left join dep_25 on dep_1.int_transformed_123_id = dep_25.stg_source_151_id
    left join dep_26 on dep_1.int_transformed_123_id = dep_26.int_transformed_26_id
    left join dep_27 on dep_1.int_transformed_123_id = dep_27.stg_source_11_id
    left join dep_28 on dep_1.int_transformed_123_id = dep_28.stg_source_98_id
    left join dep_29 on dep_1.int_transformed_123_id = dep_29.stg_source_161_id
    left join dep_30 on dep_1.int_transformed_123_id = dep_30.int_transformed_1_id
    left join dep_31 on dep_1.int_transformed_123_id = dep_31.int_transformed_128_id
    left join dep_32 on dep_1.int_transformed_123_id = dep_32.stg_source_73_id
    left join dep_33 on dep_1.int_transformed_123_id = dep_33.stg_source_137_id
    left join dep_34 on dep_1.int_transformed_123_id = dep_34.stg_source_138_id
    left join dep_35 on dep_1.int_transformed_123_id = dep_35.int_transformed_22_id
    left join dep_36 on dep_1.int_transformed_123_id = dep_36.int_transformed_94_id
    left join dep_37 on dep_1.int_transformed_123_id = dep_37.stg_source_33_id
    left join dep_38 on dep_1.int_transformed_123_id = dep_38.int_transformed_100_id
    left join dep_39 on dep_1.int_transformed_123_id = dep_39.int_transformed_61_id
    left join dep_40 on dep_1.int_transformed_123_id = dep_40.int_transformed_130_id
    left join dep_41 on dep_1.int_transformed_123_id = dep_41.stg_source_41_id
    left join dep_42 on dep_1.int_transformed_123_id = dep_42.stg_source_30_id
    left join dep_43 on dep_1.int_transformed_123_id = dep_43.stg_source_106_id
    left join dep_44 on dep_1.int_transformed_123_id = dep_44.int_transformed_80_id
    left join dep_45 on dep_1.int_transformed_123_id = dep_45.int_transformed_45_id
    left join dep_46 on dep_1.int_transformed_123_id = dep_46.int_transformed_104_id
    left join dep_47 on dep_1.int_transformed_123_id = dep_47.int_transformed_69_id
    left join dep_48 on dep_1.int_transformed_123_id = dep_48.int_transformed_101_id
    left join dep_49 on dep_1.int_transformed_123_id = dep_49.stg_source_87_id
    left join dep_50 on dep_1.int_transformed_123_id = dep_50.stg_source_149_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
