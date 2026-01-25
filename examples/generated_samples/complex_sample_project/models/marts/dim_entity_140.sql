{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_3 as (
    select * from {{ ref('stg_source_53') }}
),

dep_4 as (
    select * from {{ ref('stg_source_91') }}
),

dep_5 as (
    select * from {{ ref('stg_source_100') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_7 as (
    select * from {{ ref('stg_source_147') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_11 as (
    select * from {{ ref('stg_source_20') }}
),

dep_12 as (
    select * from {{ ref('stg_source_84') }}
),

dep_13 as (
    select * from {{ ref('stg_source_162') }}
),

dep_14 as (
    select * from {{ ref('stg_source_34') }}
),

dep_15 as (
    select * from {{ ref('stg_source_47') }}
),

dep_16 as (
    select * from {{ ref('stg_source_59') }}
),

dep_17 as (
    select * from {{ ref('stg_source_17') }}
),

dep_18 as (
    select * from {{ ref('stg_source_94') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_20 as (
    select * from {{ ref('stg_source_101') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_21') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_24 as (
    select * from {{ ref('stg_source_75') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_26 as (
    select * from {{ ref('stg_source_146') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_30 as (
    select * from {{ ref('stg_source_123') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_33 as (
    select * from {{ ref('stg_source_90') }}
),

dep_34 as (
    select * from {{ ref('stg_source_43') }}
),

dep_35 as (
    select * from {{ ref('stg_source_1') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_37 as (
    select * from {{ ref('stg_source_51') }}
),

dep_38 as (
    select * from {{ ref('stg_source_42') }}
),

dep_39 as (
    select * from {{ ref('stg_source_114') }}
),

dep_40 as (
    select * from {{ ref('stg_source_61') }}
),

dep_41 as (
    select * from {{ ref('stg_source_164') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_43 as (
    select * from {{ ref('stg_source_133') }}
),

dep_44 as (
    select * from {{ ref('stg_source_41') }}
),

dep_45 as (
    select * from {{ ref('stg_source_38') }}
),

dep_46 as (
    select * from {{ ref('stg_source_107') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_48 as (
    select * from {{ ref('stg_source_105') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_127') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_1_id']) }} as surrogate_id,
        dep_1.int_transformed_1_id as dim_entity_140_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_1_id = dep_2.int_transformed_128_id
    left join dep_3 on dep_1.int_transformed_1_id = dep_3.stg_source_53_id
    left join dep_4 on dep_1.int_transformed_1_id = dep_4.stg_source_91_id
    left join dep_5 on dep_1.int_transformed_1_id = dep_5.stg_source_100_id
    left join dep_6 on dep_1.int_transformed_1_id = dep_6.int_transformed_158_id
    left join dep_7 on dep_1.int_transformed_1_id = dep_7.stg_source_147_id
    left join dep_8 on dep_1.int_transformed_1_id = dep_8.int_transformed_5_id
    left join dep_9 on dep_1.int_transformed_1_id = dep_9.int_transformed_107_id
    left join dep_10 on dep_1.int_transformed_1_id = dep_10.int_transformed_113_id
    left join dep_11 on dep_1.int_transformed_1_id = dep_11.stg_source_20_id
    left join dep_12 on dep_1.int_transformed_1_id = dep_12.stg_source_84_id
    left join dep_13 on dep_1.int_transformed_1_id = dep_13.stg_source_162_id
    left join dep_14 on dep_1.int_transformed_1_id = dep_14.stg_source_34_id
    left join dep_15 on dep_1.int_transformed_1_id = dep_15.stg_source_47_id
    left join dep_16 on dep_1.int_transformed_1_id = dep_16.stg_source_59_id
    left join dep_17 on dep_1.int_transformed_1_id = dep_17.stg_source_17_id
    left join dep_18 on dep_1.int_transformed_1_id = dep_18.stg_source_94_id
    left join dep_19 on dep_1.int_transformed_1_id = dep_19.int_transformed_96_id
    left join dep_20 on dep_1.int_transformed_1_id = dep_20.stg_source_101_id
    left join dep_21 on dep_1.int_transformed_1_id = dep_21.int_transformed_101_id
    left join dep_22 on dep_1.int_transformed_1_id = dep_22.int_transformed_21_id
    left join dep_23 on dep_1.int_transformed_1_id = dep_23.int_transformed_74_id
    left join dep_24 on dep_1.int_transformed_1_id = dep_24.stg_source_75_id
    left join dep_25 on dep_1.int_transformed_1_id = dep_25.int_transformed_146_id
    left join dep_26 on dep_1.int_transformed_1_id = dep_26.stg_source_146_id
    left join dep_27 on dep_1.int_transformed_1_id = dep_27.int_transformed_145_id
    left join dep_28 on dep_1.int_transformed_1_id = dep_28.int_transformed_134_id
    left join dep_29 on dep_1.int_transformed_1_id = dep_29.int_transformed_45_id
    left join dep_30 on dep_1.int_transformed_1_id = dep_30.stg_source_123_id
    left join dep_31 on dep_1.int_transformed_1_id = dep_31.int_transformed_95_id
    left join dep_32 on dep_1.int_transformed_1_id = dep_32.int_transformed_99_id
    left join dep_33 on dep_1.int_transformed_1_id = dep_33.stg_source_90_id
    left join dep_34 on dep_1.int_transformed_1_id = dep_34.stg_source_43_id
    left join dep_35 on dep_1.int_transformed_1_id = dep_35.stg_source_1_id
    left join dep_36 on dep_1.int_transformed_1_id = dep_36.int_transformed_22_id
    left join dep_37 on dep_1.int_transformed_1_id = dep_37.stg_source_51_id
    left join dep_38 on dep_1.int_transformed_1_id = dep_38.stg_source_42_id
    left join dep_39 on dep_1.int_transformed_1_id = dep_39.stg_source_114_id
    left join dep_40 on dep_1.int_transformed_1_id = dep_40.stg_source_61_id
    left join dep_41 on dep_1.int_transformed_1_id = dep_41.stg_source_164_id
    left join dep_42 on dep_1.int_transformed_1_id = dep_42.int_transformed_137_id
    left join dep_43 on dep_1.int_transformed_1_id = dep_43.stg_source_133_id
    left join dep_44 on dep_1.int_transformed_1_id = dep_44.stg_source_41_id
    left join dep_45 on dep_1.int_transformed_1_id = dep_45.stg_source_38_id
    left join dep_46 on dep_1.int_transformed_1_id = dep_46.stg_source_107_id
    left join dep_47 on dep_1.int_transformed_1_id = dep_47.int_transformed_67_id
    left join dep_48 on dep_1.int_transformed_1_id = dep_48.stg_source_105_id
    left join dep_49 on dep_1.int_transformed_1_id = dep_49.int_transformed_42_id
    left join dep_50 on dep_1.int_transformed_1_id = dep_50.int_transformed_127_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
