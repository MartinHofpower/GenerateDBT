{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_34') }}
),

dep_2 as (
    select * from {{ ref('stg_source_31') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_5 as (
    select * from {{ ref('stg_source_61') }}
),

dep_6 as (
    select * from {{ ref('stg_source_58') }}
),

dep_7 as (
    select * from {{ ref('stg_source_87') }}
),

dep_8 as (
    select * from {{ ref('stg_source_1') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_12 as (
    select * from {{ ref('stg_source_9') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_14 as (
    select * from {{ ref('stg_source_21') }}
),

dep_15 as (
    select * from {{ ref('stg_source_101') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_18 as (
    select * from {{ ref('stg_source_70') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_24 as (
    select * from {{ ref('stg_source_125') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_27 as (
    select * from {{ ref('stg_source_132') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_34 as (
    select * from {{ ref('stg_source_55') }}
),

dep_35 as (
    select * from {{ ref('stg_source_146') }}
),

dep_36 as (
    select * from {{ ref('stg_source_128') }}
),

dep_37 as (
    select * from {{ ref('stg_source_67') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_39 as (
    select * from {{ ref('stg_source_166') }}
),

dep_40 as (
    select * from {{ ref('stg_source_25') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_43 as (
    select * from {{ ref('stg_source_38') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_45 as (
    select * from {{ ref('stg_source_80') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_48 as (
    select * from {{ ref('stg_source_53') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_100') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_34_id']) }} as surrogate_id,
        dep_1.stg_source_34_id as fct_business_event_43_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_34_id = dep_2.stg_source_31_id
    left join dep_3 on dep_1.stg_source_34_id = dep_3.int_transformed_105_id
    left join dep_4 on dep_1.stg_source_34_id = dep_4.int_transformed_72_id
    left join dep_5 on dep_1.stg_source_34_id = dep_5.stg_source_61_id
    left join dep_6 on dep_1.stg_source_34_id = dep_6.stg_source_58_id
    left join dep_7 on dep_1.stg_source_34_id = dep_7.stg_source_87_id
    left join dep_8 on dep_1.stg_source_34_id = dep_8.stg_source_1_id
    left join dep_9 on dep_1.stg_source_34_id = dep_9.int_transformed_99_id
    left join dep_10 on dep_1.stg_source_34_id = dep_10.int_transformed_60_id
    left join dep_11 on dep_1.stg_source_34_id = dep_11.int_transformed_64_id
    left join dep_12 on dep_1.stg_source_34_id = dep_12.stg_source_9_id
    left join dep_13 on dep_1.stg_source_34_id = dep_13.int_transformed_66_id
    left join dep_14 on dep_1.stg_source_34_id = dep_14.stg_source_21_id
    left join dep_15 on dep_1.stg_source_34_id = dep_15.stg_source_101_id
    left join dep_16 on dep_1.stg_source_34_id = dep_16.int_transformed_119_id
    left join dep_17 on dep_1.stg_source_34_id = dep_17.int_transformed_123_id
    left join dep_18 on dep_1.stg_source_34_id = dep_18.stg_source_70_id
    left join dep_19 on dep_1.stg_source_34_id = dep_19.int_transformed_23_id
    left join dep_20 on dep_1.stg_source_34_id = dep_20.int_transformed_147_id
    left join dep_21 on dep_1.stg_source_34_id = dep_21.int_transformed_27_id
    left join dep_22 on dep_1.stg_source_34_id = dep_22.int_transformed_31_id
    left join dep_23 on dep_1.stg_source_34_id = dep_23.int_transformed_71_id
    left join dep_24 on dep_1.stg_source_34_id = dep_24.stg_source_125_id
    left join dep_25 on dep_1.stg_source_34_id = dep_25.int_transformed_127_id
    left join dep_26 on dep_1.stg_source_34_id = dep_26.int_transformed_48_id
    left join dep_27 on dep_1.stg_source_34_id = dep_27.stg_source_132_id
    left join dep_28 on dep_1.stg_source_34_id = dep_28.int_transformed_1_id
    left join dep_29 on dep_1.stg_source_34_id = dep_29.int_transformed_37_id
    left join dep_30 on dep_1.stg_source_34_id = dep_30.int_transformed_47_id
    left join dep_31 on dep_1.stg_source_34_id = dep_31.int_transformed_77_id
    left join dep_32 on dep_1.stg_source_34_id = dep_32.int_transformed_132_id
    left join dep_33 on dep_1.stg_source_34_id = dep_33.int_transformed_52_id
    left join dep_34 on dep_1.stg_source_34_id = dep_34.stg_source_55_id
    left join dep_35 on dep_1.stg_source_34_id = dep_35.stg_source_146_id
    left join dep_36 on dep_1.stg_source_34_id = dep_36.stg_source_128_id
    left join dep_37 on dep_1.stg_source_34_id = dep_37.stg_source_67_id
    left join dep_38 on dep_1.stg_source_34_id = dep_38.int_transformed_110_id
    left join dep_39 on dep_1.stg_source_34_id = dep_39.stg_source_166_id
    left join dep_40 on dep_1.stg_source_34_id = dep_40.stg_source_25_id
    left join dep_41 on dep_1.stg_source_34_id = dep_41.int_transformed_68_id
    left join dep_42 on dep_1.stg_source_34_id = dep_42.int_transformed_121_id
    left join dep_43 on dep_1.stg_source_34_id = dep_43.stg_source_38_id
    left join dep_44 on dep_1.stg_source_34_id = dep_44.int_transformed_10_id
    left join dep_45 on dep_1.stg_source_34_id = dep_45.stg_source_80_id
    left join dep_46 on dep_1.stg_source_34_id = dep_46.int_transformed_59_id
    left join dep_47 on dep_1.stg_source_34_id = dep_47.int_transformed_148_id
    left join dep_48 on dep_1.stg_source_34_id = dep_48.stg_source_53_id
    left join dep_49 on dep_1.stg_source_34_id = dep_49.int_transformed_94_id
    left join dep_50 on dep_1.stg_source_34_id = dep_50.int_transformed_100_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
