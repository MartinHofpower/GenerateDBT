{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_2 as (
    select * from {{ ref('stg_source_158') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_5 as (
    select * from {{ ref('stg_source_104') }}
),

dep_6 as (
    select * from {{ ref('stg_source_98') }}
),

dep_7 as (
    select * from {{ ref('stg_source_135') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_10 as (
    select * from {{ ref('stg_source_90') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_12 as (
    select * from {{ ref('stg_source_105') }}
),

dep_13 as (
    select * from {{ ref('stg_source_4') }}
),

dep_14 as (
    select * from {{ ref('stg_source_75') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_16 as (
    select * from {{ ref('stg_source_156') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_20 as (
    select * from {{ ref('stg_source_73') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_23 as (
    select * from {{ ref('stg_source_8') }}
),

dep_24 as (
    select * from {{ ref('stg_source_55') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_30 as (
    select * from {{ ref('stg_source_12') }}
),

dep_31 as (
    select * from {{ ref('stg_source_61') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_33 as (
    select * from {{ ref('stg_source_76') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_35 as (
    select * from {{ ref('stg_source_31') }}
),

dep_36 as (
    select * from {{ ref('stg_source_37') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_38 as (
    select * from {{ ref('stg_source_162') }}
),

dep_39 as (
    select * from {{ ref('stg_source_86') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_41 as (
    select * from {{ ref('stg_source_34') }}
),

dep_42 as (
    select * from {{ ref('stg_source_95') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_130') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_30_id']) }} as surrogate_id,
        dep_1.int_transformed_30_id as fct_business_event_97_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_30_id = dep_2.stg_source_158_id
    left join dep_3 on dep_1.int_transformed_30_id = dep_3.int_transformed_79_id
    left join dep_4 on dep_1.int_transformed_30_id = dep_4.int_transformed_113_id
    left join dep_5 on dep_1.int_transformed_30_id = dep_5.stg_source_104_id
    left join dep_6 on dep_1.int_transformed_30_id = dep_6.stg_source_98_id
    left join dep_7 on dep_1.int_transformed_30_id = dep_7.stg_source_135_id
    left join dep_8 on dep_1.int_transformed_30_id = dep_8.int_transformed_49_id
    left join dep_9 on dep_1.int_transformed_30_id = dep_9.int_transformed_34_id
    left join dep_10 on dep_1.int_transformed_30_id = dep_10.stg_source_90_id
    left join dep_11 on dep_1.int_transformed_30_id = dep_11.int_transformed_40_id
    left join dep_12 on dep_1.int_transformed_30_id = dep_12.stg_source_105_id
    left join dep_13 on dep_1.int_transformed_30_id = dep_13.stg_source_4_id
    left join dep_14 on dep_1.int_transformed_30_id = dep_14.stg_source_75_id
    left join dep_15 on dep_1.int_transformed_30_id = dep_15.int_transformed_95_id
    left join dep_16 on dep_1.int_transformed_30_id = dep_16.stg_source_156_id
    left join dep_17 on dep_1.int_transformed_30_id = dep_17.int_transformed_163_id
    left join dep_18 on dep_1.int_transformed_30_id = dep_18.int_transformed_27_id
    left join dep_19 on dep_1.int_transformed_30_id = dep_19.int_transformed_110_id
    left join dep_20 on dep_1.int_transformed_30_id = dep_20.stg_source_73_id
    left join dep_21 on dep_1.int_transformed_30_id = dep_21.int_transformed_72_id
    left join dep_22 on dep_1.int_transformed_30_id = dep_22.int_transformed_90_id
    left join dep_23 on dep_1.int_transformed_30_id = dep_23.stg_source_8_id
    left join dep_24 on dep_1.int_transformed_30_id = dep_24.stg_source_55_id
    left join dep_25 on dep_1.int_transformed_30_id = dep_25.int_transformed_97_id
    left join dep_26 on dep_1.int_transformed_30_id = dep_26.int_transformed_52_id
    left join dep_27 on dep_1.int_transformed_30_id = dep_27.int_transformed_145_id
    left join dep_28 on dep_1.int_transformed_30_id = dep_28.int_transformed_121_id
    left join dep_29 on dep_1.int_transformed_30_id = dep_29.int_transformed_146_id
    left join dep_30 on dep_1.int_transformed_30_id = dep_30.stg_source_12_id
    left join dep_31 on dep_1.int_transformed_30_id = dep_31.stg_source_61_id
    left join dep_32 on dep_1.int_transformed_30_id = dep_32.int_transformed_39_id
    left join dep_33 on dep_1.int_transformed_30_id = dep_33.stg_source_76_id
    left join dep_34 on dep_1.int_transformed_30_id = dep_34.int_transformed_56_id
    left join dep_35 on dep_1.int_transformed_30_id = dep_35.stg_source_31_id
    left join dep_36 on dep_1.int_transformed_30_id = dep_36.stg_source_37_id
    left join dep_37 on dep_1.int_transformed_30_id = dep_37.int_transformed_81_id
    left join dep_38 on dep_1.int_transformed_30_id = dep_38.stg_source_162_id
    left join dep_39 on dep_1.int_transformed_30_id = dep_39.stg_source_86_id
    left join dep_40 on dep_1.int_transformed_30_id = dep_40.int_transformed_101_id
    left join dep_41 on dep_1.int_transformed_30_id = dep_41.stg_source_34_id
    left join dep_42 on dep_1.int_transformed_30_id = dep_42.stg_source_95_id
    left join dep_43 on dep_1.int_transformed_30_id = dep_43.int_transformed_23_id
    left join dep_44 on dep_1.int_transformed_30_id = dep_44.int_transformed_59_id
    left join dep_45 on dep_1.int_transformed_30_id = dep_45.int_transformed_54_id
    left join dep_46 on dep_1.int_transformed_30_id = dep_46.int_transformed_152_id
    left join dep_47 on dep_1.int_transformed_30_id = dep_47.int_transformed_35_id
    left join dep_48 on dep_1.int_transformed_30_id = dep_48.int_transformed_150_id
    left join dep_49 on dep_1.int_transformed_30_id = dep_49.int_transformed_6_id
    left join dep_50 on dep_1.int_transformed_30_id = dep_50.int_transformed_130_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
