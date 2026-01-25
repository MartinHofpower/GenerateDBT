{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_5 as (
    select * from {{ ref('stg_source_151') }}
),

dep_6 as (
    select * from {{ ref('stg_source_1') }}
),

dep_7 as (
    select * from {{ ref('stg_source_30') }}
),

dep_8 as (
    select * from {{ ref('stg_source_74') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_11 as (
    select * from {{ ref('stg_source_152') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_13 as (
    select * from {{ ref('stg_source_147') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_16 as (
    select * from {{ ref('stg_source_139') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_18 as (
    select * from {{ ref('stg_source_109') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_20 as (
    select * from {{ ref('stg_source_165') }}
),

dep_21 as (
    select * from {{ ref('stg_source_142') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_21') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_29 as (
    select * from {{ ref('stg_source_136') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_33 as (
    select * from {{ ref('stg_source_38') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_37 as (
    select * from {{ ref('stg_source_33') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_39 as (
    select * from {{ ref('stg_source_72') }}
),

dep_40 as (
    select * from {{ ref('stg_source_46') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_42 as (
    select * from {{ ref('stg_source_24') }}
),

dep_43 as (
    select * from {{ ref('stg_source_9') }}
),

dep_44 as (
    select * from {{ ref('stg_source_158') }}
),

dep_45 as (
    select * from {{ ref('stg_source_105') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_47 as (
    select * from {{ ref('stg_source_23') }}
),

dep_48 as (
    select * from {{ ref('stg_source_138') }}
),

dep_49 as (
    select * from {{ ref('stg_source_159') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_52') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_151_id']) }} as surrogate_id,
        dep_1.int_transformed_151_id as fct_business_event_61_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_151_id = dep_2.int_transformed_13_id
    left join dep_3 on dep_1.int_transformed_151_id = dep_3.int_transformed_71_id
    left join dep_4 on dep_1.int_transformed_151_id = dep_4.int_transformed_100_id
    left join dep_5 on dep_1.int_transformed_151_id = dep_5.stg_source_151_id
    left join dep_6 on dep_1.int_transformed_151_id = dep_6.stg_source_1_id
    left join dep_7 on dep_1.int_transformed_151_id = dep_7.stg_source_30_id
    left join dep_8 on dep_1.int_transformed_151_id = dep_8.stg_source_74_id
    left join dep_9 on dep_1.int_transformed_151_id = dep_9.int_transformed_27_id
    left join dep_10 on dep_1.int_transformed_151_id = dep_10.int_transformed_20_id
    left join dep_11 on dep_1.int_transformed_151_id = dep_11.stg_source_152_id
    left join dep_12 on dep_1.int_transformed_151_id = dep_12.int_transformed_5_id
    left join dep_13 on dep_1.int_transformed_151_id = dep_13.stg_source_147_id
    left join dep_14 on dep_1.int_transformed_151_id = dep_14.int_transformed_24_id
    left join dep_15 on dep_1.int_transformed_151_id = dep_15.int_transformed_42_id
    left join dep_16 on dep_1.int_transformed_151_id = dep_16.stg_source_139_id
    left join dep_17 on dep_1.int_transformed_151_id = dep_17.int_transformed_102_id
    left join dep_18 on dep_1.int_transformed_151_id = dep_18.stg_source_109_id
    left join dep_19 on dep_1.int_transformed_151_id = dep_19.int_transformed_107_id
    left join dep_20 on dep_1.int_transformed_151_id = dep_20.stg_source_165_id
    left join dep_21 on dep_1.int_transformed_151_id = dep_21.stg_source_142_id
    left join dep_22 on dep_1.int_transformed_151_id = dep_22.int_transformed_58_id
    left join dep_23 on dep_1.int_transformed_151_id = dep_23.int_transformed_145_id
    left join dep_24 on dep_1.int_transformed_151_id = dep_24.int_transformed_146_id
    left join dep_25 on dep_1.int_transformed_151_id = dep_25.int_transformed_150_id
    left join dep_26 on dep_1.int_transformed_151_id = dep_26.int_transformed_116_id
    left join dep_27 on dep_1.int_transformed_151_id = dep_27.int_transformed_21_id
    left join dep_28 on dep_1.int_transformed_151_id = dep_28.int_transformed_166_id
    left join dep_29 on dep_1.int_transformed_151_id = dep_29.stg_source_136_id
    left join dep_30 on dep_1.int_transformed_151_id = dep_30.int_transformed_106_id
    left join dep_31 on dep_1.int_transformed_151_id = dep_31.int_transformed_97_id
    left join dep_32 on dep_1.int_transformed_151_id = dep_32.int_transformed_33_id
    left join dep_33 on dep_1.int_transformed_151_id = dep_33.stg_source_38_id
    left join dep_34 on dep_1.int_transformed_151_id = dep_34.int_transformed_30_id
    left join dep_35 on dep_1.int_transformed_151_id = dep_35.int_transformed_53_id
    left join dep_36 on dep_1.int_transformed_151_id = dep_36.int_transformed_156_id
    left join dep_37 on dep_1.int_transformed_151_id = dep_37.stg_source_33_id
    left join dep_38 on dep_1.int_transformed_151_id = dep_38.int_transformed_34_id
    left join dep_39 on dep_1.int_transformed_151_id = dep_39.stg_source_72_id
    left join dep_40 on dep_1.int_transformed_151_id = dep_40.stg_source_46_id
    left join dep_41 on dep_1.int_transformed_151_id = dep_41.int_transformed_38_id
    left join dep_42 on dep_1.int_transformed_151_id = dep_42.stg_source_24_id
    left join dep_43 on dep_1.int_transformed_151_id = dep_43.stg_source_9_id
    left join dep_44 on dep_1.int_transformed_151_id = dep_44.stg_source_158_id
    left join dep_45 on dep_1.int_transformed_151_id = dep_45.stg_source_105_id
    left join dep_46 on dep_1.int_transformed_151_id = dep_46.int_transformed_61_id
    left join dep_47 on dep_1.int_transformed_151_id = dep_47.stg_source_23_id
    left join dep_48 on dep_1.int_transformed_151_id = dep_48.stg_source_138_id
    left join dep_49 on dep_1.int_transformed_151_id = dep_49.stg_source_159_id
    left join dep_50 on dep_1.int_transformed_151_id = dep_50.int_transformed_52_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
