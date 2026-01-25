{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_2 as (
    select * from {{ ref('stg_source_143') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_4 as (
    select * from {{ ref('stg_source_54') }}
),

dep_5 as (
    select * from {{ ref('stg_source_14') }}
),

dep_6 as (
    select * from {{ ref('stg_source_164') }}
),

dep_7 as (
    select * from {{ ref('stg_source_11') }}
),

dep_8 as (
    select * from {{ ref('stg_source_39') }}
),

dep_9 as (
    select * from {{ ref('stg_source_103') }}
),

dep_10 as (
    select * from {{ ref('stg_source_23') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_12 as (
    select * from {{ ref('stg_source_90') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_14 as (
    select * from {{ ref('stg_source_104') }}
),

dep_15 as (
    select * from {{ ref('stg_source_149') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_18 as (
    select * from {{ ref('stg_source_146') }}
),

dep_19 as (
    select * from {{ ref('stg_source_20') }}
),

dep_20 as (
    select * from {{ ref('stg_source_26') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_22 as (
    select * from {{ ref('stg_source_56') }}
),

dep_23 as (
    select * from {{ ref('stg_source_92') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_26 as (
    select * from {{ ref('stg_source_27') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_31 as (
    select * from {{ ref('stg_source_94') }}
),

dep_32 as (
    select * from {{ ref('stg_source_157') }}
),

dep_33 as (
    select * from {{ ref('stg_source_133') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_36 as (
    select * from {{ ref('stg_source_15') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_39 as (
    select * from {{ ref('stg_source_141') }}
),

dep_40 as (
    select * from {{ ref('stg_source_17') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_42 as (
    select * from {{ ref('stg_source_113') }}
),

dep_43 as (
    select * from {{ ref('stg_source_70') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_45 as (
    select * from {{ ref('stg_source_136') }}
),

dep_46 as (
    select * from {{ ref('stg_source_29') }}
),

dep_47 as (
    select * from {{ ref('stg_source_52') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_16') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_20_id']) }} as surrogate_id,
        dep_1.int_transformed_20_id as fct_business_event_75_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_20_id = dep_2.stg_source_143_id
    left join dep_3 on dep_1.int_transformed_20_id = dep_3.int_transformed_130_id
    left join dep_4 on dep_1.int_transformed_20_id = dep_4.stg_source_54_id
    left join dep_5 on dep_1.int_transformed_20_id = dep_5.stg_source_14_id
    left join dep_6 on dep_1.int_transformed_20_id = dep_6.stg_source_164_id
    left join dep_7 on dep_1.int_transformed_20_id = dep_7.stg_source_11_id
    left join dep_8 on dep_1.int_transformed_20_id = dep_8.stg_source_39_id
    left join dep_9 on dep_1.int_transformed_20_id = dep_9.stg_source_103_id
    left join dep_10 on dep_1.int_transformed_20_id = dep_10.stg_source_23_id
    left join dep_11 on dep_1.int_transformed_20_id = dep_11.int_transformed_46_id
    left join dep_12 on dep_1.int_transformed_20_id = dep_12.stg_source_90_id
    left join dep_13 on dep_1.int_transformed_20_id = dep_13.int_transformed_6_id
    left join dep_14 on dep_1.int_transformed_20_id = dep_14.stg_source_104_id
    left join dep_15 on dep_1.int_transformed_20_id = dep_15.stg_source_149_id
    left join dep_16 on dep_1.int_transformed_20_id = dep_16.int_transformed_48_id
    left join dep_17 on dep_1.int_transformed_20_id = dep_17.int_transformed_88_id
    left join dep_18 on dep_1.int_transformed_20_id = dep_18.stg_source_146_id
    left join dep_19 on dep_1.int_transformed_20_id = dep_19.stg_source_20_id
    left join dep_20 on dep_1.int_transformed_20_id = dep_20.stg_source_26_id
    left join dep_21 on dep_1.int_transformed_20_id = dep_21.int_transformed_74_id
    left join dep_22 on dep_1.int_transformed_20_id = dep_22.stg_source_56_id
    left join dep_23 on dep_1.int_transformed_20_id = dep_23.stg_source_92_id
    left join dep_24 on dep_1.int_transformed_20_id = dep_24.int_transformed_103_id
    left join dep_25 on dep_1.int_transformed_20_id = dep_25.int_transformed_15_id
    left join dep_26 on dep_1.int_transformed_20_id = dep_26.stg_source_27_id
    left join dep_27 on dep_1.int_transformed_20_id = dep_27.int_transformed_132_id
    left join dep_28 on dep_1.int_transformed_20_id = dep_28.int_transformed_119_id
    left join dep_29 on dep_1.int_transformed_20_id = dep_29.int_transformed_86_id
    left join dep_30 on dep_1.int_transformed_20_id = dep_30.int_transformed_33_id
    left join dep_31 on dep_1.int_transformed_20_id = dep_31.stg_source_94_id
    left join dep_32 on dep_1.int_transformed_20_id = dep_32.stg_source_157_id
    left join dep_33 on dep_1.int_transformed_20_id = dep_33.stg_source_133_id
    left join dep_34 on dep_1.int_transformed_20_id = dep_34.int_transformed_80_id
    left join dep_35 on dep_1.int_transformed_20_id = dep_35.int_transformed_56_id
    left join dep_36 on dep_1.int_transformed_20_id = dep_36.stg_source_15_id
    left join dep_37 on dep_1.int_transformed_20_id = dep_37.int_transformed_150_id
    left join dep_38 on dep_1.int_transformed_20_id = dep_38.int_transformed_123_id
    left join dep_39 on dep_1.int_transformed_20_id = dep_39.stg_source_141_id
    left join dep_40 on dep_1.int_transformed_20_id = dep_40.stg_source_17_id
    left join dep_41 on dep_1.int_transformed_20_id = dep_41.int_transformed_78_id
    left join dep_42 on dep_1.int_transformed_20_id = dep_42.stg_source_113_id
    left join dep_43 on dep_1.int_transformed_20_id = dep_43.stg_source_70_id
    left join dep_44 on dep_1.int_transformed_20_id = dep_44.int_transformed_109_id
    left join dep_45 on dep_1.int_transformed_20_id = dep_45.stg_source_136_id
    left join dep_46 on dep_1.int_transformed_20_id = dep_46.stg_source_29_id
    left join dep_47 on dep_1.int_transformed_20_id = dep_47.stg_source_52_id
    left join dep_48 on dep_1.int_transformed_20_id = dep_48.int_transformed_64_id
    left join dep_49 on dep_1.int_transformed_20_id = dep_49.int_transformed_138_id
    left join dep_50 on dep_1.int_transformed_20_id = dep_50.int_transformed_16_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
