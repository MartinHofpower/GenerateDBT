{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_68') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_3 as (
    select * from {{ ref('stg_source_166') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_5 as (
    select * from {{ ref('stg_source_71') }}
),

dep_6 as (
    select * from {{ ref('stg_source_26') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_9 as (
    select * from {{ ref('stg_source_111') }}
),

dep_10 as (
    select * from {{ ref('stg_source_158') }}
),

dep_11 as (
    select * from {{ ref('stg_source_48') }}
),

dep_12 as (
    select * from {{ ref('stg_source_138') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_14 as (
    select * from {{ ref('stg_source_24') }}
),

dep_15 as (
    select * from {{ ref('stg_source_6') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_18 as (
    select * from {{ ref('stg_source_108') }}
),

dep_19 as (
    select * from {{ ref('stg_source_31') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_21 as (
    select * from {{ ref('stg_source_163') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_23 as (
    select * from {{ ref('stg_source_90') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_25 as (
    select * from {{ ref('stg_source_100') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_28 as (
    select * from {{ ref('stg_source_77') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_31 as (
    select * from {{ ref('stg_source_110') }}
),

dep_32 as (
    select * from {{ ref('stg_source_91') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_34 as (
    select * from {{ ref('stg_source_159') }}
),

dep_35 as (
    select * from {{ ref('stg_source_72') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_37 as (
    select * from {{ ref('stg_source_112') }}
),

dep_38 as (
    select * from {{ ref('stg_source_82') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_40 as (
    select * from {{ ref('stg_source_162') }}
),

dep_41 as (
    select * from {{ ref('stg_source_152') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_43 as (
    select * from {{ ref('stg_source_44') }}
),

dep_44 as (
    select * from {{ ref('stg_source_79') }}
),

dep_45 as (
    select * from {{ ref('stg_source_21') }}
),

dep_46 as (
    select * from {{ ref('stg_source_51') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_164') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_68_id']) }} as surrogate_id,
        dep_1.stg_source_68_id as fct_business_event_73_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_68_id = dep_2.int_transformed_4_id
    left join dep_3 on dep_1.stg_source_68_id = dep_3.stg_source_166_id
    left join dep_4 on dep_1.stg_source_68_id = dep_4.int_transformed_144_id
    left join dep_5 on dep_1.stg_source_68_id = dep_5.stg_source_71_id
    left join dep_6 on dep_1.stg_source_68_id = dep_6.stg_source_26_id
    left join dep_7 on dep_1.stg_source_68_id = dep_7.int_transformed_111_id
    left join dep_8 on dep_1.stg_source_68_id = dep_8.int_transformed_33_id
    left join dep_9 on dep_1.stg_source_68_id = dep_9.stg_source_111_id
    left join dep_10 on dep_1.stg_source_68_id = dep_10.stg_source_158_id
    left join dep_11 on dep_1.stg_source_68_id = dep_11.stg_source_48_id
    left join dep_12 on dep_1.stg_source_68_id = dep_12.stg_source_138_id
    left join dep_13 on dep_1.stg_source_68_id = dep_13.int_transformed_113_id
    left join dep_14 on dep_1.stg_source_68_id = dep_14.stg_source_24_id
    left join dep_15 on dep_1.stg_source_68_id = dep_15.stg_source_6_id
    left join dep_16 on dep_1.stg_source_68_id = dep_16.int_transformed_57_id
    left join dep_17 on dep_1.stg_source_68_id = dep_17.int_transformed_105_id
    left join dep_18 on dep_1.stg_source_68_id = dep_18.stg_source_108_id
    left join dep_19 on dep_1.stg_source_68_id = dep_19.stg_source_31_id
    left join dep_20 on dep_1.stg_source_68_id = dep_20.int_transformed_125_id
    left join dep_21 on dep_1.stg_source_68_id = dep_21.stg_source_163_id
    left join dep_22 on dep_1.stg_source_68_id = dep_22.int_transformed_53_id
    left join dep_23 on dep_1.stg_source_68_id = dep_23.stg_source_90_id
    left join dep_24 on dep_1.stg_source_68_id = dep_24.int_transformed_127_id
    left join dep_25 on dep_1.stg_source_68_id = dep_25.stg_source_100_id
    left join dep_26 on dep_1.stg_source_68_id = dep_26.int_transformed_17_id
    left join dep_27 on dep_1.stg_source_68_id = dep_27.int_transformed_155_id
    left join dep_28 on dep_1.stg_source_68_id = dep_28.stg_source_77_id
    left join dep_29 on dep_1.stg_source_68_id = dep_29.int_transformed_27_id
    left join dep_30 on dep_1.stg_source_68_id = dep_30.int_transformed_60_id
    left join dep_31 on dep_1.stg_source_68_id = dep_31.stg_source_110_id
    left join dep_32 on dep_1.stg_source_68_id = dep_32.stg_source_91_id
    left join dep_33 on dep_1.stg_source_68_id = dep_33.int_transformed_61_id
    left join dep_34 on dep_1.stg_source_68_id = dep_34.stg_source_159_id
    left join dep_35 on dep_1.stg_source_68_id = dep_35.stg_source_72_id
    left join dep_36 on dep_1.stg_source_68_id = dep_36.int_transformed_29_id
    left join dep_37 on dep_1.stg_source_68_id = dep_37.stg_source_112_id
    left join dep_38 on dep_1.stg_source_68_id = dep_38.stg_source_82_id
    left join dep_39 on dep_1.stg_source_68_id = dep_39.int_transformed_129_id
    left join dep_40 on dep_1.stg_source_68_id = dep_40.stg_source_162_id
    left join dep_41 on dep_1.stg_source_68_id = dep_41.stg_source_152_id
    left join dep_42 on dep_1.stg_source_68_id = dep_42.int_transformed_1_id
    left join dep_43 on dep_1.stg_source_68_id = dep_43.stg_source_44_id
    left join dep_44 on dep_1.stg_source_68_id = dep_44.stg_source_79_id
    left join dep_45 on dep_1.stg_source_68_id = dep_45.stg_source_21_id
    left join dep_46 on dep_1.stg_source_68_id = dep_46.stg_source_51_id
    left join dep_47 on dep_1.stg_source_68_id = dep_47.int_transformed_134_id
    left join dep_48 on dep_1.stg_source_68_id = dep_48.int_transformed_87_id
    left join dep_49 on dep_1.stg_source_68_id = dep_49.int_transformed_158_id
    left join dep_50 on dep_1.stg_source_68_id = dep_50.int_transformed_164_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
