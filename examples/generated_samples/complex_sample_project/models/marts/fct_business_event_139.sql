{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_2 as (
    select * from {{ ref('stg_source_49') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_5 as (
    select * from {{ ref('stg_source_57') }}
),

dep_6 as (
    select * from {{ ref('stg_source_76') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_8 as (
    select * from {{ ref('stg_source_107') }}
),

dep_9 as (
    select * from {{ ref('stg_source_145') }}
),

dep_10 as (
    select * from {{ ref('stg_source_95') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_12 as (
    select * from {{ ref('stg_source_136') }}
),

dep_13 as (
    select * from {{ ref('stg_source_18') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_16 as (
    select * from {{ ref('stg_source_43') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_18 as (
    select * from {{ ref('stg_source_35') }}
),

dep_19 as (
    select * from {{ ref('stg_source_56') }}
),

dep_20 as (
    select * from {{ ref('stg_source_40') }}
),

dep_21 as (
    select * from {{ ref('stg_source_89') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_25 as (
    select * from {{ ref('stg_source_3') }}
),

dep_26 as (
    select * from {{ ref('stg_source_135') }}
),

dep_27 as (
    select * from {{ ref('stg_source_100') }}
),

dep_28 as (
    select * from {{ ref('stg_source_10') }}
),

dep_29 as (
    select * from {{ ref('stg_source_130') }}
),

dep_30 as (
    select * from {{ ref('stg_source_148') }}
),

dep_31 as (
    select * from {{ ref('stg_source_74') }}
),

dep_32 as (
    select * from {{ ref('stg_source_69') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_34 as (
    select * from {{ ref('stg_source_72') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_36 as (
    select * from {{ ref('stg_source_98') }}
),

dep_37 as (
    select * from {{ ref('stg_source_118') }}
),

dep_38 as (
    select * from {{ ref('stg_source_36') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_42 as (
    select * from {{ ref('stg_source_54') }}
),

dep_43 as (
    select * from {{ ref('stg_source_1') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_45 as (
    select * from {{ ref('stg_source_119') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_49 as (
    select * from {{ ref('stg_source_150') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_24') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_141_id']) }} as surrogate_id,
        dep_1.int_transformed_141_id as fct_business_event_139_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_141_id = dep_2.stg_source_49_id
    left join dep_3 on dep_1.int_transformed_141_id = dep_3.int_transformed_72_id
    left join dep_4 on dep_1.int_transformed_141_id = dep_4.int_transformed_96_id
    left join dep_5 on dep_1.int_transformed_141_id = dep_5.stg_source_57_id
    left join dep_6 on dep_1.int_transformed_141_id = dep_6.stg_source_76_id
    left join dep_7 on dep_1.int_transformed_141_id = dep_7.int_transformed_127_id
    left join dep_8 on dep_1.int_transformed_141_id = dep_8.stg_source_107_id
    left join dep_9 on dep_1.int_transformed_141_id = dep_9.stg_source_145_id
    left join dep_10 on dep_1.int_transformed_141_id = dep_10.stg_source_95_id
    left join dep_11 on dep_1.int_transformed_141_id = dep_11.int_transformed_44_id
    left join dep_12 on dep_1.int_transformed_141_id = dep_12.stg_source_136_id
    left join dep_13 on dep_1.int_transformed_141_id = dep_13.stg_source_18_id
    left join dep_14 on dep_1.int_transformed_141_id = dep_14.int_transformed_19_id
    left join dep_15 on dep_1.int_transformed_141_id = dep_15.int_transformed_11_id
    left join dep_16 on dep_1.int_transformed_141_id = dep_16.stg_source_43_id
    left join dep_17 on dep_1.int_transformed_141_id = dep_17.int_transformed_104_id
    left join dep_18 on dep_1.int_transformed_141_id = dep_18.stg_source_35_id
    left join dep_19 on dep_1.int_transformed_141_id = dep_19.stg_source_56_id
    left join dep_20 on dep_1.int_transformed_141_id = dep_20.stg_source_40_id
    left join dep_21 on dep_1.int_transformed_141_id = dep_21.stg_source_89_id
    left join dep_22 on dep_1.int_transformed_141_id = dep_22.int_transformed_86_id
    left join dep_23 on dep_1.int_transformed_141_id = dep_23.int_transformed_29_id
    left join dep_24 on dep_1.int_transformed_141_id = dep_24.int_transformed_74_id
    left join dep_25 on dep_1.int_transformed_141_id = dep_25.stg_source_3_id
    left join dep_26 on dep_1.int_transformed_141_id = dep_26.stg_source_135_id
    left join dep_27 on dep_1.int_transformed_141_id = dep_27.stg_source_100_id
    left join dep_28 on dep_1.int_transformed_141_id = dep_28.stg_source_10_id
    left join dep_29 on dep_1.int_transformed_141_id = dep_29.stg_source_130_id
    left join dep_30 on dep_1.int_transformed_141_id = dep_30.stg_source_148_id
    left join dep_31 on dep_1.int_transformed_141_id = dep_31.stg_source_74_id
    left join dep_32 on dep_1.int_transformed_141_id = dep_32.stg_source_69_id
    left join dep_33 on dep_1.int_transformed_141_id = dep_33.int_transformed_113_id
    left join dep_34 on dep_1.int_transformed_141_id = dep_34.stg_source_72_id
    left join dep_35 on dep_1.int_transformed_141_id = dep_35.int_transformed_8_id
    left join dep_36 on dep_1.int_transformed_141_id = dep_36.stg_source_98_id
    left join dep_37 on dep_1.int_transformed_141_id = dep_37.stg_source_118_id
    left join dep_38 on dep_1.int_transformed_141_id = dep_38.stg_source_36_id
    left join dep_39 on dep_1.int_transformed_141_id = dep_39.int_transformed_129_id
    left join dep_40 on dep_1.int_transformed_141_id = dep_40.int_transformed_62_id
    left join dep_41 on dep_1.int_transformed_141_id = dep_41.int_transformed_137_id
    left join dep_42 on dep_1.int_transformed_141_id = dep_42.stg_source_54_id
    left join dep_43 on dep_1.int_transformed_141_id = dep_43.stg_source_1_id
    left join dep_44 on dep_1.int_transformed_141_id = dep_44.int_transformed_132_id
    left join dep_45 on dep_1.int_transformed_141_id = dep_45.stg_source_119_id
    left join dep_46 on dep_1.int_transformed_141_id = dep_46.int_transformed_64_id
    left join dep_47 on dep_1.int_transformed_141_id = dep_47.int_transformed_116_id
    left join dep_48 on dep_1.int_transformed_141_id = dep_48.int_transformed_68_id
    left join dep_49 on dep_1.int_transformed_141_id = dep_49.stg_source_150_id
    left join dep_50 on dep_1.int_transformed_141_id = dep_50.int_transformed_24_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
