{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_3 as (
    select * from {{ ref('stg_source_126') }}
),

dep_4 as (
    select * from {{ ref('stg_source_88') }}
),

dep_5 as (
    select * from {{ ref('stg_source_128') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_8 as (
    select * from {{ ref('stg_source_71') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_12 as (
    select * from {{ ref('stg_source_26') }}
),

dep_13 as (
    select * from {{ ref('stg_source_166') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_15 as (
    select * from {{ ref('stg_source_94') }}
),

dep_16 as (
    select * from {{ ref('stg_source_124') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_21 as (
    select * from {{ ref('stg_source_121') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_23 as (
    select * from {{ ref('stg_source_76') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_25 as (
    select * from {{ ref('stg_source_143') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_27 as (
    select * from {{ ref('stg_source_28') }}
),

dep_28 as (
    select * from {{ ref('stg_source_137') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_31 as (
    select * from {{ ref('stg_source_138') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_33 as (
    select * from {{ ref('stg_source_146') }}
),

dep_34 as (
    select * from {{ ref('stg_source_38') }}
),

dep_35 as (
    select * from {{ ref('stg_source_96') }}
),

dep_36 as (
    select * from {{ ref('stg_source_47') }}
),

dep_37 as (
    select * from {{ ref('stg_source_68') }}
),

dep_38 as (
    select * from {{ ref('stg_source_56') }}
),

dep_39 as (
    select * from {{ ref('stg_source_78') }}
),

dep_40 as (
    select * from {{ ref('stg_source_18') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_42 as (
    select * from {{ ref('stg_source_73') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_44 as (
    select * from {{ ref('stg_source_23') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_47 as (
    select * from {{ ref('stg_source_135') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_49 as (
    select * from {{ ref('stg_source_39') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_16') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_37_id']) }} as surrogate_id,
        dep_1.int_transformed_37_id as fct_business_event_81_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_37_id = dep_2.int_transformed_66_id
    left join dep_3 on dep_1.int_transformed_37_id = dep_3.stg_source_126_id
    left join dep_4 on dep_1.int_transformed_37_id = dep_4.stg_source_88_id
    left join dep_5 on dep_1.int_transformed_37_id = dep_5.stg_source_128_id
    left join dep_6 on dep_1.int_transformed_37_id = dep_6.int_transformed_120_id
    left join dep_7 on dep_1.int_transformed_37_id = dep_7.int_transformed_139_id
    left join dep_8 on dep_1.int_transformed_37_id = dep_8.stg_source_71_id
    left join dep_9 on dep_1.int_transformed_37_id = dep_9.int_transformed_52_id
    left join dep_10 on dep_1.int_transformed_37_id = dep_10.int_transformed_36_id
    left join dep_11 on dep_1.int_transformed_37_id = dep_11.int_transformed_143_id
    left join dep_12 on dep_1.int_transformed_37_id = dep_12.stg_source_26_id
    left join dep_13 on dep_1.int_transformed_37_id = dep_13.stg_source_166_id
    left join dep_14 on dep_1.int_transformed_37_id = dep_14.int_transformed_76_id
    left join dep_15 on dep_1.int_transformed_37_id = dep_15.stg_source_94_id
    left join dep_16 on dep_1.int_transformed_37_id = dep_16.stg_source_124_id
    left join dep_17 on dep_1.int_transformed_37_id = dep_17.int_transformed_108_id
    left join dep_18 on dep_1.int_transformed_37_id = dep_18.int_transformed_38_id
    left join dep_19 on dep_1.int_transformed_37_id = dep_19.int_transformed_78_id
    left join dep_20 on dep_1.int_transformed_37_id = dep_20.int_transformed_49_id
    left join dep_21 on dep_1.int_transformed_37_id = dep_21.stg_source_121_id
    left join dep_22 on dep_1.int_transformed_37_id = dep_22.int_transformed_64_id
    left join dep_23 on dep_1.int_transformed_37_id = dep_23.stg_source_76_id
    left join dep_24 on dep_1.int_transformed_37_id = dep_24.int_transformed_132_id
    left join dep_25 on dep_1.int_transformed_37_id = dep_25.stg_source_143_id
    left join dep_26 on dep_1.int_transformed_37_id = dep_26.int_transformed_161_id
    left join dep_27 on dep_1.int_transformed_37_id = dep_27.stg_source_28_id
    left join dep_28 on dep_1.int_transformed_37_id = dep_28.stg_source_137_id
    left join dep_29 on dep_1.int_transformed_37_id = dep_29.int_transformed_45_id
    left join dep_30 on dep_1.int_transformed_37_id = dep_30.int_transformed_129_id
    left join dep_31 on dep_1.int_transformed_37_id = dep_31.stg_source_138_id
    left join dep_32 on dep_1.int_transformed_37_id = dep_32.int_transformed_14_id
    left join dep_33 on dep_1.int_transformed_37_id = dep_33.stg_source_146_id
    left join dep_34 on dep_1.int_transformed_37_id = dep_34.stg_source_38_id
    left join dep_35 on dep_1.int_transformed_37_id = dep_35.stg_source_96_id
    left join dep_36 on dep_1.int_transformed_37_id = dep_36.stg_source_47_id
    left join dep_37 on dep_1.int_transformed_37_id = dep_37.stg_source_68_id
    left join dep_38 on dep_1.int_transformed_37_id = dep_38.stg_source_56_id
    left join dep_39 on dep_1.int_transformed_37_id = dep_39.stg_source_78_id
    left join dep_40 on dep_1.int_transformed_37_id = dep_40.stg_source_18_id
    left join dep_41 on dep_1.int_transformed_37_id = dep_41.int_transformed_67_id
    left join dep_42 on dep_1.int_transformed_37_id = dep_42.stg_source_73_id
    left join dep_43 on dep_1.int_transformed_37_id = dep_43.int_transformed_69_id
    left join dep_44 on dep_1.int_transformed_37_id = dep_44.stg_source_23_id
    left join dep_45 on dep_1.int_transformed_37_id = dep_45.int_transformed_63_id
    left join dep_46 on dep_1.int_transformed_37_id = dep_46.int_transformed_91_id
    left join dep_47 on dep_1.int_transformed_37_id = dep_47.stg_source_135_id
    left join dep_48 on dep_1.int_transformed_37_id = dep_48.int_transformed_162_id
    left join dep_49 on dep_1.int_transformed_37_id = dep_49.stg_source_39_id
    left join dep_50 on dep_1.int_transformed_37_id = dep_50.int_transformed_16_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
