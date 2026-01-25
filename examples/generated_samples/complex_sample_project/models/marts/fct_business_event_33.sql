{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_2 as (
    select * from {{ ref('stg_source_40') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_5 as (
    select * from {{ ref('stg_source_16') }}
),

dep_6 as (
    select * from {{ ref('stg_source_57') }}
),

dep_7 as (
    select * from {{ ref('stg_source_13') }}
),

dep_8 as (
    select * from {{ ref('stg_source_20') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_10 as (
    select * from {{ ref('stg_source_42') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_12 as (
    select * from {{ ref('stg_source_18') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_15 as (
    select * from {{ ref('stg_source_2') }}
),

dep_16 as (
    select * from {{ ref('stg_source_147') }}
),

dep_17 as (
    select * from {{ ref('stg_source_135') }}
),

dep_18 as (
    select * from {{ ref('stg_source_22') }}
),

dep_19 as (
    select * from {{ ref('stg_source_58') }}
),

dep_20 as (
    select * from {{ ref('stg_source_104') }}
),

dep_21 as (
    select * from {{ ref('stg_source_24') }}
),

dep_22 as (
    select * from {{ ref('stg_source_29') }}
),

dep_23 as (
    select * from {{ ref('stg_source_143') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_25 as (
    select * from {{ ref('stg_source_53') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_27 as (
    select * from {{ ref('stg_source_86') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_32 as (
    select * from {{ ref('stg_source_73') }}
),

dep_33 as (
    select * from {{ ref('stg_source_41') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_36 as (
    select * from {{ ref('stg_source_126') }}
),

dep_37 as (
    select * from {{ ref('stg_source_133') }}
),

dep_38 as (
    select * from {{ ref('stg_source_4') }}
),

dep_39 as (
    select * from {{ ref('stg_source_102') }}
),

dep_40 as (
    select * from {{ ref('stg_source_92') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_42 as (
    select * from {{ ref('stg_source_137') }}
),

dep_43 as (
    select * from {{ ref('stg_source_49') }}
),

dep_44 as (
    select * from {{ ref('stg_source_43') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_47 as (
    select * from {{ ref('stg_source_51') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_49 as (
    select * from {{ ref('stg_source_94') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_34') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_7_id']) }} as surrogate_id,
        dep_1.int_transformed_7_id as fct_business_event_33_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_7_id = dep_2.stg_source_40_id
    left join dep_3 on dep_1.int_transformed_7_id = dep_3.int_transformed_93_id
    left join dep_4 on dep_1.int_transformed_7_id = dep_4.int_transformed_14_id
    left join dep_5 on dep_1.int_transformed_7_id = dep_5.stg_source_16_id
    left join dep_6 on dep_1.int_transformed_7_id = dep_6.stg_source_57_id
    left join dep_7 on dep_1.int_transformed_7_id = dep_7.stg_source_13_id
    left join dep_8 on dep_1.int_transformed_7_id = dep_8.stg_source_20_id
    left join dep_9 on dep_1.int_transformed_7_id = dep_9.int_transformed_132_id
    left join dep_10 on dep_1.int_transformed_7_id = dep_10.stg_source_42_id
    left join dep_11 on dep_1.int_transformed_7_id = dep_11.int_transformed_116_id
    left join dep_12 on dep_1.int_transformed_7_id = dep_12.stg_source_18_id
    left join dep_13 on dep_1.int_transformed_7_id = dep_13.int_transformed_111_id
    left join dep_14 on dep_1.int_transformed_7_id = dep_14.int_transformed_61_id
    left join dep_15 on dep_1.int_transformed_7_id = dep_15.stg_source_2_id
    left join dep_16 on dep_1.int_transformed_7_id = dep_16.stg_source_147_id
    left join dep_17 on dep_1.int_transformed_7_id = dep_17.stg_source_135_id
    left join dep_18 on dep_1.int_transformed_7_id = dep_18.stg_source_22_id
    left join dep_19 on dep_1.int_transformed_7_id = dep_19.stg_source_58_id
    left join dep_20 on dep_1.int_transformed_7_id = dep_20.stg_source_104_id
    left join dep_21 on dep_1.int_transformed_7_id = dep_21.stg_source_24_id
    left join dep_22 on dep_1.int_transformed_7_id = dep_22.stg_source_29_id
    left join dep_23 on dep_1.int_transformed_7_id = dep_23.stg_source_143_id
    left join dep_24 on dep_1.int_transformed_7_id = dep_24.int_transformed_37_id
    left join dep_25 on dep_1.int_transformed_7_id = dep_25.stg_source_53_id
    left join dep_26 on dep_1.int_transformed_7_id = dep_26.int_transformed_52_id
    left join dep_27 on dep_1.int_transformed_7_id = dep_27.stg_source_86_id
    left join dep_28 on dep_1.int_transformed_7_id = dep_28.int_transformed_162_id
    left join dep_29 on dep_1.int_transformed_7_id = dep_29.int_transformed_135_id
    left join dep_30 on dep_1.int_transformed_7_id = dep_30.int_transformed_9_id
    left join dep_31 on dep_1.int_transformed_7_id = dep_31.int_transformed_51_id
    left join dep_32 on dep_1.int_transformed_7_id = dep_32.stg_source_73_id
    left join dep_33 on dep_1.int_transformed_7_id = dep_33.stg_source_41_id
    left join dep_34 on dep_1.int_transformed_7_id = dep_34.int_transformed_113_id
    left join dep_35 on dep_1.int_transformed_7_id = dep_35.int_transformed_22_id
    left join dep_36 on dep_1.int_transformed_7_id = dep_36.stg_source_126_id
    left join dep_37 on dep_1.int_transformed_7_id = dep_37.stg_source_133_id
    left join dep_38 on dep_1.int_transformed_7_id = dep_38.stg_source_4_id
    left join dep_39 on dep_1.int_transformed_7_id = dep_39.stg_source_102_id
    left join dep_40 on dep_1.int_transformed_7_id = dep_40.stg_source_92_id
    left join dep_41 on dep_1.int_transformed_7_id = dep_41.int_transformed_145_id
    left join dep_42 on dep_1.int_transformed_7_id = dep_42.stg_source_137_id
    left join dep_43 on dep_1.int_transformed_7_id = dep_43.stg_source_49_id
    left join dep_44 on dep_1.int_transformed_7_id = dep_44.stg_source_43_id
    left join dep_45 on dep_1.int_transformed_7_id = dep_45.int_transformed_20_id
    left join dep_46 on dep_1.int_transformed_7_id = dep_46.int_transformed_77_id
    left join dep_47 on dep_1.int_transformed_7_id = dep_47.stg_source_51_id
    left join dep_48 on dep_1.int_transformed_7_id = dep_48.int_transformed_23_id
    left join dep_49 on dep_1.int_transformed_7_id = dep_49.stg_source_94_id
    left join dep_50 on dep_1.int_transformed_7_id = dep_50.int_transformed_34_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
