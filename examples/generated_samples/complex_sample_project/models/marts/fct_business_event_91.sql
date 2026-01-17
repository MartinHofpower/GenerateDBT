{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_7 as (
    select * from {{ ref('stg_source_134') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_11 as (
    select * from {{ ref('stg_source_87') }}
),

dep_12 as (
    select * from {{ ref('stg_source_34') }}
),

dep_13 as (
    select * from {{ ref('stg_source_132') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_16 as (
    select * from {{ ref('stg_source_63') }}
),

dep_17 as (
    select * from {{ ref('stg_source_76') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_20 as (
    select * from {{ ref('stg_source_101') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_23 as (
    select * from {{ ref('stg_source_57') }}
),

dep_24 as (
    select * from {{ ref('stg_source_102') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_27 as (
    select * from {{ ref('stg_source_72') }}
),

dep_28 as (
    select * from {{ ref('stg_source_156') }}
),

dep_29 as (
    select * from {{ ref('stg_source_23') }}
),

dep_30 as (
    select * from {{ ref('stg_source_151') }}
),

dep_31 as (
    select * from {{ ref('stg_source_24') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_34 as (
    select * from {{ ref('stg_source_71') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_36 as (
    select * from {{ ref('stg_source_163') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_39 as (
    select * from {{ ref('stg_source_135') }}
),

dep_40 as (
    select * from {{ ref('stg_source_96') }}
),

dep_41 as (
    select * from {{ ref('stg_source_138') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_44 as (
    select * from {{ ref('stg_source_14') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_46 as (
    select * from {{ ref('stg_source_38') }}
),

dep_47 as (
    select * from {{ ref('stg_source_129') }}
),

dep_48 as (
    select * from {{ ref('stg_source_19') }}
),

dep_49 as (
    select * from {{ ref('stg_source_54') }}
),

dep_50 as (
    select * from {{ ref('stg_source_115') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_99_id']) }} as surrogate_id,
        dep_1.int_transformed_99_id as fct_business_event_91_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_99_id = dep_2.int_transformed_149_id
    left join dep_3 on dep_1.int_transformed_99_id = dep_3.int_transformed_62_id
    left join dep_4 on dep_1.int_transformed_99_id = dep_4.int_transformed_78_id
    left join dep_5 on dep_1.int_transformed_99_id = dep_5.int_transformed_50_id
    left join dep_6 on dep_1.int_transformed_99_id = dep_6.int_transformed_103_id
    left join dep_7 on dep_1.int_transformed_99_id = dep_7.stg_source_134_id
    left join dep_8 on dep_1.int_transformed_99_id = dep_8.int_transformed_117_id
    left join dep_9 on dep_1.int_transformed_99_id = dep_9.int_transformed_94_id
    left join dep_10 on dep_1.int_transformed_99_id = dep_10.int_transformed_153_id
    left join dep_11 on dep_1.int_transformed_99_id = dep_11.stg_source_87_id
    left join dep_12 on dep_1.int_transformed_99_id = dep_12.stg_source_34_id
    left join dep_13 on dep_1.int_transformed_99_id = dep_13.stg_source_132_id
    left join dep_14 on dep_1.int_transformed_99_id = dep_14.int_transformed_154_id
    left join dep_15 on dep_1.int_transformed_99_id = dep_15.int_transformed_129_id
    left join dep_16 on dep_1.int_transformed_99_id = dep_16.stg_source_63_id
    left join dep_17 on dep_1.int_transformed_99_id = dep_17.stg_source_76_id
    left join dep_18 on dep_1.int_transformed_99_id = dep_18.int_transformed_45_id
    left join dep_19 on dep_1.int_transformed_99_id = dep_19.int_transformed_86_id
    left join dep_20 on dep_1.int_transformed_99_id = dep_20.stg_source_101_id
    left join dep_21 on dep_1.int_transformed_99_id = dep_21.int_transformed_88_id
    left join dep_22 on dep_1.int_transformed_99_id = dep_22.int_transformed_131_id
    left join dep_23 on dep_1.int_transformed_99_id = dep_23.stg_source_57_id
    left join dep_24 on dep_1.int_transformed_99_id = dep_24.stg_source_102_id
    left join dep_25 on dep_1.int_transformed_99_id = dep_25.int_transformed_80_id
    left join dep_26 on dep_1.int_transformed_99_id = dep_26.int_transformed_42_id
    left join dep_27 on dep_1.int_transformed_99_id = dep_27.stg_source_72_id
    left join dep_28 on dep_1.int_transformed_99_id = dep_28.stg_source_156_id
    left join dep_29 on dep_1.int_transformed_99_id = dep_29.stg_source_23_id
    left join dep_30 on dep_1.int_transformed_99_id = dep_30.stg_source_151_id
    left join dep_31 on dep_1.int_transformed_99_id = dep_31.stg_source_24_id
    left join dep_32 on dep_1.int_transformed_99_id = dep_32.int_transformed_64_id
    left join dep_33 on dep_1.int_transformed_99_id = dep_33.int_transformed_75_id
    left join dep_34 on dep_1.int_transformed_99_id = dep_34.stg_source_71_id
    left join dep_35 on dep_1.int_transformed_99_id = dep_35.int_transformed_113_id
    left join dep_36 on dep_1.int_transformed_99_id = dep_36.stg_source_163_id
    left join dep_37 on dep_1.int_transformed_99_id = dep_37.int_transformed_126_id
    left join dep_38 on dep_1.int_transformed_99_id = dep_38.int_transformed_56_id
    left join dep_39 on dep_1.int_transformed_99_id = dep_39.stg_source_135_id
    left join dep_40 on dep_1.int_transformed_99_id = dep_40.stg_source_96_id
    left join dep_41 on dep_1.int_transformed_99_id = dep_41.stg_source_138_id
    left join dep_42 on dep_1.int_transformed_99_id = dep_42.int_transformed_93_id
    left join dep_43 on dep_1.int_transformed_99_id = dep_43.int_transformed_41_id
    left join dep_44 on dep_1.int_transformed_99_id = dep_44.stg_source_14_id
    left join dep_45 on dep_1.int_transformed_99_id = dep_45.int_transformed_8_id
    left join dep_46 on dep_1.int_transformed_99_id = dep_46.stg_source_38_id
    left join dep_47 on dep_1.int_transformed_99_id = dep_47.stg_source_129_id
    left join dep_48 on dep_1.int_transformed_99_id = dep_48.stg_source_19_id
    left join dep_49 on dep_1.int_transformed_99_id = dep_49.stg_source_54_id
    left join dep_50 on dep_1.int_transformed_99_id = dep_50.stg_source_115_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
