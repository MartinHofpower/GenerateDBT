{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_99') }}
),

dep_2 as (
    select * from {{ ref('stg_source_101') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_5 as (
    select * from {{ ref('stg_source_4') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_7 as (
    select * from {{ ref('stg_source_36') }}
),

dep_8 as (
    select * from {{ ref('stg_source_144') }}
),

dep_9 as (
    select * from {{ ref('stg_source_22') }}
),

dep_10 as (
    select * from {{ ref('stg_source_24') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_12 as (
    select * from {{ ref('stg_source_7') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_14 as (
    select * from {{ ref('stg_source_126') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_16 as (
    select * from {{ ref('stg_source_164') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_165') }}
),

dep_18 as (
    select * from {{ ref('stg_source_140') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_20 as (
    select * from {{ ref('stg_source_105') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_23 as (
    select * from {{ ref('stg_source_29') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_26 as (
    select * from {{ ref('stg_source_19') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_28 as (
    select * from {{ ref('stg_source_79') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_30 as (
    select * from {{ ref('stg_source_132') }}
),

dep_31 as (
    select * from {{ ref('stg_source_49') }}
),

dep_32 as (
    select * from {{ ref('stg_source_27') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_38 as (
    select * from {{ ref('stg_source_54') }}
),

dep_39 as (
    select * from {{ ref('stg_source_57') }}
),

dep_40 as (
    select * from {{ ref('stg_source_100') }}
),

dep_41 as (
    select * from {{ ref('stg_source_52') }}
),

dep_42 as (
    select * from {{ ref('stg_source_128') }}
),

dep_43 as (
    select * from {{ ref('stg_source_135') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_46 as (
    select * from {{ ref('stg_source_11') }}
),

dep_47 as (
    select * from {{ ref('stg_source_160') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_49 as (
    select * from {{ ref('stg_source_153') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_129') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_99_id']) }} as surrogate_id,
        dep_1.stg_source_99_id as fct_business_event_157_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_99_id = dep_2.stg_source_101_id
    left join dep_3 on dep_1.stg_source_99_id = dep_3.int_transformed_64_id
    left join dep_4 on dep_1.stg_source_99_id = dep_4.int_transformed_55_id
    left join dep_5 on dep_1.stg_source_99_id = dep_5.stg_source_4_id
    left join dep_6 on dep_1.stg_source_99_id = dep_6.int_transformed_46_id
    left join dep_7 on dep_1.stg_source_99_id = dep_7.stg_source_36_id
    left join dep_8 on dep_1.stg_source_99_id = dep_8.stg_source_144_id
    left join dep_9 on dep_1.stg_source_99_id = dep_9.stg_source_22_id
    left join dep_10 on dep_1.stg_source_99_id = dep_10.stg_source_24_id
    left join dep_11 on dep_1.stg_source_99_id = dep_11.int_transformed_52_id
    left join dep_12 on dep_1.stg_source_99_id = dep_12.stg_source_7_id
    left join dep_13 on dep_1.stg_source_99_id = dep_13.int_transformed_24_id
    left join dep_14 on dep_1.stg_source_99_id = dep_14.stg_source_126_id
    left join dep_15 on dep_1.stg_source_99_id = dep_15.int_transformed_154_id
    left join dep_16 on dep_1.stg_source_99_id = dep_16.stg_source_164_id
    left join dep_17 on dep_1.stg_source_99_id = dep_17.int_transformed_165_id
    left join dep_18 on dep_1.stg_source_99_id = dep_18.stg_source_140_id
    left join dep_19 on dep_1.stg_source_99_id = dep_19.int_transformed_138_id
    left join dep_20 on dep_1.stg_source_99_id = dep_20.stg_source_105_id
    left join dep_21 on dep_1.stg_source_99_id = dep_21.int_transformed_45_id
    left join dep_22 on dep_1.stg_source_99_id = dep_22.int_transformed_20_id
    left join dep_23 on dep_1.stg_source_99_id = dep_23.stg_source_29_id
    left join dep_24 on dep_1.stg_source_99_id = dep_24.int_transformed_101_id
    left join dep_25 on dep_1.stg_source_99_id = dep_25.int_transformed_137_id
    left join dep_26 on dep_1.stg_source_99_id = dep_26.stg_source_19_id
    left join dep_27 on dep_1.stg_source_99_id = dep_27.int_transformed_78_id
    left join dep_28 on dep_1.stg_source_99_id = dep_28.stg_source_79_id
    left join dep_29 on dep_1.stg_source_99_id = dep_29.int_transformed_147_id
    left join dep_30 on dep_1.stg_source_99_id = dep_30.stg_source_132_id
    left join dep_31 on dep_1.stg_source_99_id = dep_31.stg_source_49_id
    left join dep_32 on dep_1.stg_source_99_id = dep_32.stg_source_27_id
    left join dep_33 on dep_1.stg_source_99_id = dep_33.int_transformed_72_id
    left join dep_34 on dep_1.stg_source_99_id = dep_34.int_transformed_29_id
    left join dep_35 on dep_1.stg_source_99_id = dep_35.int_transformed_7_id
    left join dep_36 on dep_1.stg_source_99_id = dep_36.int_transformed_47_id
    left join dep_37 on dep_1.stg_source_99_id = dep_37.int_transformed_146_id
    left join dep_38 on dep_1.stg_source_99_id = dep_38.stg_source_54_id
    left join dep_39 on dep_1.stg_source_99_id = dep_39.stg_source_57_id
    left join dep_40 on dep_1.stg_source_99_id = dep_40.stg_source_100_id
    left join dep_41 on dep_1.stg_source_99_id = dep_41.stg_source_52_id
    left join dep_42 on dep_1.stg_source_99_id = dep_42.stg_source_128_id
    left join dep_43 on dep_1.stg_source_99_id = dep_43.stg_source_135_id
    left join dep_44 on dep_1.stg_source_99_id = dep_44.int_transformed_90_id
    left join dep_45 on dep_1.stg_source_99_id = dep_45.int_transformed_115_id
    left join dep_46 on dep_1.stg_source_99_id = dep_46.stg_source_11_id
    left join dep_47 on dep_1.stg_source_99_id = dep_47.stg_source_160_id
    left join dep_48 on dep_1.stg_source_99_id = dep_48.int_transformed_4_id
    left join dep_49 on dep_1.stg_source_99_id = dep_49.stg_source_153_id
    left join dep_50 on dep_1.stg_source_99_id = dep_50.int_transformed_129_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
