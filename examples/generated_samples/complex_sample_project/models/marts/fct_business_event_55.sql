{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_51') }}
),

dep_2 as (
    select * from {{ ref('stg_source_35') }}
),

dep_3 as (
    select * from {{ ref('stg_source_109') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_5 as (
    select * from {{ ref('stg_source_54') }}
),

dep_6 as (
    select * from {{ ref('stg_source_49') }}
),

dep_7 as (
    select * from {{ ref('stg_source_17') }}
),

dep_8 as (
    select * from {{ ref('stg_source_122') }}
),

dep_9 as (
    select * from {{ ref('stg_source_67') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_11 as (
    select * from {{ ref('stg_source_37') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_13 as (
    select * from {{ ref('stg_source_113') }}
),

dep_14 as (
    select * from {{ ref('stg_source_11') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_20 as (
    select * from {{ ref('stg_source_77') }}
),

dep_21 as (
    select * from {{ ref('stg_source_141') }}
),

dep_22 as (
    select * from {{ ref('stg_source_65') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_27 as (
    select * from {{ ref('stg_source_135') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_29 as (
    select * from {{ ref('stg_source_151') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_34 as (
    select * from {{ ref('stg_source_61') }}
),

dep_35 as (
    select * from {{ ref('stg_source_71') }}
),

dep_36 as (
    select * from {{ ref('stg_source_3') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_38 as (
    select * from {{ ref('stg_source_84') }}
),

dep_39 as (
    select * from {{ ref('stg_source_53') }}
),

dep_40 as (
    select * from {{ ref('stg_source_38') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_43 as (
    select * from {{ ref('stg_source_27') }}
),

dep_44 as (
    select * from {{ ref('stg_source_147') }}
),

dep_45 as (
    select * from {{ ref('stg_source_45') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_47 as (
    select * from {{ ref('stg_source_21') }}
),

dep_48 as (
    select * from {{ ref('stg_source_36') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_138') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_51_id']) }} as surrogate_id,
        dep_1.stg_source_51_id as fct_business_event_55_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_51_id = dep_2.stg_source_35_id
    left join dep_3 on dep_1.stg_source_51_id = dep_3.stg_source_109_id
    left join dep_4 on dep_1.stg_source_51_id = dep_4.int_transformed_14_id
    left join dep_5 on dep_1.stg_source_51_id = dep_5.stg_source_54_id
    left join dep_6 on dep_1.stg_source_51_id = dep_6.stg_source_49_id
    left join dep_7 on dep_1.stg_source_51_id = dep_7.stg_source_17_id
    left join dep_8 on dep_1.stg_source_51_id = dep_8.stg_source_122_id
    left join dep_9 on dep_1.stg_source_51_id = dep_9.stg_source_67_id
    left join dep_10 on dep_1.stg_source_51_id = dep_10.int_transformed_101_id
    left join dep_11 on dep_1.stg_source_51_id = dep_11.stg_source_37_id
    left join dep_12 on dep_1.stg_source_51_id = dep_12.int_transformed_29_id
    left join dep_13 on dep_1.stg_source_51_id = dep_13.stg_source_113_id
    left join dep_14 on dep_1.stg_source_51_id = dep_14.stg_source_11_id
    left join dep_15 on dep_1.stg_source_51_id = dep_15.int_transformed_10_id
    left join dep_16 on dep_1.stg_source_51_id = dep_16.int_transformed_148_id
    left join dep_17 on dep_1.stg_source_51_id = dep_17.int_transformed_8_id
    left join dep_18 on dep_1.stg_source_51_id = dep_18.int_transformed_102_id
    left join dep_19 on dep_1.stg_source_51_id = dep_19.int_transformed_151_id
    left join dep_20 on dep_1.stg_source_51_id = dep_20.stg_source_77_id
    left join dep_21 on dep_1.stg_source_51_id = dep_21.stg_source_141_id
    left join dep_22 on dep_1.stg_source_51_id = dep_22.stg_source_65_id
    left join dep_23 on dep_1.stg_source_51_id = dep_23.int_transformed_159_id
    left join dep_24 on dep_1.stg_source_51_id = dep_24.int_transformed_97_id
    left join dep_25 on dep_1.stg_source_51_id = dep_25.int_transformed_150_id
    left join dep_26 on dep_1.stg_source_51_id = dep_26.int_transformed_140_id
    left join dep_27 on dep_1.stg_source_51_id = dep_27.stg_source_135_id
    left join dep_28 on dep_1.stg_source_51_id = dep_28.int_transformed_68_id
    left join dep_29 on dep_1.stg_source_51_id = dep_29.stg_source_151_id
    left join dep_30 on dep_1.stg_source_51_id = dep_30.int_transformed_15_id
    left join dep_31 on dep_1.stg_source_51_id = dep_31.int_transformed_96_id
    left join dep_32 on dep_1.stg_source_51_id = dep_32.int_transformed_93_id
    left join dep_33 on dep_1.stg_source_51_id = dep_33.int_transformed_85_id
    left join dep_34 on dep_1.stg_source_51_id = dep_34.stg_source_61_id
    left join dep_35 on dep_1.stg_source_51_id = dep_35.stg_source_71_id
    left join dep_36 on dep_1.stg_source_51_id = dep_36.stg_source_3_id
    left join dep_37 on dep_1.stg_source_51_id = dep_37.int_transformed_42_id
    left join dep_38 on dep_1.stg_source_51_id = dep_38.stg_source_84_id
    left join dep_39 on dep_1.stg_source_51_id = dep_39.stg_source_53_id
    left join dep_40 on dep_1.stg_source_51_id = dep_40.stg_source_38_id
    left join dep_41 on dep_1.stg_source_51_id = dep_41.int_transformed_64_id
    left join dep_42 on dep_1.stg_source_51_id = dep_42.int_transformed_94_id
    left join dep_43 on dep_1.stg_source_51_id = dep_43.stg_source_27_id
    left join dep_44 on dep_1.stg_source_51_id = dep_44.stg_source_147_id
    left join dep_45 on dep_1.stg_source_51_id = dep_45.stg_source_45_id
    left join dep_46 on dep_1.stg_source_51_id = dep_46.int_transformed_111_id
    left join dep_47 on dep_1.stg_source_51_id = dep_47.stg_source_21_id
    left join dep_48 on dep_1.stg_source_51_id = dep_48.stg_source_36_id
    left join dep_49 on dep_1.stg_source_51_id = dep_49.int_transformed_25_id
    left join dep_50 on dep_1.stg_source_51_id = dep_50.int_transformed_138_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
