{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_69') }}
),

dep_3 as (
    select * from {{ ref('stg_source_62') }}
),

dep_4 as (
    select * from {{ ref('stg_source_123') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_6 as (
    select * from {{ ref('stg_source_32') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_8 as (
    select * from {{ ref('stg_source_166') }}
),

dep_9 as (
    select * from {{ ref('stg_source_159') }}
),

dep_10 as (
    select * from {{ ref('stg_source_54') }}
),

dep_11 as (
    select * from {{ ref('stg_source_94') }}
),

dep_12 as (
    select * from {{ ref('stg_source_22') }}
),

dep_13 as (
    select * from {{ ref('stg_source_9') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_18 as (
    select * from {{ ref('stg_source_83') }}
),

dep_19 as (
    select * from {{ ref('stg_source_47') }}
),

dep_20 as (
    select * from {{ ref('stg_source_53') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_22 as (
    select * from {{ ref('stg_source_39') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_24 as (
    select * from {{ ref('stg_source_136') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_26 as (
    select * from {{ ref('stg_source_55') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_28 as (
    select * from {{ ref('stg_source_138') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_38 as (
    select * from {{ ref('stg_source_135') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_41 as (
    select * from {{ ref('stg_source_116') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_43 as (
    select * from {{ ref('stg_source_19') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_21') }}
),

dep_46 as (
    select * from {{ ref('stg_source_36') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_48 as (
    select * from {{ ref('stg_source_69') }}
),

dep_49 as (
    select * from {{ ref('stg_source_120') }}
),

dep_50 as (
    select * from {{ ref('stg_source_154') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_155_id']) }} as surrogate_id,
        dep_1.int_transformed_155_id as fct_business_event_119_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_155_id = dep_2.int_transformed_69_id
    left join dep_3 on dep_1.int_transformed_155_id = dep_3.stg_source_62_id
    left join dep_4 on dep_1.int_transformed_155_id = dep_4.stg_source_123_id
    left join dep_5 on dep_1.int_transformed_155_id = dep_5.int_transformed_18_id
    left join dep_6 on dep_1.int_transformed_155_id = dep_6.stg_source_32_id
    left join dep_7 on dep_1.int_transformed_155_id = dep_7.int_transformed_84_id
    left join dep_8 on dep_1.int_transformed_155_id = dep_8.stg_source_166_id
    left join dep_9 on dep_1.int_transformed_155_id = dep_9.stg_source_159_id
    left join dep_10 on dep_1.int_transformed_155_id = dep_10.stg_source_54_id
    left join dep_11 on dep_1.int_transformed_155_id = dep_11.stg_source_94_id
    left join dep_12 on dep_1.int_transformed_155_id = dep_12.stg_source_22_id
    left join dep_13 on dep_1.int_transformed_155_id = dep_13.stg_source_9_id
    left join dep_14 on dep_1.int_transformed_155_id = dep_14.int_transformed_40_id
    left join dep_15 on dep_1.int_transformed_155_id = dep_15.int_transformed_68_id
    left join dep_16 on dep_1.int_transformed_155_id = dep_16.int_transformed_24_id
    left join dep_17 on dep_1.int_transformed_155_id = dep_17.int_transformed_8_id
    left join dep_18 on dep_1.int_transformed_155_id = dep_18.stg_source_83_id
    left join dep_19 on dep_1.int_transformed_155_id = dep_19.stg_source_47_id
    left join dep_20 on dep_1.int_transformed_155_id = dep_20.stg_source_53_id
    left join dep_21 on dep_1.int_transformed_155_id = dep_21.int_transformed_115_id
    left join dep_22 on dep_1.int_transformed_155_id = dep_22.stg_source_39_id
    left join dep_23 on dep_1.int_transformed_155_id = dep_23.int_transformed_100_id
    left join dep_24 on dep_1.int_transformed_155_id = dep_24.stg_source_136_id
    left join dep_25 on dep_1.int_transformed_155_id = dep_25.int_transformed_73_id
    left join dep_26 on dep_1.int_transformed_155_id = dep_26.stg_source_55_id
    left join dep_27 on dep_1.int_transformed_155_id = dep_27.int_transformed_143_id
    left join dep_28 on dep_1.int_transformed_155_id = dep_28.stg_source_138_id
    left join dep_29 on dep_1.int_transformed_155_id = dep_29.int_transformed_66_id
    left join dep_30 on dep_1.int_transformed_155_id = dep_30.int_transformed_35_id
    left join dep_31 on dep_1.int_transformed_155_id = dep_31.int_transformed_117_id
    left join dep_32 on dep_1.int_transformed_155_id = dep_32.int_transformed_94_id
    left join dep_33 on dep_1.int_transformed_155_id = dep_33.int_transformed_98_id
    left join dep_34 on dep_1.int_transformed_155_id = dep_34.int_transformed_128_id
    left join dep_35 on dep_1.int_transformed_155_id = dep_35.int_transformed_89_id
    left join dep_36 on dep_1.int_transformed_155_id = dep_36.int_transformed_106_id
    left join dep_37 on dep_1.int_transformed_155_id = dep_37.int_transformed_61_id
    left join dep_38 on dep_1.int_transformed_155_id = dep_38.stg_source_135_id
    left join dep_39 on dep_1.int_transformed_155_id = dep_39.int_transformed_29_id
    left join dep_40 on dep_1.int_transformed_155_id = dep_40.int_transformed_122_id
    left join dep_41 on dep_1.int_transformed_155_id = dep_41.stg_source_116_id
    left join dep_42 on dep_1.int_transformed_155_id = dep_42.int_transformed_134_id
    left join dep_43 on dep_1.int_transformed_155_id = dep_43.stg_source_19_id
    left join dep_44 on dep_1.int_transformed_155_id = dep_44.int_transformed_140_id
    left join dep_45 on dep_1.int_transformed_155_id = dep_45.int_transformed_21_id
    left join dep_46 on dep_1.int_transformed_155_id = dep_46.stg_source_36_id
    left join dep_47 on dep_1.int_transformed_155_id = dep_47.int_transformed_60_id
    left join dep_48 on dep_1.int_transformed_155_id = dep_48.stg_source_69_id
    left join dep_49 on dep_1.int_transformed_155_id = dep_49.stg_source_120_id
    left join dep_50 on dep_1.int_transformed_155_id = dep_50.stg_source_154_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
