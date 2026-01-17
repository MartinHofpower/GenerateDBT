{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_57') }}
),

dep_2 as (
    select * from {{ ref('stg_source_108') }}
),

dep_3 as (
    select * from {{ ref('stg_source_12') }}
),

dep_4 as (
    select * from {{ ref('stg_source_79') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_6 as (
    select * from {{ ref('stg_source_151') }}
),

dep_7 as (
    select * from {{ ref('stg_source_17') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_9 as (
    select * from {{ ref('stg_source_159') }}
),

dep_10 as (
    select * from {{ ref('stg_source_15') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_15 as (
    select * from {{ ref('stg_source_68') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_21 as (
    select * from {{ ref('stg_source_150') }}
),

dep_22 as (
    select * from {{ ref('stg_source_73') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_25 as (
    select * from {{ ref('stg_source_97') }}
),

dep_26 as (
    select * from {{ ref('stg_source_154') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_28 as (
    select * from {{ ref('stg_source_58') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_30 as (
    select * from {{ ref('stg_source_54') }}
),

dep_31 as (
    select * from {{ ref('stg_source_117') }}
),

dep_32 as (
    select * from {{ ref('stg_source_99') }}
),

dep_33 as (
    select * from {{ ref('stg_source_119') }}
),

dep_34 as (
    select * from {{ ref('stg_source_102') }}
),

dep_35 as (
    select * from {{ ref('stg_source_30') }}
),

dep_36 as (
    select * from {{ ref('stg_source_84') }}
),

dep_37 as (
    select * from {{ ref('stg_source_153') }}
),

dep_38 as (
    select * from {{ ref('stg_source_122') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_41 as (
    select * from {{ ref('stg_source_26') }}
),

dep_42 as (
    select * from {{ ref('stg_source_96') }}
),

dep_43 as (
    select * from {{ ref('stg_source_100') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_47 as (
    select * from {{ ref('stg_source_132') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_50 as (
    select * from {{ ref('stg_source_112') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_57_id']) }} as surrogate_id,
        dep_1.stg_source_57_id as fct_business_event_53_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_57_id = dep_2.stg_source_108_id
    left join dep_3 on dep_1.stg_source_57_id = dep_3.stg_source_12_id
    left join dep_4 on dep_1.stg_source_57_id = dep_4.stg_source_79_id
    left join dep_5 on dep_1.stg_source_57_id = dep_5.int_transformed_11_id
    left join dep_6 on dep_1.stg_source_57_id = dep_6.stg_source_151_id
    left join dep_7 on dep_1.stg_source_57_id = dep_7.stg_source_17_id
    left join dep_8 on dep_1.stg_source_57_id = dep_8.int_transformed_12_id
    left join dep_9 on dep_1.stg_source_57_id = dep_9.stg_source_159_id
    left join dep_10 on dep_1.stg_source_57_id = dep_10.stg_source_15_id
    left join dep_11 on dep_1.stg_source_57_id = dep_11.int_transformed_104_id
    left join dep_12 on dep_1.stg_source_57_id = dep_12.int_transformed_145_id
    left join dep_13 on dep_1.stg_source_57_id = dep_13.int_transformed_105_id
    left join dep_14 on dep_1.stg_source_57_id = dep_14.int_transformed_33_id
    left join dep_15 on dep_1.stg_source_57_id = dep_15.stg_source_68_id
    left join dep_16 on dep_1.stg_source_57_id = dep_16.int_transformed_67_id
    left join dep_17 on dep_1.stg_source_57_id = dep_17.int_transformed_29_id
    left join dep_18 on dep_1.stg_source_57_id = dep_18.int_transformed_26_id
    left join dep_19 on dep_1.stg_source_57_id = dep_19.int_transformed_132_id
    left join dep_20 on dep_1.stg_source_57_id = dep_20.int_transformed_74_id
    left join dep_21 on dep_1.stg_source_57_id = dep_21.stg_source_150_id
    left join dep_22 on dep_1.stg_source_57_id = dep_22.stg_source_73_id
    left join dep_23 on dep_1.stg_source_57_id = dep_23.int_transformed_48_id
    left join dep_24 on dep_1.stg_source_57_id = dep_24.int_transformed_75_id
    left join dep_25 on dep_1.stg_source_57_id = dep_25.stg_source_97_id
    left join dep_26 on dep_1.stg_source_57_id = dep_26.stg_source_154_id
    left join dep_27 on dep_1.stg_source_57_id = dep_27.int_transformed_149_id
    left join dep_28 on dep_1.stg_source_57_id = dep_28.stg_source_58_id
    left join dep_29 on dep_1.stg_source_57_id = dep_29.int_transformed_85_id
    left join dep_30 on dep_1.stg_source_57_id = dep_30.stg_source_54_id
    left join dep_31 on dep_1.stg_source_57_id = dep_31.stg_source_117_id
    left join dep_32 on dep_1.stg_source_57_id = dep_32.stg_source_99_id
    left join dep_33 on dep_1.stg_source_57_id = dep_33.stg_source_119_id
    left join dep_34 on dep_1.stg_source_57_id = dep_34.stg_source_102_id
    left join dep_35 on dep_1.stg_source_57_id = dep_35.stg_source_30_id
    left join dep_36 on dep_1.stg_source_57_id = dep_36.stg_source_84_id
    left join dep_37 on dep_1.stg_source_57_id = dep_37.stg_source_153_id
    left join dep_38 on dep_1.stg_source_57_id = dep_38.stg_source_122_id
    left join dep_39 on dep_1.stg_source_57_id = dep_39.int_transformed_122_id
    left join dep_40 on dep_1.stg_source_57_id = dep_40.int_transformed_62_id
    left join dep_41 on dep_1.stg_source_57_id = dep_41.stg_source_26_id
    left join dep_42 on dep_1.stg_source_57_id = dep_42.stg_source_96_id
    left join dep_43 on dep_1.stg_source_57_id = dep_43.stg_source_100_id
    left join dep_44 on dep_1.stg_source_57_id = dep_44.int_transformed_24_id
    left join dep_45 on dep_1.stg_source_57_id = dep_45.int_transformed_128_id
    left join dep_46 on dep_1.stg_source_57_id = dep_46.int_transformed_66_id
    left join dep_47 on dep_1.stg_source_57_id = dep_47.stg_source_132_id
    left join dep_48 on dep_1.stg_source_57_id = dep_48.int_transformed_16_id
    left join dep_49 on dep_1.stg_source_57_id = dep_49.int_transformed_166_id
    left join dep_50 on dep_1.stg_source_57_id = dep_50.stg_source_112_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
