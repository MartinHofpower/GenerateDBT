{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_2 as (
    select * from {{ ref('stg_source_125') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_7 as (
    select * from {{ ref('stg_source_80') }}
),

dep_8 as (
    select * from {{ ref('stg_source_104') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_10 as (
    select * from {{ ref('stg_source_67') }}
),

dep_11 as (
    select * from {{ ref('stg_source_8') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_15 as (
    select * from {{ ref('stg_source_17') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_19 as (
    select * from {{ ref('stg_source_165') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_21 as (
    select * from {{ ref('stg_source_54') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_25 as (
    select * from {{ ref('stg_source_38') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_27 as (
    select * from {{ ref('stg_source_136') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_29 as (
    select * from {{ ref('stg_source_55') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_32 as (
    select * from {{ ref('stg_source_103') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_34 as (
    select * from {{ ref('stg_source_161') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_37 as (
    select * from {{ ref('stg_source_160') }}
),

dep_38 as (
    select * from {{ ref('stg_source_42') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_40 as (
    select * from {{ ref('stg_source_37') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_43 as (
    select * from {{ ref('stg_source_68') }}
),

dep_44 as (
    select * from {{ ref('stg_source_72') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_46 as (
    select * from {{ ref('stg_source_57') }}
),

dep_47 as (
    select * from {{ ref('stg_source_18') }}
),

dep_48 as (
    select * from {{ ref('stg_source_2') }}
),

dep_49 as (
    select * from {{ ref('stg_source_119') }}
),

dep_50 as (
    select * from {{ ref('stg_source_128') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_72_id']) }} as surrogate_id,
        dep_1.int_transformed_72_id as fct_business_event_35_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_72_id = dep_2.stg_source_125_id
    left join dep_3 on dep_1.int_transformed_72_id = dep_3.int_transformed_114_id
    left join dep_4 on dep_1.int_transformed_72_id = dep_4.int_transformed_66_id
    left join dep_5 on dep_1.int_transformed_72_id = dep_5.int_transformed_51_id
    left join dep_6 on dep_1.int_transformed_72_id = dep_6.int_transformed_158_id
    left join dep_7 on dep_1.int_transformed_72_id = dep_7.stg_source_80_id
    left join dep_8 on dep_1.int_transformed_72_id = dep_8.stg_source_104_id
    left join dep_9 on dep_1.int_transformed_72_id = dep_9.int_transformed_150_id
    left join dep_10 on dep_1.int_transformed_72_id = dep_10.stg_source_67_id
    left join dep_11 on dep_1.int_transformed_72_id = dep_11.stg_source_8_id
    left join dep_12 on dep_1.int_transformed_72_id = dep_12.int_transformed_28_id
    left join dep_13 on dep_1.int_transformed_72_id = dep_13.int_transformed_96_id
    left join dep_14 on dep_1.int_transformed_72_id = dep_14.int_transformed_164_id
    left join dep_15 on dep_1.int_transformed_72_id = dep_15.stg_source_17_id
    left join dep_16 on dep_1.int_transformed_72_id = dep_16.int_transformed_131_id
    left join dep_17 on dep_1.int_transformed_72_id = dep_17.int_transformed_112_id
    left join dep_18 on dep_1.int_transformed_72_id = dep_18.int_transformed_79_id
    left join dep_19 on dep_1.int_transformed_72_id = dep_19.stg_source_165_id
    left join dep_20 on dep_1.int_transformed_72_id = dep_20.int_transformed_30_id
    left join dep_21 on dep_1.int_transformed_72_id = dep_21.stg_source_54_id
    left join dep_22 on dep_1.int_transformed_72_id = dep_22.int_transformed_19_id
    left join dep_23 on dep_1.int_transformed_72_id = dep_23.int_transformed_80_id
    left join dep_24 on dep_1.int_transformed_72_id = dep_24.int_transformed_23_id
    left join dep_25 on dep_1.int_transformed_72_id = dep_25.stg_source_38_id
    left join dep_26 on dep_1.int_transformed_72_id = dep_26.int_transformed_155_id
    left join dep_27 on dep_1.int_transformed_72_id = dep_27.stg_source_136_id
    left join dep_28 on dep_1.int_transformed_72_id = dep_28.int_transformed_2_id
    left join dep_29 on dep_1.int_transformed_72_id = dep_29.stg_source_55_id
    left join dep_30 on dep_1.int_transformed_72_id = dep_30.int_transformed_89_id
    left join dep_31 on dep_1.int_transformed_72_id = dep_31.int_transformed_154_id
    left join dep_32 on dep_1.int_transformed_72_id = dep_32.stg_source_103_id
    left join dep_33 on dep_1.int_transformed_72_id = dep_33.int_transformed_142_id
    left join dep_34 on dep_1.int_transformed_72_id = dep_34.stg_source_161_id
    left join dep_35 on dep_1.int_transformed_72_id = dep_35.int_transformed_111_id
    left join dep_36 on dep_1.int_transformed_72_id = dep_36.int_transformed_12_id
    left join dep_37 on dep_1.int_transformed_72_id = dep_37.stg_source_160_id
    left join dep_38 on dep_1.int_transformed_72_id = dep_38.stg_source_42_id
    left join dep_39 on dep_1.int_transformed_72_id = dep_39.int_transformed_152_id
    left join dep_40 on dep_1.int_transformed_72_id = dep_40.stg_source_37_id
    left join dep_41 on dep_1.int_transformed_72_id = dep_41.int_transformed_42_id
    left join dep_42 on dep_1.int_transformed_72_id = dep_42.int_transformed_43_id
    left join dep_43 on dep_1.int_transformed_72_id = dep_43.stg_source_68_id
    left join dep_44 on dep_1.int_transformed_72_id = dep_44.stg_source_72_id
    left join dep_45 on dep_1.int_transformed_72_id = dep_45.int_transformed_49_id
    left join dep_46 on dep_1.int_transformed_72_id = dep_46.stg_source_57_id
    left join dep_47 on dep_1.int_transformed_72_id = dep_47.stg_source_18_id
    left join dep_48 on dep_1.int_transformed_72_id = dep_48.stg_source_2_id
    left join dep_49 on dep_1.int_transformed_72_id = dep_49.stg_source_119_id
    left join dep_50 on dep_1.int_transformed_72_id = dep_50.stg_source_128_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
