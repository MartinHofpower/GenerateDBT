{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_4 as (
    select * from {{ ref('stg_source_33') }}
),

dep_5 as (
    select * from {{ ref('stg_source_165') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_9 as (
    select * from {{ ref('stg_source_25') }}
),

dep_10 as (
    select * from {{ ref('stg_source_61') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_12 as (
    select * from {{ ref('stg_source_41') }}
),

dep_13 as (
    select * from {{ ref('stg_source_42') }}
),

dep_14 as (
    select * from {{ ref('stg_source_117') }}
),

dep_15 as (
    select * from {{ ref('stg_source_144') }}
),

dep_16 as (
    select * from {{ ref('stg_source_71') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_21 as (
    select * from {{ ref('stg_source_108') }}
),

dep_22 as (
    select * from {{ ref('stg_source_26') }}
),

dep_23 as (
    select * from {{ ref('stg_source_20') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_25 as (
    select * from {{ ref('stg_source_158') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_27 as (
    select * from {{ ref('stg_source_3') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_29 as (
    select * from {{ ref('stg_source_161') }}
),

dep_30 as (
    select * from {{ ref('stg_source_34') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_32 as (
    select * from {{ ref('stg_source_109') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_37 as (
    select * from {{ ref('stg_source_143') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_39 as (
    select * from {{ ref('stg_source_96') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_43 as (
    select * from {{ ref('stg_source_63') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_46 as (
    select * from {{ ref('stg_source_83') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_48 as (
    select * from {{ ref('stg_source_138') }}
),

dep_49 as (
    select * from {{ ref('stg_source_8') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_117') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_80_id']) }} as surrogate_id,
        dep_1.int_transformed_80_id as dim_entity_138_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_80_id = dep_2.int_transformed_75_id
    left join dep_3 on dep_1.int_transformed_80_id = dep_3.int_transformed_89_id
    left join dep_4 on dep_1.int_transformed_80_id = dep_4.stg_source_33_id
    left join dep_5 on dep_1.int_transformed_80_id = dep_5.stg_source_165_id
    left join dep_6 on dep_1.int_transformed_80_id = dep_6.int_transformed_57_id
    left join dep_7 on dep_1.int_transformed_80_id = dep_7.int_transformed_114_id
    left join dep_8 on dep_1.int_transformed_80_id = dep_8.int_transformed_19_id
    left join dep_9 on dep_1.int_transformed_80_id = dep_9.stg_source_25_id
    left join dep_10 on dep_1.int_transformed_80_id = dep_10.stg_source_61_id
    left join dep_11 on dep_1.int_transformed_80_id = dep_11.int_transformed_123_id
    left join dep_12 on dep_1.int_transformed_80_id = dep_12.stg_source_41_id
    left join dep_13 on dep_1.int_transformed_80_id = dep_13.stg_source_42_id
    left join dep_14 on dep_1.int_transformed_80_id = dep_14.stg_source_117_id
    left join dep_15 on dep_1.int_transformed_80_id = dep_15.stg_source_144_id
    left join dep_16 on dep_1.int_transformed_80_id = dep_16.stg_source_71_id
    left join dep_17 on dep_1.int_transformed_80_id = dep_17.int_transformed_147_id
    left join dep_18 on dep_1.int_transformed_80_id = dep_18.int_transformed_93_id
    left join dep_19 on dep_1.int_transformed_80_id = dep_19.int_transformed_50_id
    left join dep_20 on dep_1.int_transformed_80_id = dep_20.int_transformed_66_id
    left join dep_21 on dep_1.int_transformed_80_id = dep_21.stg_source_108_id
    left join dep_22 on dep_1.int_transformed_80_id = dep_22.stg_source_26_id
    left join dep_23 on dep_1.int_transformed_80_id = dep_23.stg_source_20_id
    left join dep_24 on dep_1.int_transformed_80_id = dep_24.int_transformed_84_id
    left join dep_25 on dep_1.int_transformed_80_id = dep_25.stg_source_158_id
    left join dep_26 on dep_1.int_transformed_80_id = dep_26.int_transformed_104_id
    left join dep_27 on dep_1.int_transformed_80_id = dep_27.stg_source_3_id
    left join dep_28 on dep_1.int_transformed_80_id = dep_28.int_transformed_164_id
    left join dep_29 on dep_1.int_transformed_80_id = dep_29.stg_source_161_id
    left join dep_30 on dep_1.int_transformed_80_id = dep_30.stg_source_34_id
    left join dep_31 on dep_1.int_transformed_80_id = dep_31.int_transformed_67_id
    left join dep_32 on dep_1.int_transformed_80_id = dep_32.stg_source_109_id
    left join dep_33 on dep_1.int_transformed_80_id = dep_33.int_transformed_121_id
    left join dep_34 on dep_1.int_transformed_80_id = dep_34.int_transformed_95_id
    left join dep_35 on dep_1.int_transformed_80_id = dep_35.int_transformed_79_id
    left join dep_36 on dep_1.int_transformed_80_id = dep_36.int_transformed_64_id
    left join dep_37 on dep_1.int_transformed_80_id = dep_37.stg_source_143_id
    left join dep_38 on dep_1.int_transformed_80_id = dep_38.int_transformed_20_id
    left join dep_39 on dep_1.int_transformed_80_id = dep_39.stg_source_96_id
    left join dep_40 on dep_1.int_transformed_80_id = dep_40.int_transformed_68_id
    left join dep_41 on dep_1.int_transformed_80_id = dep_41.int_transformed_153_id
    left join dep_42 on dep_1.int_transformed_80_id = dep_42.int_transformed_45_id
    left join dep_43 on dep_1.int_transformed_80_id = dep_43.stg_source_63_id
    left join dep_44 on dep_1.int_transformed_80_id = dep_44.int_transformed_71_id
    left join dep_45 on dep_1.int_transformed_80_id = dep_45.int_transformed_158_id
    left join dep_46 on dep_1.int_transformed_80_id = dep_46.stg_source_83_id
    left join dep_47 on dep_1.int_transformed_80_id = dep_47.int_transformed_51_id
    left join dep_48 on dep_1.int_transformed_80_id = dep_48.stg_source_138_id
    left join dep_49 on dep_1.int_transformed_80_id = dep_49.stg_source_8_id
    left join dep_50 on dep_1.int_transformed_80_id = dep_50.int_transformed_117_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
