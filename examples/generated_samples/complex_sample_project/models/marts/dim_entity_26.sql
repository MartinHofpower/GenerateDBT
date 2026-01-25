{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_2 as (
    select * from {{ ref('stg_source_143') }}
),

dep_3 as (
    select * from {{ ref('stg_source_58') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_5 as (
    select * from {{ ref('stg_source_89') }}
),

dep_6 as (
    select * from {{ ref('stg_source_154') }}
),

dep_7 as (
    select * from {{ ref('stg_source_55') }}
),

dep_8 as (
    select * from {{ ref('stg_source_12') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_10 as (
    select * from {{ ref('stg_source_119') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_12 as (
    select * from {{ ref('stg_source_97') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_14 as (
    select * from {{ ref('stg_source_126') }}
),

dep_15 as (
    select * from {{ ref('stg_source_7') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_96') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_21 as (
    select * from {{ ref('stg_source_64') }}
),

dep_22 as (
    select * from {{ ref('stg_source_114') }}
),

dep_23 as (
    select * from {{ ref('stg_source_133') }}
),

dep_24 as (
    select * from {{ ref('stg_source_27') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_26 as (
    select * from {{ ref('stg_source_99') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_28 as (
    select * from {{ ref('stg_source_30') }}
),

dep_29 as (
    select * from {{ ref('stg_source_122') }}
),

dep_30 as (
    select * from {{ ref('stg_source_79') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_32 as (
    select * from {{ ref('stg_source_109') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_34 as (
    select * from {{ ref('stg_source_107') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_36 as (
    select * from {{ ref('stg_source_16') }}
),

dep_37 as (
    select * from {{ ref('stg_source_77') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_39 as (
    select * from {{ ref('stg_source_20') }}
),

dep_40 as (
    select * from {{ ref('stg_source_125') }}
),

dep_41 as (
    select * from {{ ref('stg_source_41') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_44 as (
    select * from {{ ref('stg_source_116') }}
),

dep_45 as (
    select * from {{ ref('stg_source_96') }}
),

dep_46 as (
    select * from {{ ref('stg_source_160') }}
),

dep_47 as (
    select * from {{ ref('stg_source_54') }}
),

dep_48 as (
    select * from {{ ref('stg_source_129') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_11') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_33_id']) }} as surrogate_id,
        dep_1.int_transformed_33_id as dim_entity_26_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_33_id = dep_2.stg_source_143_id
    left join dep_3 on dep_1.int_transformed_33_id = dep_3.stg_source_58_id
    left join dep_4 on dep_1.int_transformed_33_id = dep_4.int_transformed_161_id
    left join dep_5 on dep_1.int_transformed_33_id = dep_5.stg_source_89_id
    left join dep_6 on dep_1.int_transformed_33_id = dep_6.stg_source_154_id
    left join dep_7 on dep_1.int_transformed_33_id = dep_7.stg_source_55_id
    left join dep_8 on dep_1.int_transformed_33_id = dep_8.stg_source_12_id
    left join dep_9 on dep_1.int_transformed_33_id = dep_9.int_transformed_77_id
    left join dep_10 on dep_1.int_transformed_33_id = dep_10.stg_source_119_id
    left join dep_11 on dep_1.int_transformed_33_id = dep_11.int_transformed_112_id
    left join dep_12 on dep_1.int_transformed_33_id = dep_12.stg_source_97_id
    left join dep_13 on dep_1.int_transformed_33_id = dep_13.int_transformed_140_id
    left join dep_14 on dep_1.int_transformed_33_id = dep_14.stg_source_126_id
    left join dep_15 on dep_1.int_transformed_33_id = dep_15.stg_source_7_id
    left join dep_16 on dep_1.int_transformed_33_id = dep_16.int_transformed_145_id
    left join dep_17 on dep_1.int_transformed_33_id = dep_17.int_transformed_122_id
    left join dep_18 on dep_1.int_transformed_33_id = dep_18.int_transformed_64_id
    left join dep_19 on dep_1.int_transformed_33_id = dep_19.int_transformed_96_id
    left join dep_20 on dep_1.int_transformed_33_id = dep_20.int_transformed_10_id
    left join dep_21 on dep_1.int_transformed_33_id = dep_21.stg_source_64_id
    left join dep_22 on dep_1.int_transformed_33_id = dep_22.stg_source_114_id
    left join dep_23 on dep_1.int_transformed_33_id = dep_23.stg_source_133_id
    left join dep_24 on dep_1.int_transformed_33_id = dep_24.stg_source_27_id
    left join dep_25 on dep_1.int_transformed_33_id = dep_25.int_transformed_143_id
    left join dep_26 on dep_1.int_transformed_33_id = dep_26.stg_source_99_id
    left join dep_27 on dep_1.int_transformed_33_id = dep_27.int_transformed_75_id
    left join dep_28 on dep_1.int_transformed_33_id = dep_28.stg_source_30_id
    left join dep_29 on dep_1.int_transformed_33_id = dep_29.stg_source_122_id
    left join dep_30 on dep_1.int_transformed_33_id = dep_30.stg_source_79_id
    left join dep_31 on dep_1.int_transformed_33_id = dep_31.int_transformed_50_id
    left join dep_32 on dep_1.int_transformed_33_id = dep_32.stg_source_109_id
    left join dep_33 on dep_1.int_transformed_33_id = dep_33.int_transformed_97_id
    left join dep_34 on dep_1.int_transformed_33_id = dep_34.stg_source_107_id
    left join dep_35 on dep_1.int_transformed_33_id = dep_35.int_transformed_29_id
    left join dep_36 on dep_1.int_transformed_33_id = dep_36.stg_source_16_id
    left join dep_37 on dep_1.int_transformed_33_id = dep_37.stg_source_77_id
    left join dep_38 on dep_1.int_transformed_33_id = dep_38.int_transformed_46_id
    left join dep_39 on dep_1.int_transformed_33_id = dep_39.stg_source_20_id
    left join dep_40 on dep_1.int_transformed_33_id = dep_40.stg_source_125_id
    left join dep_41 on dep_1.int_transformed_33_id = dep_41.stg_source_41_id
    left join dep_42 on dep_1.int_transformed_33_id = dep_42.int_transformed_87_id
    left join dep_43 on dep_1.int_transformed_33_id = dep_43.int_transformed_129_id
    left join dep_44 on dep_1.int_transformed_33_id = dep_44.stg_source_116_id
    left join dep_45 on dep_1.int_transformed_33_id = dep_45.stg_source_96_id
    left join dep_46 on dep_1.int_transformed_33_id = dep_46.stg_source_160_id
    left join dep_47 on dep_1.int_transformed_33_id = dep_47.stg_source_54_id
    left join dep_48 on dep_1.int_transformed_33_id = dep_48.stg_source_129_id
    left join dep_49 on dep_1.int_transformed_33_id = dep_49.int_transformed_100_id
    left join dep_50 on dep_1.int_transformed_33_id = dep_50.int_transformed_11_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
