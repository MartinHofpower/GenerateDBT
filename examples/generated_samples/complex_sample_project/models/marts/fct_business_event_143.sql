{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_2 as (
    select * from {{ ref('stg_source_48') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_4 as (
    select * from {{ ref('stg_source_104') }}
),

dep_5 as (
    select * from {{ ref('stg_source_77') }}
),

dep_6 as (
    select * from {{ ref('stg_source_112') }}
),

dep_7 as (
    select * from {{ ref('stg_source_36') }}
),

dep_8 as (
    select * from {{ ref('stg_source_55') }}
),

dep_9 as (
    select * from {{ ref('stg_source_80') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_11 as (
    select * from {{ ref('stg_source_147') }}
),

dep_12 as (
    select * from {{ ref('stg_source_162') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_14 as (
    select * from {{ ref('stg_source_51') }}
),

dep_15 as (
    select * from {{ ref('stg_source_155') }}
),

dep_16 as (
    select * from {{ ref('stg_source_25') }}
),

dep_17 as (
    select * from {{ ref('stg_source_160') }}
),

dep_18 as (
    select * from {{ ref('stg_source_86') }}
),

dep_19 as (
    select * from {{ ref('stg_source_22') }}
),

dep_20 as (
    select * from {{ ref('stg_source_38') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_22 as (
    select * from {{ ref('stg_source_53') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_24 as (
    select * from {{ ref('stg_source_75') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_26 as (
    select * from {{ ref('stg_source_50') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_29 as (
    select * from {{ ref('stg_source_63') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_49') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_33 as (
    select * from {{ ref('stg_source_12') }}
),

dep_34 as (
    select * from {{ ref('stg_source_145') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_37 as (
    select * from {{ ref('stg_source_121') }}
),

dep_38 as (
    select * from {{ ref('stg_source_11') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_40 as (
    select * from {{ ref('stg_source_164') }}
),

dep_41 as (
    select * from {{ ref('stg_source_90') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_44 as (
    select * from {{ ref('stg_source_157') }}
),

dep_45 as (
    select * from {{ ref('stg_source_146') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_48 as (
    select * from {{ ref('stg_source_83') }}
),

dep_49 as (
    select * from {{ ref('stg_source_153') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_7') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_157_id']) }} as surrogate_id,
        dep_1.int_transformed_157_id as fct_business_event_143_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_157_id = dep_2.stg_source_48_id
    left join dep_3 on dep_1.int_transformed_157_id = dep_3.int_transformed_85_id
    left join dep_4 on dep_1.int_transformed_157_id = dep_4.stg_source_104_id
    left join dep_5 on dep_1.int_transformed_157_id = dep_5.stg_source_77_id
    left join dep_6 on dep_1.int_transformed_157_id = dep_6.stg_source_112_id
    left join dep_7 on dep_1.int_transformed_157_id = dep_7.stg_source_36_id
    left join dep_8 on dep_1.int_transformed_157_id = dep_8.stg_source_55_id
    left join dep_9 on dep_1.int_transformed_157_id = dep_9.stg_source_80_id
    left join dep_10 on dep_1.int_transformed_157_id = dep_10.int_transformed_132_id
    left join dep_11 on dep_1.int_transformed_157_id = dep_11.stg_source_147_id
    left join dep_12 on dep_1.int_transformed_157_id = dep_12.stg_source_162_id
    left join dep_13 on dep_1.int_transformed_157_id = dep_13.int_transformed_71_id
    left join dep_14 on dep_1.int_transformed_157_id = dep_14.stg_source_51_id
    left join dep_15 on dep_1.int_transformed_157_id = dep_15.stg_source_155_id
    left join dep_16 on dep_1.int_transformed_157_id = dep_16.stg_source_25_id
    left join dep_17 on dep_1.int_transformed_157_id = dep_17.stg_source_160_id
    left join dep_18 on dep_1.int_transformed_157_id = dep_18.stg_source_86_id
    left join dep_19 on dep_1.int_transformed_157_id = dep_19.stg_source_22_id
    left join dep_20 on dep_1.int_transformed_157_id = dep_20.stg_source_38_id
    left join dep_21 on dep_1.int_transformed_157_id = dep_21.int_transformed_60_id
    left join dep_22 on dep_1.int_transformed_157_id = dep_22.stg_source_53_id
    left join dep_23 on dep_1.int_transformed_157_id = dep_23.int_transformed_119_id
    left join dep_24 on dep_1.int_transformed_157_id = dep_24.stg_source_75_id
    left join dep_25 on dep_1.int_transformed_157_id = dep_25.int_transformed_43_id
    left join dep_26 on dep_1.int_transformed_157_id = dep_26.stg_source_50_id
    left join dep_27 on dep_1.int_transformed_157_id = dep_27.int_transformed_153_id
    left join dep_28 on dep_1.int_transformed_157_id = dep_28.int_transformed_5_id
    left join dep_29 on dep_1.int_transformed_157_id = dep_29.stg_source_63_id
    left join dep_30 on dep_1.int_transformed_157_id = dep_30.int_transformed_94_id
    left join dep_31 on dep_1.int_transformed_157_id = dep_31.int_transformed_49_id
    left join dep_32 on dep_1.int_transformed_157_id = dep_32.int_transformed_144_id
    left join dep_33 on dep_1.int_transformed_157_id = dep_33.stg_source_12_id
    left join dep_34 on dep_1.int_transformed_157_id = dep_34.stg_source_145_id
    left join dep_35 on dep_1.int_transformed_157_id = dep_35.int_transformed_28_id
    left join dep_36 on dep_1.int_transformed_157_id = dep_36.int_transformed_53_id
    left join dep_37 on dep_1.int_transformed_157_id = dep_37.stg_source_121_id
    left join dep_38 on dep_1.int_transformed_157_id = dep_38.stg_source_11_id
    left join dep_39 on dep_1.int_transformed_157_id = dep_39.int_transformed_36_id
    left join dep_40 on dep_1.int_transformed_157_id = dep_40.stg_source_164_id
    left join dep_41 on dep_1.int_transformed_157_id = dep_41.stg_source_90_id
    left join dep_42 on dep_1.int_transformed_157_id = dep_42.int_transformed_22_id
    left join dep_43 on dep_1.int_transformed_157_id = dep_43.int_transformed_150_id
    left join dep_44 on dep_1.int_transformed_157_id = dep_44.stg_source_157_id
    left join dep_45 on dep_1.int_transformed_157_id = dep_45.stg_source_146_id
    left join dep_46 on dep_1.int_transformed_157_id = dep_46.int_transformed_74_id
    left join dep_47 on dep_1.int_transformed_157_id = dep_47.int_transformed_52_id
    left join dep_48 on dep_1.int_transformed_157_id = dep_48.stg_source_83_id
    left join dep_49 on dep_1.int_transformed_157_id = dep_49.stg_source_153_id
    left join dep_50 on dep_1.int_transformed_157_id = dep_50.int_transformed_7_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
