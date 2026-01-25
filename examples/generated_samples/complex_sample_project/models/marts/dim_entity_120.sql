{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_82') }}
),

dep_2 as (
    select * from {{ ref('stg_source_132') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_5 as (
    select * from {{ ref('stg_source_143') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_9 as (
    select * from {{ ref('stg_source_163') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_79') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_40') }}
),

dep_14 as (
    select * from {{ ref('stg_source_41') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_16 as (
    select * from {{ ref('stg_source_18') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_18 as (
    select * from {{ ref('stg_source_137') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_21 as (
    select * from {{ ref('stg_source_78') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_25 as (
    select * from {{ ref('stg_source_108') }}
),

dep_26 as (
    select * from {{ ref('stg_source_65') }}
),

dep_27 as (
    select * from {{ ref('stg_source_100') }}
),

dep_28 as (
    select * from {{ ref('stg_source_157') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_32 as (
    select * from {{ ref('stg_source_90') }}
),

dep_33 as (
    select * from {{ ref('stg_source_40') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_35 as (
    select * from {{ ref('stg_source_128') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_37 as (
    select * from {{ ref('stg_source_159') }}
),

dep_38 as (
    select * from {{ ref('stg_source_91') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_42 as (
    select * from {{ ref('stg_source_149') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_44 as (
    select * from {{ ref('stg_source_125') }}
),

dep_45 as (
    select * from {{ ref('stg_source_20') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_47 as (
    select * from {{ ref('stg_source_113') }}
),

dep_48 as (
    select * from {{ ref('stg_source_150') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_134') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_82_id']) }} as surrogate_id,
        dep_1.stg_source_82_id as dim_entity_120_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_82_id = dep_2.stg_source_132_id
    left join dep_3 on dep_1.stg_source_82_id = dep_3.int_transformed_144_id
    left join dep_4 on dep_1.stg_source_82_id = dep_4.int_transformed_126_id
    left join dep_5 on dep_1.stg_source_82_id = dep_5.stg_source_143_id
    left join dep_6 on dep_1.stg_source_82_id = dep_6.int_transformed_122_id
    left join dep_7 on dep_1.stg_source_82_id = dep_7.int_transformed_125_id
    left join dep_8 on dep_1.stg_source_82_id = dep_8.int_transformed_137_id
    left join dep_9 on dep_1.stg_source_82_id = dep_9.stg_source_163_id
    left join dep_10 on dep_1.stg_source_82_id = dep_10.int_transformed_166_id
    left join dep_11 on dep_1.stg_source_82_id = dep_11.int_transformed_79_id
    left join dep_12 on dep_1.stg_source_82_id = dep_12.int_transformed_127_id
    left join dep_13 on dep_1.stg_source_82_id = dep_13.int_transformed_40_id
    left join dep_14 on dep_1.stg_source_82_id = dep_14.stg_source_41_id
    left join dep_15 on dep_1.stg_source_82_id = dep_15.int_transformed_129_id
    left join dep_16 on dep_1.stg_source_82_id = dep_16.stg_source_18_id
    left join dep_17 on dep_1.stg_source_82_id = dep_17.int_transformed_38_id
    left join dep_18 on dep_1.stg_source_82_id = dep_18.stg_source_137_id
    left join dep_19 on dep_1.stg_source_82_id = dep_19.int_transformed_119_id
    left join dep_20 on dep_1.stg_source_82_id = dep_20.int_transformed_148_id
    left join dep_21 on dep_1.stg_source_82_id = dep_21.stg_source_78_id
    left join dep_22 on dep_1.stg_source_82_id = dep_22.int_transformed_77_id
    left join dep_23 on dep_1.stg_source_82_id = dep_23.int_transformed_33_id
    left join dep_24 on dep_1.stg_source_82_id = dep_24.int_transformed_161_id
    left join dep_25 on dep_1.stg_source_82_id = dep_25.stg_source_108_id
    left join dep_26 on dep_1.stg_source_82_id = dep_26.stg_source_65_id
    left join dep_27 on dep_1.stg_source_82_id = dep_27.stg_source_100_id
    left join dep_28 on dep_1.stg_source_82_id = dep_28.stg_source_157_id
    left join dep_29 on dep_1.stg_source_82_id = dep_29.int_transformed_8_id
    left join dep_30 on dep_1.stg_source_82_id = dep_30.int_transformed_51_id
    left join dep_31 on dep_1.stg_source_82_id = dep_31.int_transformed_146_id
    left join dep_32 on dep_1.stg_source_82_id = dep_32.stg_source_90_id
    left join dep_33 on dep_1.stg_source_82_id = dep_33.stg_source_40_id
    left join dep_34 on dep_1.stg_source_82_id = dep_34.int_transformed_131_id
    left join dep_35 on dep_1.stg_source_82_id = dep_35.stg_source_128_id
    left join dep_36 on dep_1.stg_source_82_id = dep_36.int_transformed_106_id
    left join dep_37 on dep_1.stg_source_82_id = dep_37.stg_source_159_id
    left join dep_38 on dep_1.stg_source_82_id = dep_38.stg_source_91_id
    left join dep_39 on dep_1.stg_source_82_id = dep_39.int_transformed_53_id
    left join dep_40 on dep_1.stg_source_82_id = dep_40.int_transformed_43_id
    left join dep_41 on dep_1.stg_source_82_id = dep_41.int_transformed_150_id
    left join dep_42 on dep_1.stg_source_82_id = dep_42.stg_source_149_id
    left join dep_43 on dep_1.stg_source_82_id = dep_43.int_transformed_12_id
    left join dep_44 on dep_1.stg_source_82_id = dep_44.stg_source_125_id
    left join dep_45 on dep_1.stg_source_82_id = dep_45.stg_source_20_id
    left join dep_46 on dep_1.stg_source_82_id = dep_46.int_transformed_41_id
    left join dep_47 on dep_1.stg_source_82_id = dep_47.stg_source_113_id
    left join dep_48 on dep_1.stg_source_82_id = dep_48.stg_source_150_id
    left join dep_49 on dep_1.stg_source_82_id = dep_49.int_transformed_66_id
    left join dep_50 on dep_1.stg_source_82_id = dep_50.int_transformed_134_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
