{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_2 as (
    select * from {{ ref('stg_source_41') }}
),

dep_3 as (
    select * from {{ ref('stg_source_166') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_5 as (
    select * from {{ ref('stg_source_56') }}
),

dep_6 as (
    select * from {{ ref('stg_source_57') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_8 as (
    select * from {{ ref('stg_source_10') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_11 as (
    select * from {{ ref('stg_source_105') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_16 as (
    select * from {{ ref('stg_source_20') }}
),

dep_17 as (
    select * from {{ ref('stg_source_32') }}
),

dep_18 as (
    select * from {{ ref('stg_source_24') }}
),

dep_19 as (
    select * from {{ ref('stg_source_12') }}
),

dep_20 as (
    select * from {{ ref('stg_source_69') }}
),

dep_21 as (
    select * from {{ ref('stg_source_28') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_23 as (
    select * from {{ ref('stg_source_145') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_26 as (
    select * from {{ ref('stg_source_123') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_28 as (
    select * from {{ ref('stg_source_165') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_30 as (
    select * from {{ ref('stg_source_34') }}
),

dep_31 as (
    select * from {{ ref('stg_source_5') }}
),

dep_32 as (
    select * from {{ ref('stg_source_59') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_34 as (
    select * from {{ ref('stg_source_58') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_36 as (
    select * from {{ ref('stg_source_117') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_38 as (
    select * from {{ ref('stg_source_15') }}
),

dep_39 as (
    select * from {{ ref('stg_source_164') }}
),

dep_40 as (
    select * from {{ ref('stg_source_121') }}
),

dep_41 as (
    select * from {{ ref('stg_source_79') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_44 as (
    select * from {{ ref('stg_source_115') }}
),

dep_45 as (
    select * from {{ ref('stg_source_135') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_47 as (
    select * from {{ ref('stg_source_124') }}
),

dep_48 as (
    select * from {{ ref('stg_source_148') }}
),

dep_49 as (
    select * from {{ ref('stg_source_33') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_47') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_161_id']) }} as surrogate_id,
        dep_1.int_transformed_161_id as fct_business_event_83_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_161_id = dep_2.stg_source_41_id
    left join dep_3 on dep_1.int_transformed_161_id = dep_3.stg_source_166_id
    left join dep_4 on dep_1.int_transformed_161_id = dep_4.int_transformed_91_id
    left join dep_5 on dep_1.int_transformed_161_id = dep_5.stg_source_56_id
    left join dep_6 on dep_1.int_transformed_161_id = dep_6.stg_source_57_id
    left join dep_7 on dep_1.int_transformed_161_id = dep_7.int_transformed_127_id
    left join dep_8 on dep_1.int_transformed_161_id = dep_8.stg_source_10_id
    left join dep_9 on dep_1.int_transformed_161_id = dep_9.int_transformed_115_id
    left join dep_10 on dep_1.int_transformed_161_id = dep_10.int_transformed_17_id
    left join dep_11 on dep_1.int_transformed_161_id = dep_11.stg_source_105_id
    left join dep_12 on dep_1.int_transformed_161_id = dep_12.int_transformed_162_id
    left join dep_13 on dep_1.int_transformed_161_id = dep_13.int_transformed_8_id
    left join dep_14 on dep_1.int_transformed_161_id = dep_14.int_transformed_16_id
    left join dep_15 on dep_1.int_transformed_161_id = dep_15.int_transformed_56_id
    left join dep_16 on dep_1.int_transformed_161_id = dep_16.stg_source_20_id
    left join dep_17 on dep_1.int_transformed_161_id = dep_17.stg_source_32_id
    left join dep_18 on dep_1.int_transformed_161_id = dep_18.stg_source_24_id
    left join dep_19 on dep_1.int_transformed_161_id = dep_19.stg_source_12_id
    left join dep_20 on dep_1.int_transformed_161_id = dep_20.stg_source_69_id
    left join dep_21 on dep_1.int_transformed_161_id = dep_21.stg_source_28_id
    left join dep_22 on dep_1.int_transformed_161_id = dep_22.int_transformed_84_id
    left join dep_23 on dep_1.int_transformed_161_id = dep_23.stg_source_145_id
    left join dep_24 on dep_1.int_transformed_161_id = dep_24.int_transformed_23_id
    left join dep_25 on dep_1.int_transformed_161_id = dep_25.int_transformed_71_id
    left join dep_26 on dep_1.int_transformed_161_id = dep_26.stg_source_123_id
    left join dep_27 on dep_1.int_transformed_161_id = dep_27.int_transformed_37_id
    left join dep_28 on dep_1.int_transformed_161_id = dep_28.stg_source_165_id
    left join dep_29 on dep_1.int_transformed_161_id = dep_29.int_transformed_160_id
    left join dep_30 on dep_1.int_transformed_161_id = dep_30.stg_source_34_id
    left join dep_31 on dep_1.int_transformed_161_id = dep_31.stg_source_5_id
    left join dep_32 on dep_1.int_transformed_161_id = dep_32.stg_source_59_id
    left join dep_33 on dep_1.int_transformed_161_id = dep_33.int_transformed_63_id
    left join dep_34 on dep_1.int_transformed_161_id = dep_34.stg_source_58_id
    left join dep_35 on dep_1.int_transformed_161_id = dep_35.int_transformed_124_id
    left join dep_36 on dep_1.int_transformed_161_id = dep_36.stg_source_117_id
    left join dep_37 on dep_1.int_transformed_161_id = dep_37.int_transformed_50_id
    left join dep_38 on dep_1.int_transformed_161_id = dep_38.stg_source_15_id
    left join dep_39 on dep_1.int_transformed_161_id = dep_39.stg_source_164_id
    left join dep_40 on dep_1.int_transformed_161_id = dep_40.stg_source_121_id
    left join dep_41 on dep_1.int_transformed_161_id = dep_41.stg_source_79_id
    left join dep_42 on dep_1.int_transformed_161_id = dep_42.int_transformed_159_id
    left join dep_43 on dep_1.int_transformed_161_id = dep_43.int_transformed_140_id
    left join dep_44 on dep_1.int_transformed_161_id = dep_44.stg_source_115_id
    left join dep_45 on dep_1.int_transformed_161_id = dep_45.stg_source_135_id
    left join dep_46 on dep_1.int_transformed_161_id = dep_46.int_transformed_108_id
    left join dep_47 on dep_1.int_transformed_161_id = dep_47.stg_source_124_id
    left join dep_48 on dep_1.int_transformed_161_id = dep_48.stg_source_148_id
    left join dep_49 on dep_1.int_transformed_161_id = dep_49.stg_source_33_id
    left join dep_50 on dep_1.int_transformed_161_id = dep_50.int_transformed_47_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
