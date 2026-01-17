{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_166') }}
),

dep_2 as (
    select * from {{ ref('stg_source_164') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_4 as (
    select * from {{ ref('stg_source_96') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_6 as (
    select * from {{ ref('stg_source_86') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_10 as (
    select * from {{ ref('stg_source_131') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_14 as (
    select * from {{ ref('stg_source_161') }}
),

dep_15 as (
    select * from {{ ref('stg_source_119') }}
),

dep_16 as (
    select * from {{ ref('stg_source_24') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_18 as (
    select * from {{ ref('stg_source_64') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_22 as (
    select * from {{ ref('stg_source_75') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_27 as (
    select * from {{ ref('stg_source_93') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_30 as (
    select * from {{ ref('stg_source_78') }}
),

dep_31 as (
    select * from {{ ref('stg_source_140') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_34 as (
    select * from {{ ref('stg_source_33') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_36 as (
    select * from {{ ref('stg_source_29') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_38 as (
    select * from {{ ref('stg_source_82') }}
),

dep_39 as (
    select * from {{ ref('stg_source_44') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_43 as (
    select * from {{ ref('stg_source_40') }}
),

dep_44 as (
    select * from {{ ref('stg_source_110') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_46 as (
    select * from {{ ref('stg_source_28') }}
),

dep_47 as (
    select * from {{ ref('stg_source_76') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_49 as (
    select * from {{ ref('stg_source_60') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_131') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_166_id']) }} as surrogate_id,
        dep_1.stg_source_166_id as dim_entity_40_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_166_id = dep_2.stg_source_164_id
    left join dep_3 on dep_1.stg_source_166_id = dep_3.int_transformed_71_id
    left join dep_4 on dep_1.stg_source_166_id = dep_4.stg_source_96_id
    left join dep_5 on dep_1.stg_source_166_id = dep_5.int_transformed_117_id
    left join dep_6 on dep_1.stg_source_166_id = dep_6.stg_source_86_id
    left join dep_7 on dep_1.stg_source_166_id = dep_7.int_transformed_26_id
    left join dep_8 on dep_1.stg_source_166_id = dep_8.int_transformed_124_id
    left join dep_9 on dep_1.stg_source_166_id = dep_9.int_transformed_42_id
    left join dep_10 on dep_1.stg_source_166_id = dep_10.stg_source_131_id
    left join dep_11 on dep_1.stg_source_166_id = dep_11.int_transformed_2_id
    left join dep_12 on dep_1.stg_source_166_id = dep_12.int_transformed_20_id
    left join dep_13 on dep_1.stg_source_166_id = dep_13.int_transformed_119_id
    left join dep_14 on dep_1.stg_source_166_id = dep_14.stg_source_161_id
    left join dep_15 on dep_1.stg_source_166_id = dep_15.stg_source_119_id
    left join dep_16 on dep_1.stg_source_166_id = dep_16.stg_source_24_id
    left join dep_17 on dep_1.stg_source_166_id = dep_17.int_transformed_12_id
    left join dep_18 on dep_1.stg_source_166_id = dep_18.stg_source_64_id
    left join dep_19 on dep_1.stg_source_166_id = dep_19.int_transformed_28_id
    left join dep_20 on dep_1.stg_source_166_id = dep_20.int_transformed_18_id
    left join dep_21 on dep_1.stg_source_166_id = dep_21.int_transformed_143_id
    left join dep_22 on dep_1.stg_source_166_id = dep_22.stg_source_75_id
    left join dep_23 on dep_1.stg_source_166_id = dep_23.int_transformed_115_id
    left join dep_24 on dep_1.stg_source_166_id = dep_24.int_transformed_151_id
    left join dep_25 on dep_1.stg_source_166_id = dep_25.int_transformed_148_id
    left join dep_26 on dep_1.stg_source_166_id = dep_26.int_transformed_74_id
    left join dep_27 on dep_1.stg_source_166_id = dep_27.stg_source_93_id
    left join dep_28 on dep_1.stg_source_166_id = dep_28.int_transformed_116_id
    left join dep_29 on dep_1.stg_source_166_id = dep_29.int_transformed_77_id
    left join dep_30 on dep_1.stg_source_166_id = dep_30.stg_source_78_id
    left join dep_31 on dep_1.stg_source_166_id = dep_31.stg_source_140_id
    left join dep_32 on dep_1.stg_source_166_id = dep_32.int_transformed_37_id
    left join dep_33 on dep_1.stg_source_166_id = dep_33.int_transformed_13_id
    left join dep_34 on dep_1.stg_source_166_id = dep_34.stg_source_33_id
    left join dep_35 on dep_1.stg_source_166_id = dep_35.int_transformed_101_id
    left join dep_36 on dep_1.stg_source_166_id = dep_36.stg_source_29_id
    left join dep_37 on dep_1.stg_source_166_id = dep_37.int_transformed_78_id
    left join dep_38 on dep_1.stg_source_166_id = dep_38.stg_source_82_id
    left join dep_39 on dep_1.stg_source_166_id = dep_39.stg_source_44_id
    left join dep_40 on dep_1.stg_source_166_id = dep_40.int_transformed_133_id
    left join dep_41 on dep_1.stg_source_166_id = dep_41.int_transformed_128_id
    left join dep_42 on dep_1.stg_source_166_id = dep_42.int_transformed_11_id
    left join dep_43 on dep_1.stg_source_166_id = dep_43.stg_source_40_id
    left join dep_44 on dep_1.stg_source_166_id = dep_44.stg_source_110_id
    left join dep_45 on dep_1.stg_source_166_id = dep_45.int_transformed_125_id
    left join dep_46 on dep_1.stg_source_166_id = dep_46.stg_source_28_id
    left join dep_47 on dep_1.stg_source_166_id = dep_47.stg_source_76_id
    left join dep_48 on dep_1.stg_source_166_id = dep_48.int_transformed_57_id
    left join dep_49 on dep_1.stg_source_166_id = dep_49.stg_source_60_id
    left join dep_50 on dep_1.stg_source_166_id = dep_50.int_transformed_131_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
