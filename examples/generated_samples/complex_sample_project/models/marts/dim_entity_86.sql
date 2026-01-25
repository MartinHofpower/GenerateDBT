{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_40') }}
),

dep_2 as (
    select * from {{ ref('stg_source_137') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_142') }}
),

dep_4 as (
    select * from {{ ref('stg_source_107') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_6 as (
    select * from {{ ref('stg_source_79') }}
),

dep_7 as (
    select * from {{ ref('stg_source_46') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_10 as (
    select * from {{ ref('stg_source_117') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_12 as (
    select * from {{ ref('stg_source_78') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_14 as (
    select * from {{ ref('stg_source_68') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_16 as (
    select * from {{ ref('stg_source_76') }}
),

dep_17 as (
    select * from {{ ref('stg_source_25') }}
),

dep_18 as (
    select * from {{ ref('stg_source_147') }}
),

dep_19 as (
    select * from {{ ref('stg_source_55') }}
),

dep_20 as (
    select * from {{ ref('stg_source_82') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_24 as (
    select * from {{ ref('stg_source_151') }}
),

dep_25 as (
    select * from {{ ref('stg_source_12') }}
),

dep_26 as (
    select * from {{ ref('stg_source_22') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_28 as (
    select * from {{ ref('stg_source_88') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_32 as (
    select * from {{ ref('stg_source_161') }}
),

dep_33 as (
    select * from {{ ref('stg_source_152') }}
),

dep_34 as (
    select * from {{ ref('stg_source_3') }}
),

dep_35 as (
    select * from {{ ref('stg_source_85') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_38 as (
    select * from {{ ref('stg_source_29') }}
),

dep_39 as (
    select * from {{ ref('stg_source_97') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_41 as (
    select * from {{ ref('stg_source_70') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_45 as (
    select * from {{ ref('stg_source_126') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_47 as (
    select * from {{ ref('stg_source_162') }}
),

dep_48 as (
    select * from {{ ref('stg_source_133') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_53') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_40_id']) }} as surrogate_id,
        dep_1.stg_source_40_id as dim_entity_86_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_40_id = dep_2.stg_source_137_id
    left join dep_3 on dep_1.stg_source_40_id = dep_3.int_transformed_142_id
    left join dep_4 on dep_1.stg_source_40_id = dep_4.stg_source_107_id
    left join dep_5 on dep_1.stg_source_40_id = dep_5.int_transformed_19_id
    left join dep_6 on dep_1.stg_source_40_id = dep_6.stg_source_79_id
    left join dep_7 on dep_1.stg_source_40_id = dep_7.stg_source_46_id
    left join dep_8 on dep_1.stg_source_40_id = dep_8.int_transformed_135_id
    left join dep_9 on dep_1.stg_source_40_id = dep_9.int_transformed_30_id
    left join dep_10 on dep_1.stg_source_40_id = dep_10.stg_source_117_id
    left join dep_11 on dep_1.stg_source_40_id = dep_11.int_transformed_152_id
    left join dep_12 on dep_1.stg_source_40_id = dep_12.stg_source_78_id
    left join dep_13 on dep_1.stg_source_40_id = dep_13.int_transformed_8_id
    left join dep_14 on dep_1.stg_source_40_id = dep_14.stg_source_68_id
    left join dep_15 on dep_1.stg_source_40_id = dep_15.int_transformed_162_id
    left join dep_16 on dep_1.stg_source_40_id = dep_16.stg_source_76_id
    left join dep_17 on dep_1.stg_source_40_id = dep_17.stg_source_25_id
    left join dep_18 on dep_1.stg_source_40_id = dep_18.stg_source_147_id
    left join dep_19 on dep_1.stg_source_40_id = dep_19.stg_source_55_id
    left join dep_20 on dep_1.stg_source_40_id = dep_20.stg_source_82_id
    left join dep_21 on dep_1.stg_source_40_id = dep_21.int_transformed_147_id
    left join dep_22 on dep_1.stg_source_40_id = dep_22.int_transformed_109_id
    left join dep_23 on dep_1.stg_source_40_id = dep_23.int_transformed_103_id
    left join dep_24 on dep_1.stg_source_40_id = dep_24.stg_source_151_id
    left join dep_25 on dep_1.stg_source_40_id = dep_25.stg_source_12_id
    left join dep_26 on dep_1.stg_source_40_id = dep_26.stg_source_22_id
    left join dep_27 on dep_1.stg_source_40_id = dep_27.int_transformed_137_id
    left join dep_28 on dep_1.stg_source_40_id = dep_28.stg_source_88_id
    left join dep_29 on dep_1.stg_source_40_id = dep_29.int_transformed_136_id
    left join dep_30 on dep_1.stg_source_40_id = dep_30.int_transformed_138_id
    left join dep_31 on dep_1.stg_source_40_id = dep_31.int_transformed_112_id
    left join dep_32 on dep_1.stg_source_40_id = dep_32.stg_source_161_id
    left join dep_33 on dep_1.stg_source_40_id = dep_33.stg_source_152_id
    left join dep_34 on dep_1.stg_source_40_id = dep_34.stg_source_3_id
    left join dep_35 on dep_1.stg_source_40_id = dep_35.stg_source_85_id
    left join dep_36 on dep_1.stg_source_40_id = dep_36.int_transformed_85_id
    left join dep_37 on dep_1.stg_source_40_id = dep_37.int_transformed_126_id
    left join dep_38 on dep_1.stg_source_40_id = dep_38.stg_source_29_id
    left join dep_39 on dep_1.stg_source_40_id = dep_39.stg_source_97_id
    left join dep_40 on dep_1.stg_source_40_id = dep_40.int_transformed_4_id
    left join dep_41 on dep_1.stg_source_40_id = dep_41.stg_source_70_id
    left join dep_42 on dep_1.stg_source_40_id = dep_42.int_transformed_13_id
    left join dep_43 on dep_1.stg_source_40_id = dep_43.int_transformed_48_id
    left join dep_44 on dep_1.stg_source_40_id = dep_44.int_transformed_67_id
    left join dep_45 on dep_1.stg_source_40_id = dep_45.stg_source_126_id
    left join dep_46 on dep_1.stg_source_40_id = dep_46.int_transformed_56_id
    left join dep_47 on dep_1.stg_source_40_id = dep_47.stg_source_162_id
    left join dep_48 on dep_1.stg_source_40_id = dep_48.stg_source_133_id
    left join dep_49 on dep_1.stg_source_40_id = dep_49.int_transformed_119_id
    left join dep_50 on dep_1.stg_source_40_id = dep_50.int_transformed_53_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
