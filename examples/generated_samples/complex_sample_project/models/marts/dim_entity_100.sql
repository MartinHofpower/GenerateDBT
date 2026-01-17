{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_3 as (
    select * from {{ ref('stg_source_70') }}
),

dep_4 as (
    select * from {{ ref('stg_source_73') }}
),

dep_5 as (
    select * from {{ ref('stg_source_86') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_8 as (
    select * from {{ ref('stg_source_17') }}
),

dep_9 as (
    select * from {{ ref('stg_source_33') }}
),

dep_10 as (
    select * from {{ ref('stg_source_83') }}
),

dep_11 as (
    select * from {{ ref('stg_source_147') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_14 as (
    select * from {{ ref('stg_source_26') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_16 as (
    select * from {{ ref('stg_source_111') }}
),

dep_17 as (
    select * from {{ ref('stg_source_81') }}
),

dep_18 as (
    select * from {{ ref('stg_source_131') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_20 as (
    select * from {{ ref('stg_source_59') }}
),

dep_21 as (
    select * from {{ ref('stg_source_7') }}
),

dep_22 as (
    select * from {{ ref('stg_source_57') }}
),

dep_23 as (
    select * from {{ ref('stg_source_146') }}
),

dep_24 as (
    select * from {{ ref('stg_source_8') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_27 as (
    select * from {{ ref('stg_source_113') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_32 as (
    select * from {{ ref('stg_source_104') }}
),

dep_33 as (
    select * from {{ ref('stg_source_41') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_35 as (
    select * from {{ ref('stg_source_10') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_39 as (
    select * from {{ ref('stg_source_94') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_42 as (
    select * from {{ ref('stg_source_13') }}
),

dep_43 as (
    select * from {{ ref('stg_source_64') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_45 as (
    select * from {{ ref('stg_source_28') }}
),

dep_46 as (
    select * from {{ ref('stg_source_112') }}
),

dep_47 as (
    select * from {{ ref('stg_source_130') }}
),

dep_48 as (
    select * from {{ ref('stg_source_60') }}
),

dep_49 as (
    select * from {{ ref('stg_source_96') }}
),

dep_50 as (
    select * from {{ ref('stg_source_63') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_164_id']) }} as surrogate_id,
        dep_1.int_transformed_164_id as dim_entity_100_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_164_id = dep_2.int_transformed_1_id
    left join dep_3 on dep_1.int_transformed_164_id = dep_3.stg_source_70_id
    left join dep_4 on dep_1.int_transformed_164_id = dep_4.stg_source_73_id
    left join dep_5 on dep_1.int_transformed_164_id = dep_5.stg_source_86_id
    left join dep_6 on dep_1.int_transformed_164_id = dep_6.int_transformed_106_id
    left join dep_7 on dep_1.int_transformed_164_id = dep_7.int_transformed_42_id
    left join dep_8 on dep_1.int_transformed_164_id = dep_8.stg_source_17_id
    left join dep_9 on dep_1.int_transformed_164_id = dep_9.stg_source_33_id
    left join dep_10 on dep_1.int_transformed_164_id = dep_10.stg_source_83_id
    left join dep_11 on dep_1.int_transformed_164_id = dep_11.stg_source_147_id
    left join dep_12 on dep_1.int_transformed_164_id = dep_12.int_transformed_119_id
    left join dep_13 on dep_1.int_transformed_164_id = dep_13.int_transformed_85_id
    left join dep_14 on dep_1.int_transformed_164_id = dep_14.stg_source_26_id
    left join dep_15 on dep_1.int_transformed_164_id = dep_15.int_transformed_104_id
    left join dep_16 on dep_1.int_transformed_164_id = dep_16.stg_source_111_id
    left join dep_17 on dep_1.int_transformed_164_id = dep_17.stg_source_81_id
    left join dep_18 on dep_1.int_transformed_164_id = dep_18.stg_source_131_id
    left join dep_19 on dep_1.int_transformed_164_id = dep_19.int_transformed_138_id
    left join dep_20 on dep_1.int_transformed_164_id = dep_20.stg_source_59_id
    left join dep_21 on dep_1.int_transformed_164_id = dep_21.stg_source_7_id
    left join dep_22 on dep_1.int_transformed_164_id = dep_22.stg_source_57_id
    left join dep_23 on dep_1.int_transformed_164_id = dep_23.stg_source_146_id
    left join dep_24 on dep_1.int_transformed_164_id = dep_24.stg_source_8_id
    left join dep_25 on dep_1.int_transformed_164_id = dep_25.int_transformed_52_id
    left join dep_26 on dep_1.int_transformed_164_id = dep_26.int_transformed_48_id
    left join dep_27 on dep_1.int_transformed_164_id = dep_27.stg_source_113_id
    left join dep_28 on dep_1.int_transformed_164_id = dep_28.int_transformed_102_id
    left join dep_29 on dep_1.int_transformed_164_id = dep_29.int_transformed_160_id
    left join dep_30 on dep_1.int_transformed_164_id = dep_30.int_transformed_23_id
    left join dep_31 on dep_1.int_transformed_164_id = dep_31.int_transformed_128_id
    left join dep_32 on dep_1.int_transformed_164_id = dep_32.stg_source_104_id
    left join dep_33 on dep_1.int_transformed_164_id = dep_33.stg_source_41_id
    left join dep_34 on dep_1.int_transformed_164_id = dep_34.int_transformed_64_id
    left join dep_35 on dep_1.int_transformed_164_id = dep_35.stg_source_10_id
    left join dep_36 on dep_1.int_transformed_164_id = dep_36.int_transformed_61_id
    left join dep_37 on dep_1.int_transformed_164_id = dep_37.int_transformed_129_id
    left join dep_38 on dep_1.int_transformed_164_id = dep_38.int_transformed_117_id
    left join dep_39 on dep_1.int_transformed_164_id = dep_39.stg_source_94_id
    left join dep_40 on dep_1.int_transformed_164_id = dep_40.int_transformed_29_id
    left join dep_41 on dep_1.int_transformed_164_id = dep_41.int_transformed_130_id
    left join dep_42 on dep_1.int_transformed_164_id = dep_42.stg_source_13_id
    left join dep_43 on dep_1.int_transformed_164_id = dep_43.stg_source_64_id
    left join dep_44 on dep_1.int_transformed_164_id = dep_44.int_transformed_161_id
    left join dep_45 on dep_1.int_transformed_164_id = dep_45.stg_source_28_id
    left join dep_46 on dep_1.int_transformed_164_id = dep_46.stg_source_112_id
    left join dep_47 on dep_1.int_transformed_164_id = dep_47.stg_source_130_id
    left join dep_48 on dep_1.int_transformed_164_id = dep_48.stg_source_60_id
    left join dep_49 on dep_1.int_transformed_164_id = dep_49.stg_source_96_id
    left join dep_50 on dep_1.int_transformed_164_id = dep_50.stg_source_63_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
