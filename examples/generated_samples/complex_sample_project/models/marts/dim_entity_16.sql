{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_3 as (
    select * from {{ ref('stg_source_5') }}
),

dep_4 as (
    select * from {{ ref('stg_source_104') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_9 as (
    select * from {{ ref('stg_source_29') }}
),

dep_10 as (
    select * from {{ ref('stg_source_70') }}
),

dep_11 as (
    select * from {{ ref('stg_source_116') }}
),

dep_12 as (
    select * from {{ ref('stg_source_49') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_17 as (
    select * from {{ ref('stg_source_113') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_19 as (
    select * from {{ ref('stg_source_62') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_152') }}
),

dep_21 as (
    select * from {{ ref('stg_source_138') }}
),

dep_22 as (
    select * from {{ ref('stg_source_35') }}
),

dep_23 as (
    select * from {{ ref('stg_source_15') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_25 as (
    select * from {{ ref('stg_source_43') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_102') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_32 as (
    select * from {{ ref('stg_source_81') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_35 as (
    select * from {{ ref('stg_source_24') }}
),

dep_36 as (
    select * from {{ ref('stg_source_58') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_38 as (
    select * from {{ ref('stg_source_102') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_42 as (
    select * from {{ ref('stg_source_123') }}
),

dep_43 as (
    select * from {{ ref('stg_source_30') }}
),

dep_44 as (
    select * from {{ ref('stg_source_125') }}
),

dep_45 as (
    select * from {{ ref('stg_source_33') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_48 as (
    select * from {{ ref('stg_source_52') }}
),

dep_49 as (
    select * from {{ ref('stg_source_40') }}
),

dep_50 as (
    select * from {{ ref('stg_source_61') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_116_id']) }} as surrogate_id,
        dep_1.int_transformed_116_id as dim_entity_16_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_116_id = dep_2.int_transformed_32_id
    left join dep_3 on dep_1.int_transformed_116_id = dep_3.stg_source_5_id
    left join dep_4 on dep_1.int_transformed_116_id = dep_4.stg_source_104_id
    left join dep_5 on dep_1.int_transformed_116_id = dep_5.int_transformed_4_id
    left join dep_6 on dep_1.int_transformed_116_id = dep_6.int_transformed_41_id
    left join dep_7 on dep_1.int_transformed_116_id = dep_7.int_transformed_132_id
    left join dep_8 on dep_1.int_transformed_116_id = dep_8.int_transformed_137_id
    left join dep_9 on dep_1.int_transformed_116_id = dep_9.stg_source_29_id
    left join dep_10 on dep_1.int_transformed_116_id = dep_10.stg_source_70_id
    left join dep_11 on dep_1.int_transformed_116_id = dep_11.stg_source_116_id
    left join dep_12 on dep_1.int_transformed_116_id = dep_12.stg_source_49_id
    left join dep_13 on dep_1.int_transformed_116_id = dep_13.int_transformed_58_id
    left join dep_14 on dep_1.int_transformed_116_id = dep_14.int_transformed_162_id
    left join dep_15 on dep_1.int_transformed_116_id = dep_15.int_transformed_144_id
    left join dep_16 on dep_1.int_transformed_116_id = dep_16.int_transformed_164_id
    left join dep_17 on dep_1.int_transformed_116_id = dep_17.stg_source_113_id
    left join dep_18 on dep_1.int_transformed_116_id = dep_18.int_transformed_92_id
    left join dep_19 on dep_1.int_transformed_116_id = dep_19.stg_source_62_id
    left join dep_20 on dep_1.int_transformed_116_id = dep_20.int_transformed_152_id
    left join dep_21 on dep_1.int_transformed_116_id = dep_21.stg_source_138_id
    left join dep_22 on dep_1.int_transformed_116_id = dep_22.stg_source_35_id
    left join dep_23 on dep_1.int_transformed_116_id = dep_23.stg_source_15_id
    left join dep_24 on dep_1.int_transformed_116_id = dep_24.int_transformed_7_id
    left join dep_25 on dep_1.int_transformed_116_id = dep_25.stg_source_43_id
    left join dep_26 on dep_1.int_transformed_116_id = dep_26.int_transformed_133_id
    left join dep_27 on dep_1.int_transformed_116_id = dep_27.int_transformed_75_id
    left join dep_28 on dep_1.int_transformed_116_id = dep_28.int_transformed_124_id
    left join dep_29 on dep_1.int_transformed_116_id = dep_29.int_transformed_15_id
    left join dep_30 on dep_1.int_transformed_116_id = dep_30.int_transformed_102_id
    left join dep_31 on dep_1.int_transformed_116_id = dep_31.int_transformed_34_id
    left join dep_32 on dep_1.int_transformed_116_id = dep_32.stg_source_81_id
    left join dep_33 on dep_1.int_transformed_116_id = dep_33.int_transformed_23_id
    left join dep_34 on dep_1.int_transformed_116_id = dep_34.int_transformed_33_id
    left join dep_35 on dep_1.int_transformed_116_id = dep_35.stg_source_24_id
    left join dep_36 on dep_1.int_transformed_116_id = dep_36.stg_source_58_id
    left join dep_37 on dep_1.int_transformed_116_id = dep_37.int_transformed_148_id
    left join dep_38 on dep_1.int_transformed_116_id = dep_38.stg_source_102_id
    left join dep_39 on dep_1.int_transformed_116_id = dep_39.int_transformed_9_id
    left join dep_40 on dep_1.int_transformed_116_id = dep_40.int_transformed_43_id
    left join dep_41 on dep_1.int_transformed_116_id = dep_41.int_transformed_159_id
    left join dep_42 on dep_1.int_transformed_116_id = dep_42.stg_source_123_id
    left join dep_43 on dep_1.int_transformed_116_id = dep_43.stg_source_30_id
    left join dep_44 on dep_1.int_transformed_116_id = dep_44.stg_source_125_id
    left join dep_45 on dep_1.int_transformed_116_id = dep_45.stg_source_33_id
    left join dep_46 on dep_1.int_transformed_116_id = dep_46.int_transformed_81_id
    left join dep_47 on dep_1.int_transformed_116_id = dep_47.int_transformed_52_id
    left join dep_48 on dep_1.int_transformed_116_id = dep_48.stg_source_52_id
    left join dep_49 on dep_1.int_transformed_116_id = dep_49.stg_source_40_id
    left join dep_50 on dep_1.int_transformed_116_id = dep_50.stg_source_61_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
