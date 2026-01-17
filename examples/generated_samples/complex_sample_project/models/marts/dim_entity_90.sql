{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_138') }}
),

dep_2 as (
    select * from {{ ref('stg_source_159') }}
),

dep_3 as (
    select * from {{ ref('stg_source_44') }}
),

dep_4 as (
    select * from {{ ref('stg_source_55') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_6 as (
    select * from {{ ref('stg_source_23') }}
),

dep_7 as (
    select * from {{ ref('stg_source_16') }}
),

dep_8 as (
    select * from {{ ref('stg_source_67') }}
),

dep_9 as (
    select * from {{ ref('stg_source_31') }}
),

dep_10 as (
    select * from {{ ref('stg_source_69') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_12 as (
    select * from {{ ref('stg_source_97') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_14 as (
    select * from {{ ref('stg_source_128') }}
),

dep_15 as (
    select * from {{ ref('stg_source_68') }}
),

dep_16 as (
    select * from {{ ref('stg_source_88') }}
),

dep_17 as (
    select * from {{ ref('stg_source_145') }}
),

dep_18 as (
    select * from {{ ref('stg_source_136') }}
),

dep_19 as (
    select * from {{ ref('stg_source_22') }}
),

dep_20 as (
    select * from {{ ref('stg_source_61') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_22 as (
    select * from {{ ref('stg_source_163') }}
),

dep_23 as (
    select * from {{ ref('stg_source_160') }}
),

dep_24 as (
    select * from {{ ref('stg_source_14') }}
),

dep_25 as (
    select * from {{ ref('stg_source_41') }}
),

dep_26 as (
    select * from {{ ref('stg_source_115') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_28 as (
    select * from {{ ref('stg_source_64') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_33 as (
    select * from {{ ref('stg_source_74') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_35 as (
    select * from {{ ref('stg_source_11') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_37 as (
    select * from {{ ref('stg_source_52') }}
),

dep_38 as (
    select * from {{ ref('stg_source_28') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_40 as (
    select * from {{ ref('stg_source_53') }}
),

dep_41 as (
    select * from {{ ref('stg_source_153') }}
),

dep_42 as (
    select * from {{ ref('stg_source_157') }}
),

dep_43 as (
    select * from {{ ref('stg_source_105') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_47 as (
    select * from {{ ref('stg_source_82') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_50 as (
    select * from {{ ref('stg_source_104') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_138_id']) }} as surrogate_id,
        dep_1.stg_source_138_id as dim_entity_90_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_138_id = dep_2.stg_source_159_id
    left join dep_3 on dep_1.stg_source_138_id = dep_3.stg_source_44_id
    left join dep_4 on dep_1.stg_source_138_id = dep_4.stg_source_55_id
    left join dep_5 on dep_1.stg_source_138_id = dep_5.int_transformed_125_id
    left join dep_6 on dep_1.stg_source_138_id = dep_6.stg_source_23_id
    left join dep_7 on dep_1.stg_source_138_id = dep_7.stg_source_16_id
    left join dep_8 on dep_1.stg_source_138_id = dep_8.stg_source_67_id
    left join dep_9 on dep_1.stg_source_138_id = dep_9.stg_source_31_id
    left join dep_10 on dep_1.stg_source_138_id = dep_10.stg_source_69_id
    left join dep_11 on dep_1.stg_source_138_id = dep_11.int_transformed_143_id
    left join dep_12 on dep_1.stg_source_138_id = dep_12.stg_source_97_id
    left join dep_13 on dep_1.stg_source_138_id = dep_13.int_transformed_29_id
    left join dep_14 on dep_1.stg_source_138_id = dep_14.stg_source_128_id
    left join dep_15 on dep_1.stg_source_138_id = dep_15.stg_source_68_id
    left join dep_16 on dep_1.stg_source_138_id = dep_16.stg_source_88_id
    left join dep_17 on dep_1.stg_source_138_id = dep_17.stg_source_145_id
    left join dep_18 on dep_1.stg_source_138_id = dep_18.stg_source_136_id
    left join dep_19 on dep_1.stg_source_138_id = dep_19.stg_source_22_id
    left join dep_20 on dep_1.stg_source_138_id = dep_20.stg_source_61_id
    left join dep_21 on dep_1.stg_source_138_id = dep_21.int_transformed_31_id
    left join dep_22 on dep_1.stg_source_138_id = dep_22.stg_source_163_id
    left join dep_23 on dep_1.stg_source_138_id = dep_23.stg_source_160_id
    left join dep_24 on dep_1.stg_source_138_id = dep_24.stg_source_14_id
    left join dep_25 on dep_1.stg_source_138_id = dep_25.stg_source_41_id
    left join dep_26 on dep_1.stg_source_138_id = dep_26.stg_source_115_id
    left join dep_27 on dep_1.stg_source_138_id = dep_27.int_transformed_148_id
    left join dep_28 on dep_1.stg_source_138_id = dep_28.stg_source_64_id
    left join dep_29 on dep_1.stg_source_138_id = dep_29.int_transformed_123_id
    left join dep_30 on dep_1.stg_source_138_id = dep_30.int_transformed_134_id
    left join dep_31 on dep_1.stg_source_138_id = dep_31.int_transformed_23_id
    left join dep_32 on dep_1.stg_source_138_id = dep_32.int_transformed_48_id
    left join dep_33 on dep_1.stg_source_138_id = dep_33.stg_source_74_id
    left join dep_34 on dep_1.stg_source_138_id = dep_34.int_transformed_26_id
    left join dep_35 on dep_1.stg_source_138_id = dep_35.stg_source_11_id
    left join dep_36 on dep_1.stg_source_138_id = dep_36.int_transformed_118_id
    left join dep_37 on dep_1.stg_source_138_id = dep_37.stg_source_52_id
    left join dep_38 on dep_1.stg_source_138_id = dep_38.stg_source_28_id
    left join dep_39 on dep_1.stg_source_138_id = dep_39.int_transformed_97_id
    left join dep_40 on dep_1.stg_source_138_id = dep_40.stg_source_53_id
    left join dep_41 on dep_1.stg_source_138_id = dep_41.stg_source_153_id
    left join dep_42 on dep_1.stg_source_138_id = dep_42.stg_source_157_id
    left join dep_43 on dep_1.stg_source_138_id = dep_43.stg_source_105_id
    left join dep_44 on dep_1.stg_source_138_id = dep_44.int_transformed_39_id
    left join dep_45 on dep_1.stg_source_138_id = dep_45.int_transformed_162_id
    left join dep_46 on dep_1.stg_source_138_id = dep_46.int_transformed_85_id
    left join dep_47 on dep_1.stg_source_138_id = dep_47.stg_source_82_id
    left join dep_48 on dep_1.stg_source_138_id = dep_48.int_transformed_19_id
    left join dep_49 on dep_1.stg_source_138_id = dep_49.int_transformed_76_id
    left join dep_50 on dep_1.stg_source_138_id = dep_50.stg_source_104_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
