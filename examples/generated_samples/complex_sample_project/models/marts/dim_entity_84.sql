{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_3 as (
    select * from {{ ref('stg_source_12') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_5 as (
    select * from {{ ref('stg_source_51') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_8 as (
    select * from {{ ref('stg_source_154') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_105') }}
),

dep_12 as (
    select * from {{ ref('stg_source_157') }}
),

dep_13 as (
    select * from {{ ref('stg_source_113') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_15 as (
    select * from {{ ref('stg_source_55') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_17 as (
    select * from {{ ref('stg_source_106') }}
),

dep_18 as (
    select * from {{ ref('stg_source_115') }}
),

dep_19 as (
    select * from {{ ref('stg_source_67') }}
),

dep_20 as (
    select * from {{ ref('stg_source_50') }}
),

dep_21 as (
    select * from {{ ref('stg_source_54') }}
),

dep_22 as (
    select * from {{ ref('stg_source_98') }}
),

dep_23 as (
    select * from {{ ref('stg_source_117') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_25 as (
    select * from {{ ref('stg_source_102') }}
),

dep_26 as (
    select * from {{ ref('stg_source_99') }}
),

dep_27 as (
    select * from {{ ref('stg_source_42') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_32 as (
    select * from {{ ref('stg_source_33') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_25') }}
),

dep_34 as (
    select * from {{ ref('stg_source_11') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_139') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_38 as (
    select * from {{ ref('stg_source_97') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_40 as (
    select * from {{ ref('stg_source_25') }}
),

dep_41 as (
    select * from {{ ref('stg_source_104') }}
),

dep_42 as (
    select * from {{ ref('stg_source_56') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_44 as (
    select * from {{ ref('stg_source_160') }}
),

dep_45 as (
    select * from {{ ref('stg_source_82') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_145') }}
),

dep_47 as (
    select * from {{ ref('stg_source_103') }}
),

dep_48 as (
    select * from {{ ref('stg_source_107') }}
),

dep_49 as (
    select * from {{ ref('stg_source_120') }}
),

dep_50 as (
    select * from {{ ref('stg_source_147') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_81_id']) }} as surrogate_id,
        dep_1.int_transformed_81_id as dim_entity_84_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_81_id = dep_2.int_transformed_110_id
    left join dep_3 on dep_1.int_transformed_81_id = dep_3.stg_source_12_id
    left join dep_4 on dep_1.int_transformed_81_id = dep_4.int_transformed_10_id
    left join dep_5 on dep_1.int_transformed_81_id = dep_5.stg_source_51_id
    left join dep_6 on dep_1.int_transformed_81_id = dep_6.int_transformed_138_id
    left join dep_7 on dep_1.int_transformed_81_id = dep_7.int_transformed_133_id
    left join dep_8 on dep_1.int_transformed_81_id = dep_8.stg_source_154_id
    left join dep_9 on dep_1.int_transformed_81_id = dep_9.int_transformed_148_id
    left join dep_10 on dep_1.int_transformed_81_id = dep_10.int_transformed_92_id
    left join dep_11 on dep_1.int_transformed_81_id = dep_11.int_transformed_105_id
    left join dep_12 on dep_1.int_transformed_81_id = dep_12.stg_source_157_id
    left join dep_13 on dep_1.int_transformed_81_id = dep_13.stg_source_113_id
    left join dep_14 on dep_1.int_transformed_81_id = dep_14.int_transformed_114_id
    left join dep_15 on dep_1.int_transformed_81_id = dep_15.stg_source_55_id
    left join dep_16 on dep_1.int_transformed_81_id = dep_16.int_transformed_16_id
    left join dep_17 on dep_1.int_transformed_81_id = dep_17.stg_source_106_id
    left join dep_18 on dep_1.int_transformed_81_id = dep_18.stg_source_115_id
    left join dep_19 on dep_1.int_transformed_81_id = dep_19.stg_source_67_id
    left join dep_20 on dep_1.int_transformed_81_id = dep_20.stg_source_50_id
    left join dep_21 on dep_1.int_transformed_81_id = dep_21.stg_source_54_id
    left join dep_22 on dep_1.int_transformed_81_id = dep_22.stg_source_98_id
    left join dep_23 on dep_1.int_transformed_81_id = dep_23.stg_source_117_id
    left join dep_24 on dep_1.int_transformed_81_id = dep_24.int_transformed_153_id
    left join dep_25 on dep_1.int_transformed_81_id = dep_25.stg_source_102_id
    left join dep_26 on dep_1.int_transformed_81_id = dep_26.stg_source_99_id
    left join dep_27 on dep_1.int_transformed_81_id = dep_27.stg_source_42_id
    left join dep_28 on dep_1.int_transformed_81_id = dep_28.int_transformed_32_id
    left join dep_29 on dep_1.int_transformed_81_id = dep_29.int_transformed_57_id
    left join dep_30 on dep_1.int_transformed_81_id = dep_30.int_transformed_24_id
    left join dep_31 on dep_1.int_transformed_81_id = dep_31.int_transformed_26_id
    left join dep_32 on dep_1.int_transformed_81_id = dep_32.stg_source_33_id
    left join dep_33 on dep_1.int_transformed_81_id = dep_33.int_transformed_25_id
    left join dep_34 on dep_1.int_transformed_81_id = dep_34.stg_source_11_id
    left join dep_35 on dep_1.int_transformed_81_id = dep_35.int_transformed_139_id
    left join dep_36 on dep_1.int_transformed_81_id = dep_36.int_transformed_141_id
    left join dep_37 on dep_1.int_transformed_81_id = dep_37.int_transformed_8_id
    left join dep_38 on dep_1.int_transformed_81_id = dep_38.stg_source_97_id
    left join dep_39 on dep_1.int_transformed_81_id = dep_39.int_transformed_44_id
    left join dep_40 on dep_1.int_transformed_81_id = dep_40.stg_source_25_id
    left join dep_41 on dep_1.int_transformed_81_id = dep_41.stg_source_104_id
    left join dep_42 on dep_1.int_transformed_81_id = dep_42.stg_source_56_id
    left join dep_43 on dep_1.int_transformed_81_id = dep_43.int_transformed_107_id
    left join dep_44 on dep_1.int_transformed_81_id = dep_44.stg_source_160_id
    left join dep_45 on dep_1.int_transformed_81_id = dep_45.stg_source_82_id
    left join dep_46 on dep_1.int_transformed_81_id = dep_46.int_transformed_145_id
    left join dep_47 on dep_1.int_transformed_81_id = dep_47.stg_source_103_id
    left join dep_48 on dep_1.int_transformed_81_id = dep_48.stg_source_107_id
    left join dep_49 on dep_1.int_transformed_81_id = dep_49.stg_source_120_id
    left join dep_50 on dep_1.int_transformed_81_id = dep_50.stg_source_147_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
