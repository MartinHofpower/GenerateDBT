{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_2 as (
    select * from {{ ref('stg_source_127') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_125') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_6 as (
    select * from {{ ref('stg_source_36') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_12 as (
    select * from {{ ref('stg_source_46') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_15 as (
    select * from {{ ref('stg_source_27') }}
),

dep_16 as (
    select * from {{ ref('stg_source_67') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_18 as (
    select * from {{ ref('stg_source_111') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_20 as (
    select * from {{ ref('stg_source_165') }}
),

dep_21 as (
    select * from {{ ref('stg_source_72') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_24 as (
    select * from {{ ref('stg_source_38') }}
),

dep_25 as (
    select * from {{ ref('stg_source_162') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_30 as (
    select * from {{ ref('stg_source_7') }}
),

dep_31 as (
    select * from {{ ref('stg_source_21') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_115') }}
),

dep_33 as (
    select * from {{ ref('stg_source_122') }}
),

dep_34 as (
    select * from {{ ref('stg_source_139') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_82') }}
),

dep_41 as (
    select * from {{ ref('stg_source_80') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_43 as (
    select * from {{ ref('stg_source_90') }}
),

dep_44 as (
    select * from {{ ref('stg_source_61') }}
),

dep_45 as (
    select * from {{ ref('stg_source_92') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_47 as (
    select * from {{ ref('stg_source_40') }}
),

dep_48 as (
    select * from {{ ref('stg_source_57') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_111') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_138_id']) }} as surrogate_id,
        dep_1.int_transformed_138_id as dim_entity_168_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_138_id = dep_2.stg_source_127_id
    left join dep_3 on dep_1.int_transformed_138_id = dep_3.int_transformed_125_id
    left join dep_4 on dep_1.int_transformed_138_id = dep_4.int_transformed_131_id
    left join dep_5 on dep_1.int_transformed_138_id = dep_5.int_transformed_126_id
    left join dep_6 on dep_1.int_transformed_138_id = dep_6.stg_source_36_id
    left join dep_7 on dep_1.int_transformed_138_id = dep_7.int_transformed_128_id
    left join dep_8 on dep_1.int_transformed_138_id = dep_8.int_transformed_136_id
    left join dep_9 on dep_1.int_transformed_138_id = dep_9.int_transformed_110_id
    left join dep_10 on dep_1.int_transformed_138_id = dep_10.int_transformed_53_id
    left join dep_11 on dep_1.int_transformed_138_id = dep_11.int_transformed_11_id
    left join dep_12 on dep_1.int_transformed_138_id = dep_12.stg_source_46_id
    left join dep_13 on dep_1.int_transformed_138_id = dep_13.int_transformed_8_id
    left join dep_14 on dep_1.int_transformed_138_id = dep_14.int_transformed_89_id
    left join dep_15 on dep_1.int_transformed_138_id = dep_15.stg_source_27_id
    left join dep_16 on dep_1.int_transformed_138_id = dep_16.stg_source_67_id
    left join dep_17 on dep_1.int_transformed_138_id = dep_17.int_transformed_116_id
    left join dep_18 on dep_1.int_transformed_138_id = dep_18.stg_source_111_id
    left join dep_19 on dep_1.int_transformed_138_id = dep_19.int_transformed_93_id
    left join dep_20 on dep_1.int_transformed_138_id = dep_20.stg_source_165_id
    left join dep_21 on dep_1.int_transformed_138_id = dep_21.stg_source_72_id
    left join dep_22 on dep_1.int_transformed_138_id = dep_22.int_transformed_100_id
    left join dep_23 on dep_1.int_transformed_138_id = dep_23.int_transformed_23_id
    left join dep_24 on dep_1.int_transformed_138_id = dep_24.stg_source_38_id
    left join dep_25 on dep_1.int_transformed_138_id = dep_25.stg_source_162_id
    left join dep_26 on dep_1.int_transformed_138_id = dep_26.int_transformed_80_id
    left join dep_27 on dep_1.int_transformed_138_id = dep_27.int_transformed_127_id
    left join dep_28 on dep_1.int_transformed_138_id = dep_28.int_transformed_91_id
    left join dep_29 on dep_1.int_transformed_138_id = dep_29.int_transformed_35_id
    left join dep_30 on dep_1.int_transformed_138_id = dep_30.stg_source_7_id
    left join dep_31 on dep_1.int_transformed_138_id = dep_31.stg_source_21_id
    left join dep_32 on dep_1.int_transformed_138_id = dep_32.int_transformed_115_id
    left join dep_33 on dep_1.int_transformed_138_id = dep_33.stg_source_122_id
    left join dep_34 on dep_1.int_transformed_138_id = dep_34.stg_source_139_id
    left join dep_35 on dep_1.int_transformed_138_id = dep_35.int_transformed_42_id
    left join dep_36 on dep_1.int_transformed_138_id = dep_36.int_transformed_106_id
    left join dep_37 on dep_1.int_transformed_138_id = dep_37.int_transformed_104_id
    left join dep_38 on dep_1.int_transformed_138_id = dep_38.int_transformed_19_id
    left join dep_39 on dep_1.int_transformed_138_id = dep_39.int_transformed_48_id
    left join dep_40 on dep_1.int_transformed_138_id = dep_40.int_transformed_82_id
    left join dep_41 on dep_1.int_transformed_138_id = dep_41.stg_source_80_id
    left join dep_42 on dep_1.int_transformed_138_id = dep_42.int_transformed_61_id
    left join dep_43 on dep_1.int_transformed_138_id = dep_43.stg_source_90_id
    left join dep_44 on dep_1.int_transformed_138_id = dep_44.stg_source_61_id
    left join dep_45 on dep_1.int_transformed_138_id = dep_45.stg_source_92_id
    left join dep_46 on dep_1.int_transformed_138_id = dep_46.int_transformed_34_id
    left join dep_47 on dep_1.int_transformed_138_id = dep_47.stg_source_40_id
    left join dep_48 on dep_1.int_transformed_138_id = dep_48.stg_source_57_id
    left join dep_49 on dep_1.int_transformed_138_id = dep_49.int_transformed_14_id
    left join dep_50 on dep_1.int_transformed_138_id = dep_50.int_transformed_111_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
