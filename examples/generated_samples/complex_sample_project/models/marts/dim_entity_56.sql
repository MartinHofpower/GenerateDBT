{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_3 as (
    select * from {{ ref('stg_source_128') }}
),

dep_4 as (
    select * from {{ ref('stg_source_113') }}
),

dep_5 as (
    select * from {{ ref('stg_source_53') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_8 as (
    select * from {{ ref('stg_source_71') }}
),

dep_9 as (
    select * from {{ ref('stg_source_3') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_11 as (
    select * from {{ ref('stg_source_96') }}
),

dep_12 as (
    select * from {{ ref('stg_source_50') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_134') }}
),

dep_14 as (
    select * from {{ ref('stg_source_121') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_16 as (
    select * from {{ ref('stg_source_22') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_18 as (
    select * from {{ ref('stg_source_110') }}
),

dep_19 as (
    select * from {{ ref('stg_source_38') }}
),

dep_20 as (
    select * from {{ ref('stg_source_31') }}
),

dep_21 as (
    select * from {{ ref('stg_source_40') }}
),

dep_22 as (
    select * from {{ ref('stg_source_52') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_25 as (
    select * from {{ ref('stg_source_75') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_27 as (
    select * from {{ ref('stg_source_27') }}
),

dep_28 as (
    select * from {{ ref('stg_source_100') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_30 as (
    select * from {{ ref('stg_source_151') }}
),

dep_31 as (
    select * from {{ ref('stg_source_28') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_11') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_35 as (
    select * from {{ ref('stg_source_32') }}
),

dep_36 as (
    select * from {{ ref('stg_source_58') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_42 as (
    select * from {{ ref('stg_source_72') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_45 as (
    select * from {{ ref('stg_source_135') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_47 as (
    select * from {{ ref('stg_source_36') }}
),

dep_48 as (
    select * from {{ ref('stg_source_29') }}
),

dep_49 as (
    select * from {{ ref('stg_source_155') }}
),

dep_50 as (
    select * from {{ ref('stg_source_103') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_126_id']) }} as surrogate_id,
        dep_1.int_transformed_126_id as dim_entity_56_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_126_id = dep_2.int_transformed_92_id
    left join dep_3 on dep_1.int_transformed_126_id = dep_3.stg_source_128_id
    left join dep_4 on dep_1.int_transformed_126_id = dep_4.stg_source_113_id
    left join dep_5 on dep_1.int_transformed_126_id = dep_5.stg_source_53_id
    left join dep_6 on dep_1.int_transformed_126_id = dep_6.int_transformed_61_id
    left join dep_7 on dep_1.int_transformed_126_id = dep_7.int_transformed_72_id
    left join dep_8 on dep_1.int_transformed_126_id = dep_8.stg_source_71_id
    left join dep_9 on dep_1.int_transformed_126_id = dep_9.stg_source_3_id
    left join dep_10 on dep_1.int_transformed_126_id = dep_10.int_transformed_114_id
    left join dep_11 on dep_1.int_transformed_126_id = dep_11.stg_source_96_id
    left join dep_12 on dep_1.int_transformed_126_id = dep_12.stg_source_50_id
    left join dep_13 on dep_1.int_transformed_126_id = dep_13.int_transformed_134_id
    left join dep_14 on dep_1.int_transformed_126_id = dep_14.stg_source_121_id
    left join dep_15 on dep_1.int_transformed_126_id = dep_15.int_transformed_54_id
    left join dep_16 on dep_1.int_transformed_126_id = dep_16.stg_source_22_id
    left join dep_17 on dep_1.int_transformed_126_id = dep_17.int_transformed_59_id
    left join dep_18 on dep_1.int_transformed_126_id = dep_18.stg_source_110_id
    left join dep_19 on dep_1.int_transformed_126_id = dep_19.stg_source_38_id
    left join dep_20 on dep_1.int_transformed_126_id = dep_20.stg_source_31_id
    left join dep_21 on dep_1.int_transformed_126_id = dep_21.stg_source_40_id
    left join dep_22 on dep_1.int_transformed_126_id = dep_22.stg_source_52_id
    left join dep_23 on dep_1.int_transformed_126_id = dep_23.int_transformed_46_id
    left join dep_24 on dep_1.int_transformed_126_id = dep_24.int_transformed_146_id
    left join dep_25 on dep_1.int_transformed_126_id = dep_25.stg_source_75_id
    left join dep_26 on dep_1.int_transformed_126_id = dep_26.int_transformed_12_id
    left join dep_27 on dep_1.int_transformed_126_id = dep_27.stg_source_27_id
    left join dep_28 on dep_1.int_transformed_126_id = dep_28.stg_source_100_id
    left join dep_29 on dep_1.int_transformed_126_id = dep_29.int_transformed_26_id
    left join dep_30 on dep_1.int_transformed_126_id = dep_30.stg_source_151_id
    left join dep_31 on dep_1.int_transformed_126_id = dep_31.stg_source_28_id
    left join dep_32 on dep_1.int_transformed_126_id = dep_32.int_transformed_154_id
    left join dep_33 on dep_1.int_transformed_126_id = dep_33.int_transformed_11_id
    left join dep_34 on dep_1.int_transformed_126_id = dep_34.int_transformed_155_id
    left join dep_35 on dep_1.int_transformed_126_id = dep_35.stg_source_32_id
    left join dep_36 on dep_1.int_transformed_126_id = dep_36.stg_source_58_id
    left join dep_37 on dep_1.int_transformed_126_id = dep_37.int_transformed_33_id
    left join dep_38 on dep_1.int_transformed_126_id = dep_38.int_transformed_4_id
    left join dep_39 on dep_1.int_transformed_126_id = dep_39.int_transformed_160_id
    left join dep_40 on dep_1.int_transformed_126_id = dep_40.int_transformed_39_id
    left join dep_41 on dep_1.int_transformed_126_id = dep_41.int_transformed_156_id
    left join dep_42 on dep_1.int_transformed_126_id = dep_42.stg_source_72_id
    left join dep_43 on dep_1.int_transformed_126_id = dep_43.int_transformed_144_id
    left join dep_44 on dep_1.int_transformed_126_id = dep_44.int_transformed_108_id
    left join dep_45 on dep_1.int_transformed_126_id = dep_45.stg_source_135_id
    left join dep_46 on dep_1.int_transformed_126_id = dep_46.int_transformed_48_id
    left join dep_47 on dep_1.int_transformed_126_id = dep_47.stg_source_36_id
    left join dep_48 on dep_1.int_transformed_126_id = dep_48.stg_source_29_id
    left join dep_49 on dep_1.int_transformed_126_id = dep_49.stg_source_155_id
    left join dep_50 on dep_1.int_transformed_126_id = dep_50.stg_source_103_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
