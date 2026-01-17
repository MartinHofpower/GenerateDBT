{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_158') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_31') }}
),

dep_3 as (
    select * from {{ ref('stg_source_1') }}
),

dep_4 as (
    select * from {{ ref('stg_source_27') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_7 as (
    select * from {{ ref('stg_source_91') }}
),

dep_8 as (
    select * from {{ ref('stg_source_104') }}
),

dep_9 as (
    select * from {{ ref('stg_source_150') }}
),

dep_10 as (
    select * from {{ ref('stg_source_25') }}
),

dep_11 as (
    select * from {{ ref('stg_source_92') }}
),

dep_12 as (
    select * from {{ ref('stg_source_8') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_17 as (
    select * from {{ ref('stg_source_82') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_19 as (
    select * from {{ ref('stg_source_42') }}
),

dep_20 as (
    select * from {{ ref('stg_source_40') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_23 as (
    select * from {{ ref('stg_source_62') }}
),

dep_24 as (
    select * from {{ ref('stg_source_122') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_27') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_27 as (
    select * from {{ ref('stg_source_63') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_30 as (
    select * from {{ ref('stg_source_108') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_32 as (
    select * from {{ ref('stg_source_22') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_34 as (
    select * from {{ ref('stg_source_161') }}
),

dep_35 as (
    select * from {{ ref('stg_source_15') }}
),

dep_36 as (
    select * from {{ ref('stg_source_140') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_38 as (
    select * from {{ ref('stg_source_164') }}
),

dep_39 as (
    select * from {{ ref('stg_source_24') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_41 as (
    select * from {{ ref('stg_source_156') }}
),

dep_42 as (
    select * from {{ ref('stg_source_81') }}
),

dep_43 as (
    select * from {{ ref('stg_source_50') }}
),

dep_44 as (
    select * from {{ ref('stg_source_136') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_46 as (
    select * from {{ ref('stg_source_17') }}
),

dep_47 as (
    select * from {{ ref('stg_source_9') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_49 as (
    select * from {{ ref('stg_source_134') }}
),

dep_50 as (
    select * from {{ ref('stg_source_87') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_158_id']) }} as surrogate_id,
        dep_1.int_transformed_158_id as dim_entity_146_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_158_id = dep_2.int_transformed_31_id
    left join dep_3 on dep_1.int_transformed_158_id = dep_3.stg_source_1_id
    left join dep_4 on dep_1.int_transformed_158_id = dep_4.stg_source_27_id
    left join dep_5 on dep_1.int_transformed_158_id = dep_5.int_transformed_28_id
    left join dep_6 on dep_1.int_transformed_158_id = dep_6.int_transformed_94_id
    left join dep_7 on dep_1.int_transformed_158_id = dep_7.stg_source_91_id
    left join dep_8 on dep_1.int_transformed_158_id = dep_8.stg_source_104_id
    left join dep_9 on dep_1.int_transformed_158_id = dep_9.stg_source_150_id
    left join dep_10 on dep_1.int_transformed_158_id = dep_10.stg_source_25_id
    left join dep_11 on dep_1.int_transformed_158_id = dep_11.stg_source_92_id
    left join dep_12 on dep_1.int_transformed_158_id = dep_12.stg_source_8_id
    left join dep_13 on dep_1.int_transformed_158_id = dep_13.int_transformed_52_id
    left join dep_14 on dep_1.int_transformed_158_id = dep_14.int_transformed_47_id
    left join dep_15 on dep_1.int_transformed_158_id = dep_15.int_transformed_97_id
    left join dep_16 on dep_1.int_transformed_158_id = dep_16.int_transformed_117_id
    left join dep_17 on dep_1.int_transformed_158_id = dep_17.stg_source_82_id
    left join dep_18 on dep_1.int_transformed_158_id = dep_18.int_transformed_155_id
    left join dep_19 on dep_1.int_transformed_158_id = dep_19.stg_source_42_id
    left join dep_20 on dep_1.int_transformed_158_id = dep_20.stg_source_40_id
    left join dep_21 on dep_1.int_transformed_158_id = dep_21.int_transformed_71_id
    left join dep_22 on dep_1.int_transformed_158_id = dep_22.int_transformed_60_id
    left join dep_23 on dep_1.int_transformed_158_id = dep_23.stg_source_62_id
    left join dep_24 on dep_1.int_transformed_158_id = dep_24.stg_source_122_id
    left join dep_25 on dep_1.int_transformed_158_id = dep_25.int_transformed_27_id
    left join dep_26 on dep_1.int_transformed_158_id = dep_26.int_transformed_86_id
    left join dep_27 on dep_1.int_transformed_158_id = dep_27.stg_source_63_id
    left join dep_28 on dep_1.int_transformed_158_id = dep_28.int_transformed_50_id
    left join dep_29 on dep_1.int_transformed_158_id = dep_29.int_transformed_108_id
    left join dep_30 on dep_1.int_transformed_158_id = dep_30.stg_source_108_id
    left join dep_31 on dep_1.int_transformed_158_id = dep_31.int_transformed_113_id
    left join dep_32 on dep_1.int_transformed_158_id = dep_32.stg_source_22_id
    left join dep_33 on dep_1.int_transformed_158_id = dep_33.int_transformed_4_id
    left join dep_34 on dep_1.int_transformed_158_id = dep_34.stg_source_161_id
    left join dep_35 on dep_1.int_transformed_158_id = dep_35.stg_source_15_id
    left join dep_36 on dep_1.int_transformed_158_id = dep_36.stg_source_140_id
    left join dep_37 on dep_1.int_transformed_158_id = dep_37.int_transformed_67_id
    left join dep_38 on dep_1.int_transformed_158_id = dep_38.stg_source_164_id
    left join dep_39 on dep_1.int_transformed_158_id = dep_39.stg_source_24_id
    left join dep_40 on dep_1.int_transformed_158_id = dep_40.int_transformed_62_id
    left join dep_41 on dep_1.int_transformed_158_id = dep_41.stg_source_156_id
    left join dep_42 on dep_1.int_transformed_158_id = dep_42.stg_source_81_id
    left join dep_43 on dep_1.int_transformed_158_id = dep_43.stg_source_50_id
    left join dep_44 on dep_1.int_transformed_158_id = dep_44.stg_source_136_id
    left join dep_45 on dep_1.int_transformed_158_id = dep_45.int_transformed_98_id
    left join dep_46 on dep_1.int_transformed_158_id = dep_46.stg_source_17_id
    left join dep_47 on dep_1.int_transformed_158_id = dep_47.stg_source_9_id
    left join dep_48 on dep_1.int_transformed_158_id = dep_48.int_transformed_18_id
    left join dep_49 on dep_1.int_transformed_158_id = dep_49.stg_source_134_id
    left join dep_50 on dep_1.int_transformed_158_id = dep_50.stg_source_87_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
