{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_121') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_4 as (
    select * from {{ ref('stg_source_57') }}
),

dep_5 as (
    select * from {{ ref('stg_source_161') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_7 as (
    select * from {{ ref('stg_source_140') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_130') }}
),

dep_9 as (
    select * from {{ ref('stg_source_159') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_11 as (
    select * from {{ ref('stg_source_141') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_13 as (
    select * from {{ ref('stg_source_9') }}
),

dep_14 as (
    select * from {{ ref('stg_source_123') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_146') }}
),

dep_16 as (
    select * from {{ ref('stg_source_111') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_18 as (
    select * from {{ ref('stg_source_164') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_20 as (
    select * from {{ ref('stg_source_119') }}
),

dep_21 as (
    select * from {{ ref('stg_source_101') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_24 as (
    select * from {{ ref('stg_source_137') }}
),

dep_25 as (
    select * from {{ ref('stg_source_11') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_27 as (
    select * from {{ ref('stg_source_158') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_31 as (
    select * from {{ ref('stg_source_19') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_35 as (
    select * from {{ ref('stg_source_62') }}
),

dep_36 as (
    select * from {{ ref('stg_source_105') }}
),

dep_37 as (
    select * from {{ ref('stg_source_21') }}
),

dep_38 as (
    select * from {{ ref('stg_source_120') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_33') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_43 as (
    select * from {{ ref('stg_source_74') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_46 as (
    select * from {{ ref('stg_source_2') }}
),

dep_47 as (
    select * from {{ ref('stg_source_153') }}
),

dep_48 as (
    select * from {{ ref('stg_source_102') }}
),

dep_49 as (
    select * from {{ ref('stg_source_127') }}
),

dep_50 as (
    select * from {{ ref('stg_source_150') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_121_id']) }} as surrogate_id,
        dep_1.stg_source_121_id as dim_entity_42_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_121_id = dep_2.int_transformed_13_id
    left join dep_3 on dep_1.stg_source_121_id = dep_3.int_transformed_92_id
    left join dep_4 on dep_1.stg_source_121_id = dep_4.stg_source_57_id
    left join dep_5 on dep_1.stg_source_121_id = dep_5.stg_source_161_id
    left join dep_6 on dep_1.stg_source_121_id = dep_6.int_transformed_76_id
    left join dep_7 on dep_1.stg_source_121_id = dep_7.stg_source_140_id
    left join dep_8 on dep_1.stg_source_121_id = dep_8.int_transformed_130_id
    left join dep_9 on dep_1.stg_source_121_id = dep_9.stg_source_159_id
    left join dep_10 on dep_1.stg_source_121_id = dep_10.int_transformed_77_id
    left join dep_11 on dep_1.stg_source_121_id = dep_11.stg_source_141_id
    left join dep_12 on dep_1.stg_source_121_id = dep_12.int_transformed_151_id
    left join dep_13 on dep_1.stg_source_121_id = dep_13.stg_source_9_id
    left join dep_14 on dep_1.stg_source_121_id = dep_14.stg_source_123_id
    left join dep_15 on dep_1.stg_source_121_id = dep_15.int_transformed_146_id
    left join dep_16 on dep_1.stg_source_121_id = dep_16.stg_source_111_id
    left join dep_17 on dep_1.stg_source_121_id = dep_17.int_transformed_19_id
    left join dep_18 on dep_1.stg_source_121_id = dep_18.stg_source_164_id
    left join dep_19 on dep_1.stg_source_121_id = dep_19.int_transformed_111_id
    left join dep_20 on dep_1.stg_source_121_id = dep_20.stg_source_119_id
    left join dep_21 on dep_1.stg_source_121_id = dep_21.stg_source_101_id
    left join dep_22 on dep_1.stg_source_121_id = dep_22.int_transformed_12_id
    left join dep_23 on dep_1.stg_source_121_id = dep_23.int_transformed_64_id
    left join dep_24 on dep_1.stg_source_121_id = dep_24.stg_source_137_id
    left join dep_25 on dep_1.stg_source_121_id = dep_25.stg_source_11_id
    left join dep_26 on dep_1.stg_source_121_id = dep_26.int_transformed_97_id
    left join dep_27 on dep_1.stg_source_121_id = dep_27.stg_source_158_id
    left join dep_28 on dep_1.stg_source_121_id = dep_28.int_transformed_51_id
    left join dep_29 on dep_1.stg_source_121_id = dep_29.int_transformed_36_id
    left join dep_30 on dep_1.stg_source_121_id = dep_30.int_transformed_155_id
    left join dep_31 on dep_1.stg_source_121_id = dep_31.stg_source_19_id
    left join dep_32 on dep_1.stg_source_121_id = dep_32.int_transformed_137_id
    left join dep_33 on dep_1.stg_source_121_id = dep_33.int_transformed_85_id
    left join dep_34 on dep_1.stg_source_121_id = dep_34.int_transformed_10_id
    left join dep_35 on dep_1.stg_source_121_id = dep_35.stg_source_62_id
    left join dep_36 on dep_1.stg_source_121_id = dep_36.stg_source_105_id
    left join dep_37 on dep_1.stg_source_121_id = dep_37.stg_source_21_id
    left join dep_38 on dep_1.stg_source_121_id = dep_38.stg_source_120_id
    left join dep_39 on dep_1.stg_source_121_id = dep_39.int_transformed_136_id
    left join dep_40 on dep_1.stg_source_121_id = dep_40.int_transformed_87_id
    left join dep_41 on dep_1.stg_source_121_id = dep_41.int_transformed_33_id
    left join dep_42 on dep_1.stg_source_121_id = dep_42.int_transformed_59_id
    left join dep_43 on dep_1.stg_source_121_id = dep_43.stg_source_74_id
    left join dep_44 on dep_1.stg_source_121_id = dep_44.int_transformed_129_id
    left join dep_45 on dep_1.stg_source_121_id = dep_45.int_transformed_122_id
    left join dep_46 on dep_1.stg_source_121_id = dep_46.stg_source_2_id
    left join dep_47 on dep_1.stg_source_121_id = dep_47.stg_source_153_id
    left join dep_48 on dep_1.stg_source_121_id = dep_48.stg_source_102_id
    left join dep_49 on dep_1.stg_source_121_id = dep_49.stg_source_127_id
    left join dep_50 on dep_1.stg_source_121_id = dep_50.stg_source_150_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
