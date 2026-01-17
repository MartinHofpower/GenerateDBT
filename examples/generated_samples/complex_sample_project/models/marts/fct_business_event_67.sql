{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_2 as (
    select * from {{ ref('stg_source_3') }}
),

dep_3 as (
    select * from {{ ref('stg_source_122') }}
),

dep_4 as (
    select * from {{ ref('stg_source_44') }}
),

dep_5 as (
    select * from {{ ref('stg_source_2') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_91') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_9 as (
    select * from {{ ref('stg_source_98') }}
),

dep_10 as (
    select * from {{ ref('stg_source_144') }}
),

dep_11 as (
    select * from {{ ref('stg_source_142') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_14 as (
    select * from {{ ref('stg_source_110') }}
),

dep_15 as (
    select * from {{ ref('stg_source_133') }}
),

dep_16 as (
    select * from {{ ref('stg_source_20') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_19 as (
    select * from {{ ref('stg_source_68') }}
),

dep_20 as (
    select * from {{ ref('stg_source_92') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_23 as (
    select * from {{ ref('stg_source_39') }}
),

dep_24 as (
    select * from {{ ref('stg_source_112') }}
),

dep_25 as (
    select * from {{ ref('stg_source_66') }}
),

dep_26 as (
    select * from {{ ref('stg_source_47') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_50') }}
),

dep_29 as (
    select * from {{ ref('stg_source_15') }}
),

dep_30 as (
    select * from {{ ref('stg_source_5') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_34 as (
    select * from {{ ref('stg_source_75') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_36 as (
    select * from {{ ref('stg_source_74') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_140') }}
),

dep_41 as (
    select * from {{ ref('stg_source_135') }}
),

dep_42 as (
    select * from {{ ref('stg_source_7') }}
),

dep_43 as (
    select * from {{ ref('stg_source_119') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_45 as (
    select * from {{ ref('stg_source_116') }}
),

dep_46 as (
    select * from {{ ref('stg_source_62') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_49 as (
    select * from {{ ref('stg_source_138') }}
),

dep_50 as (
    select * from {{ ref('stg_source_109') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.int_transformed_99_id']) }} as surrogate_id,
        dep_1.int_transformed_99_id as fct_business_event_67_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_99_id = dep_2.stg_source_3_id
    left join dep_3 on dep_1.int_transformed_99_id = dep_3.stg_source_122_id
    left join dep_4 on dep_1.int_transformed_99_id = dep_4.stg_source_44_id
    left join dep_5 on dep_1.int_transformed_99_id = dep_5.stg_source_2_id
    left join dep_6 on dep_1.int_transformed_99_id = dep_6.int_transformed_98_id
    left join dep_7 on dep_1.int_transformed_99_id = dep_7.int_transformed_91_id
    left join dep_8 on dep_1.int_transformed_99_id = dep_8.int_transformed_151_id
    left join dep_9 on dep_1.int_transformed_99_id = dep_9.stg_source_98_id
    left join dep_10 on dep_1.int_transformed_99_id = dep_10.stg_source_144_id
    left join dep_11 on dep_1.int_transformed_99_id = dep_11.stg_source_142_id
    left join dep_12 on dep_1.int_transformed_99_id = dep_12.int_transformed_153_id
    left join dep_13 on dep_1.int_transformed_99_id = dep_13.int_transformed_34_id
    left join dep_14 on dep_1.int_transformed_99_id = dep_14.stg_source_110_id
    left join dep_15 on dep_1.int_transformed_99_id = dep_15.stg_source_133_id
    left join dep_16 on dep_1.int_transformed_99_id = dep_16.stg_source_20_id
    left join dep_17 on dep_1.int_transformed_99_id = dep_17.int_transformed_162_id
    left join dep_18 on dep_1.int_transformed_99_id = dep_18.int_transformed_122_id
    left join dep_19 on dep_1.int_transformed_99_id = dep_19.stg_source_68_id
    left join dep_20 on dep_1.int_transformed_99_id = dep_20.stg_source_92_id
    left join dep_21 on dep_1.int_transformed_99_id = dep_21.int_transformed_23_id
    left join dep_22 on dep_1.int_transformed_99_id = dep_22.int_transformed_101_id
    left join dep_23 on dep_1.int_transformed_99_id = dep_23.stg_source_39_id
    left join dep_24 on dep_1.int_transformed_99_id = dep_24.stg_source_112_id
    left join dep_25 on dep_1.int_transformed_99_id = dep_25.stg_source_66_id
    left join dep_26 on dep_1.int_transformed_99_id = dep_26.stg_source_47_id
    left join dep_27 on dep_1.int_transformed_99_id = dep_27.int_transformed_77_id
    left join dep_28 on dep_1.int_transformed_99_id = dep_28.int_transformed_50_id
    left join dep_29 on dep_1.int_transformed_99_id = dep_29.stg_source_15_id
    left join dep_30 on dep_1.int_transformed_99_id = dep_30.stg_source_5_id
    left join dep_31 on dep_1.int_transformed_99_id = dep_31.int_transformed_116_id
    left join dep_32 on dep_1.int_transformed_99_id = dep_32.int_transformed_126_id
    left join dep_33 on dep_1.int_transformed_99_id = dep_33.int_transformed_56_id
    left join dep_34 on dep_1.int_transformed_99_id = dep_34.stg_source_75_id
    left join dep_35 on dep_1.int_transformed_99_id = dep_35.int_transformed_103_id
    left join dep_36 on dep_1.int_transformed_99_id = dep_36.stg_source_74_id
    left join dep_37 on dep_1.int_transformed_99_id = dep_37.int_transformed_19_id
    left join dep_38 on dep_1.int_transformed_99_id = dep_38.int_transformed_85_id
    left join dep_39 on dep_1.int_transformed_99_id = dep_39.int_transformed_124_id
    left join dep_40 on dep_1.int_transformed_99_id = dep_40.int_transformed_140_id
    left join dep_41 on dep_1.int_transformed_99_id = dep_41.stg_source_135_id
    left join dep_42 on dep_1.int_transformed_99_id = dep_42.stg_source_7_id
    left join dep_43 on dep_1.int_transformed_99_id = dep_43.stg_source_119_id
    left join dep_44 on dep_1.int_transformed_99_id = dep_44.int_transformed_13_id
    left join dep_45 on dep_1.int_transformed_99_id = dep_45.stg_source_116_id
    left join dep_46 on dep_1.int_transformed_99_id = dep_46.stg_source_62_id
    left join dep_47 on dep_1.int_transformed_99_id = dep_47.int_transformed_2_id
    left join dep_48 on dep_1.int_transformed_99_id = dep_48.int_transformed_150_id
    left join dep_49 on dep_1.int_transformed_99_id = dep_49.stg_source_138_id
    left join dep_50 on dep_1.int_transformed_99_id = dep_50.stg_source_109_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
