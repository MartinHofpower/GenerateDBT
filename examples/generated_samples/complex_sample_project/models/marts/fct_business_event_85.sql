{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_77') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_4 as (
    select * from {{ ref('stg_source_89') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_24') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_8 as (
    select * from {{ ref('stg_source_104') }}
),

dep_9 as (
    select * from {{ ref('stg_source_69') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_11 as (
    select * from {{ ref('stg_source_122') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_15 as (
    select * from {{ ref('stg_source_138') }}
),

dep_16 as (
    select * from {{ ref('stg_source_123') }}
),

dep_17 as (
    select * from {{ ref('stg_source_156') }}
),

dep_18 as (
    select * from {{ ref('stg_source_129') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_20 as (
    select * from {{ ref('stg_source_28') }}
),

dep_21 as (
    select * from {{ ref('stg_source_144') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_92') }}
),

dep_24 as (
    select * from {{ ref('stg_source_83') }}
),

dep_25 as (
    select * from {{ ref('stg_source_79') }}
),

dep_26 as (
    select * from {{ ref('stg_source_117') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_29 as (
    select * from {{ ref('stg_source_6') }}
),

dep_30 as (
    select * from {{ ref('stg_source_130') }}
),

dep_31 as (
    select * from {{ ref('stg_source_44') }}
),

dep_32 as (
    select * from {{ ref('stg_source_15') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_36 as (
    select * from {{ ref('stg_source_110') }}
),

dep_37 as (
    select * from {{ ref('stg_source_151') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_95') }}
),

dep_40 as (
    select * from {{ ref('stg_source_114') }}
),

dep_41 as (
    select * from {{ ref('stg_source_96') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_43 as (
    select * from {{ ref('stg_source_103') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_45 as (
    select * from {{ ref('stg_source_145') }}
),

dep_46 as (
    select * from {{ ref('stg_source_39') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_109') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_151') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_20_id']) }} as surrogate_id,
        dep_1.int_transformed_20_id as fct_business_event_85_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_20_id = dep_2.int_transformed_77_id
    left join dep_3 on dep_1.int_transformed_20_id = dep_3.int_transformed_133_id
    left join dep_4 on dep_1.int_transformed_20_id = dep_4.stg_source_89_id
    left join dep_5 on dep_1.int_transformed_20_id = dep_5.int_transformed_24_id
    left join dep_6 on dep_1.int_transformed_20_id = dep_6.int_transformed_101_id
    left join dep_7 on dep_1.int_transformed_20_id = dep_7.int_transformed_127_id
    left join dep_8 on dep_1.int_transformed_20_id = dep_8.stg_source_104_id
    left join dep_9 on dep_1.int_transformed_20_id = dep_9.stg_source_69_id
    left join dep_10 on dep_1.int_transformed_20_id = dep_10.int_transformed_164_id
    left join dep_11 on dep_1.int_transformed_20_id = dep_11.stg_source_122_id
    left join dep_12 on dep_1.int_transformed_20_id = dep_12.int_transformed_161_id
    left join dep_13 on dep_1.int_transformed_20_id = dep_13.int_transformed_65_id
    left join dep_14 on dep_1.int_transformed_20_id = dep_14.int_transformed_111_id
    left join dep_15 on dep_1.int_transformed_20_id = dep_15.stg_source_138_id
    left join dep_16 on dep_1.int_transformed_20_id = dep_16.stg_source_123_id
    left join dep_17 on dep_1.int_transformed_20_id = dep_17.stg_source_156_id
    left join dep_18 on dep_1.int_transformed_20_id = dep_18.stg_source_129_id
    left join dep_19 on dep_1.int_transformed_20_id = dep_19.int_transformed_78_id
    left join dep_20 on dep_1.int_transformed_20_id = dep_20.stg_source_28_id
    left join dep_21 on dep_1.int_transformed_20_id = dep_21.stg_source_144_id
    left join dep_22 on dep_1.int_transformed_20_id = dep_22.int_transformed_23_id
    left join dep_23 on dep_1.int_transformed_20_id = dep_23.int_transformed_92_id
    left join dep_24 on dep_1.int_transformed_20_id = dep_24.stg_source_83_id
    left join dep_25 on dep_1.int_transformed_20_id = dep_25.stg_source_79_id
    left join dep_26 on dep_1.int_transformed_20_id = dep_26.stg_source_117_id
    left join dep_27 on dep_1.int_transformed_20_id = dep_27.int_transformed_45_id
    left join dep_28 on dep_1.int_transformed_20_id = dep_28.int_transformed_129_id
    left join dep_29 on dep_1.int_transformed_20_id = dep_29.stg_source_6_id
    left join dep_30 on dep_1.int_transformed_20_id = dep_30.stg_source_130_id
    left join dep_31 on dep_1.int_transformed_20_id = dep_31.stg_source_44_id
    left join dep_32 on dep_1.int_transformed_20_id = dep_32.stg_source_15_id
    left join dep_33 on dep_1.int_transformed_20_id = dep_33.int_transformed_116_id
    left join dep_34 on dep_1.int_transformed_20_id = dep_34.int_transformed_52_id
    left join dep_35 on dep_1.int_transformed_20_id = dep_35.int_transformed_5_id
    left join dep_36 on dep_1.int_transformed_20_id = dep_36.stg_source_110_id
    left join dep_37 on dep_1.int_transformed_20_id = dep_37.stg_source_151_id
    left join dep_38 on dep_1.int_transformed_20_id = dep_38.int_transformed_90_id
    left join dep_39 on dep_1.int_transformed_20_id = dep_39.int_transformed_95_id
    left join dep_40 on dep_1.int_transformed_20_id = dep_40.stg_source_114_id
    left join dep_41 on dep_1.int_transformed_20_id = dep_41.stg_source_96_id
    left join dep_42 on dep_1.int_transformed_20_id = dep_42.int_transformed_160_id
    left join dep_43 on dep_1.int_transformed_20_id = dep_43.stg_source_103_id
    left join dep_44 on dep_1.int_transformed_20_id = dep_44.int_transformed_41_id
    left join dep_45 on dep_1.int_transformed_20_id = dep_45.stg_source_145_id
    left join dep_46 on dep_1.int_transformed_20_id = dep_46.stg_source_39_id
    left join dep_47 on dep_1.int_transformed_20_id = dep_47.int_transformed_109_id
    left join dep_48 on dep_1.int_transformed_20_id = dep_48.int_transformed_7_id
    left join dep_49 on dep_1.int_transformed_20_id = dep_49.int_transformed_36_id
    left join dep_50 on dep_1.int_transformed_20_id = dep_50.int_transformed_151_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
