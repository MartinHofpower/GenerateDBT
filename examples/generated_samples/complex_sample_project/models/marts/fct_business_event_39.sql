{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_103') }}
),

dep_2 as (
    select * from {{ ref('stg_source_66') }}
),

dep_3 as (
    select * from {{ ref('stg_source_30') }}
),

dep_4 as (
    select * from {{ ref('stg_source_159') }}
),

dep_5 as (
    select * from {{ ref('stg_source_14') }}
),

dep_6 as (
    select * from {{ ref('stg_source_52') }}
),

dep_7 as (
    select * from {{ ref('stg_source_128') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_72') }}
),

dep_9 as (
    select * from {{ ref('stg_source_155') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_11 as (
    select * from {{ ref('stg_source_131') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_36') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_32') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_18 as (
    select * from {{ ref('stg_source_96') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_20 as (
    select * from {{ ref('stg_source_75') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_22 as (
    select * from {{ ref('stg_source_9') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_24 as (
    select * from {{ ref('stg_source_6') }}
),

dep_25 as (
    select * from {{ ref('stg_source_4') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_147') }}
),

dep_27 as (
    select * from {{ ref('stg_source_150') }}
),

dep_28 as (
    select * from {{ ref('stg_source_49') }}
),

dep_29 as (
    select * from {{ ref('stg_source_37') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_31 as (
    select * from {{ ref('stg_source_119') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_10') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_35 as (
    select * from {{ ref('stg_source_62') }}
),

dep_36 as (
    select * from {{ ref('stg_source_74') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_38 as (
    select * from {{ ref('stg_source_24') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_26') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_63') }}
),

dep_42 as (
    select * from {{ ref('stg_source_87') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_44 as (
    select * from {{ ref('stg_source_118') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_47 as (
    select * from {{ ref('stg_source_165') }}
),

dep_48 as (
    select * from {{ ref('stg_source_18') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_111') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_103_id']) }} as surrogate_id,
        dep_1.stg_source_103_id as fct_business_event_39_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_103_id = dep_2.stg_source_66_id
    left join dep_3 on dep_1.stg_source_103_id = dep_3.stg_source_30_id
    left join dep_4 on dep_1.stg_source_103_id = dep_4.stg_source_159_id
    left join dep_5 on dep_1.stg_source_103_id = dep_5.stg_source_14_id
    left join dep_6 on dep_1.stg_source_103_id = dep_6.stg_source_52_id
    left join dep_7 on dep_1.stg_source_103_id = dep_7.stg_source_128_id
    left join dep_8 on dep_1.stg_source_103_id = dep_8.int_transformed_72_id
    left join dep_9 on dep_1.stg_source_103_id = dep_9.stg_source_155_id
    left join dep_10 on dep_1.stg_source_103_id = dep_10.int_transformed_86_id
    left join dep_11 on dep_1.stg_source_103_id = dep_11.stg_source_131_id
    left join dep_12 on dep_1.stg_source_103_id = dep_12.int_transformed_43_id
    left join dep_13 on dep_1.stg_source_103_id = dep_13.int_transformed_36_id
    left join dep_14 on dep_1.stg_source_103_id = dep_14.int_transformed_99_id
    left join dep_15 on dep_1.stg_source_103_id = dep_15.int_transformed_32_id
    left join dep_16 on dep_1.stg_source_103_id = dep_16.int_transformed_163_id
    left join dep_17 on dep_1.stg_source_103_id = dep_17.int_transformed_155_id
    left join dep_18 on dep_1.stg_source_103_id = dep_18.stg_source_96_id
    left join dep_19 on dep_1.stg_source_103_id = dep_19.int_transformed_42_id
    left join dep_20 on dep_1.stg_source_103_id = dep_20.stg_source_75_id
    left join dep_21 on dep_1.stg_source_103_id = dep_21.int_transformed_73_id
    left join dep_22 on dep_1.stg_source_103_id = dep_22.stg_source_9_id
    left join dep_23 on dep_1.stg_source_103_id = dep_23.int_transformed_23_id
    left join dep_24 on dep_1.stg_source_103_id = dep_24.stg_source_6_id
    left join dep_25 on dep_1.stg_source_103_id = dep_25.stg_source_4_id
    left join dep_26 on dep_1.stg_source_103_id = dep_26.int_transformed_147_id
    left join dep_27 on dep_1.stg_source_103_id = dep_27.stg_source_150_id
    left join dep_28 on dep_1.stg_source_103_id = dep_28.stg_source_49_id
    left join dep_29 on dep_1.stg_source_103_id = dep_29.stg_source_37_id
    left join dep_30 on dep_1.stg_source_103_id = dep_30.int_transformed_90_id
    left join dep_31 on dep_1.stg_source_103_id = dep_31.stg_source_119_id
    left join dep_32 on dep_1.stg_source_103_id = dep_32.int_transformed_10_id
    left join dep_33 on dep_1.stg_source_103_id = dep_33.int_transformed_104_id
    left join dep_34 on dep_1.stg_source_103_id = dep_34.int_transformed_117_id
    left join dep_35 on dep_1.stg_source_103_id = dep_35.stg_source_62_id
    left join dep_36 on dep_1.stg_source_103_id = dep_36.stg_source_74_id
    left join dep_37 on dep_1.stg_source_103_id = dep_37.int_transformed_85_id
    left join dep_38 on dep_1.stg_source_103_id = dep_38.stg_source_24_id
    left join dep_39 on dep_1.stg_source_103_id = dep_39.int_transformed_35_id
    left join dep_40 on dep_1.stg_source_103_id = dep_40.int_transformed_26_id
    left join dep_41 on dep_1.stg_source_103_id = dep_41.int_transformed_63_id
    left join dep_42 on dep_1.stg_source_103_id = dep_42.stg_source_87_id
    left join dep_43 on dep_1.stg_source_103_id = dep_43.int_transformed_157_id
    left join dep_44 on dep_1.stg_source_103_id = dep_44.stg_source_118_id
    left join dep_45 on dep_1.stg_source_103_id = dep_45.int_transformed_153_id
    left join dep_46 on dep_1.stg_source_103_id = dep_46.int_transformed_133_id
    left join dep_47 on dep_1.stg_source_103_id = dep_47.stg_source_165_id
    left join dep_48 on dep_1.stg_source_103_id = dep_48.stg_source_18_id
    left join dep_49 on dep_1.stg_source_103_id = dep_49.int_transformed_126_id
    left join dep_50 on dep_1.stg_source_103_id = dep_50.int_transformed_111_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
