{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_88') }}
),

dep_2 as (
    select * from {{ ref('stg_source_113') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_4 as (
    select * from {{ ref('stg_source_87') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_6 as (
    select * from {{ ref('stg_source_160') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_10 as (
    select * from {{ ref('stg_source_25') }}
),

dep_11 as (
    select * from {{ ref('stg_source_104') }}
),

dep_12 as (
    select * from {{ ref('stg_source_60') }}
),

dep_13 as (
    select * from {{ ref('stg_source_32') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_15 as (
    select * from {{ ref('stg_source_37') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_135') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_7') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_22 as (
    select * from {{ ref('stg_source_138') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_41') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_26 as (
    select * from {{ ref('stg_source_137') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_30 as (
    select * from {{ ref('stg_source_68') }}
),

dep_31 as (
    select * from {{ ref('stg_source_10') }}
),

dep_32 as (
    select * from {{ ref('stg_source_67') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_18') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_64') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_36 as (
    select * from {{ ref('stg_source_103') }}
),

dep_37 as (
    select * from {{ ref('stg_source_12') }}
),

dep_38 as (
    select * from {{ ref('stg_source_91') }}
),

dep_39 as (
    select * from {{ ref('stg_source_134') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_16') }}
),

dep_41 as (
    select * from {{ ref('stg_source_77') }}
),

dep_42 as (
    select * from {{ ref('stg_source_6') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_45 as (
    select * from {{ ref('stg_source_73') }}
),

dep_46 as (
    select * from {{ ref('stg_source_133') }}
),

dep_47 as (
    select * from {{ ref('stg_source_14') }}
),

dep_48 as (
    select * from {{ ref('stg_source_140') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_70') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_88_id']) }} as surrogate_id,
        dep_1.stg_source_88_id as fct_business_event_31_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_88_id = dep_2.stg_source_113_id
    left join dep_3 on dep_1.stg_source_88_id = dep_3.int_transformed_37_id
    left join dep_4 on dep_1.stg_source_88_id = dep_4.stg_source_87_id
    left join dep_5 on dep_1.stg_source_88_id = dep_5.int_transformed_104_id
    left join dep_6 on dep_1.stg_source_88_id = dep_6.stg_source_160_id
    left join dep_7 on dep_1.stg_source_88_id = dep_7.int_transformed_13_id
    left join dep_8 on dep_1.stg_source_88_id = dep_8.int_transformed_61_id
    left join dep_9 on dep_1.stg_source_88_id = dep_9.int_transformed_47_id
    left join dep_10 on dep_1.stg_source_88_id = dep_10.stg_source_25_id
    left join dep_11 on dep_1.stg_source_88_id = dep_11.stg_source_104_id
    left join dep_12 on dep_1.stg_source_88_id = dep_12.stg_source_60_id
    left join dep_13 on dep_1.stg_source_88_id = dep_13.stg_source_32_id
    left join dep_14 on dep_1.stg_source_88_id = dep_14.int_transformed_38_id
    left join dep_15 on dep_1.stg_source_88_id = dep_15.stg_source_37_id
    left join dep_16 on dep_1.stg_source_88_id = dep_16.int_transformed_52_id
    left join dep_17 on dep_1.stg_source_88_id = dep_17.int_transformed_135_id
    left join dep_18 on dep_1.stg_source_88_id = dep_18.int_transformed_78_id
    left join dep_19 on dep_1.stg_source_88_id = dep_19.int_transformed_7_id
    left join dep_20 on dep_1.stg_source_88_id = dep_20.int_transformed_55_id
    left join dep_21 on dep_1.stg_source_88_id = dep_21.int_transformed_43_id
    left join dep_22 on dep_1.stg_source_88_id = dep_22.stg_source_138_id
    left join dep_23 on dep_1.stg_source_88_id = dep_23.int_transformed_41_id
    left join dep_24 on dep_1.stg_source_88_id = dep_24.int_transformed_9_id
    left join dep_25 on dep_1.stg_source_88_id = dep_25.int_transformed_132_id
    left join dep_26 on dep_1.stg_source_88_id = dep_26.stg_source_137_id
    left join dep_27 on dep_1.stg_source_88_id = dep_27.int_transformed_111_id
    left join dep_28 on dep_1.stg_source_88_id = dep_28.int_transformed_120_id
    left join dep_29 on dep_1.stg_source_88_id = dep_29.int_transformed_54_id
    left join dep_30 on dep_1.stg_source_88_id = dep_30.stg_source_68_id
    left join dep_31 on dep_1.stg_source_88_id = dep_31.stg_source_10_id
    left join dep_32 on dep_1.stg_source_88_id = dep_32.stg_source_67_id
    left join dep_33 on dep_1.stg_source_88_id = dep_33.int_transformed_18_id
    left join dep_34 on dep_1.stg_source_88_id = dep_34.int_transformed_64_id
    left join dep_35 on dep_1.stg_source_88_id = dep_35.int_transformed_6_id
    left join dep_36 on dep_1.stg_source_88_id = dep_36.stg_source_103_id
    left join dep_37 on dep_1.stg_source_88_id = dep_37.stg_source_12_id
    left join dep_38 on dep_1.stg_source_88_id = dep_38.stg_source_91_id
    left join dep_39 on dep_1.stg_source_88_id = dep_39.stg_source_134_id
    left join dep_40 on dep_1.stg_source_88_id = dep_40.int_transformed_16_id
    left join dep_41 on dep_1.stg_source_88_id = dep_41.stg_source_77_id
    left join dep_42 on dep_1.stg_source_88_id = dep_42.stg_source_6_id
    left join dep_43 on dep_1.stg_source_88_id = dep_43.int_transformed_101_id
    left join dep_44 on dep_1.stg_source_88_id = dep_44.int_transformed_51_id
    left join dep_45 on dep_1.stg_source_88_id = dep_45.stg_source_73_id
    left join dep_46 on dep_1.stg_source_88_id = dep_46.stg_source_133_id
    left join dep_47 on dep_1.stg_source_88_id = dep_47.stg_source_14_id
    left join dep_48 on dep_1.stg_source_88_id = dep_48.stg_source_140_id
    left join dep_49 on dep_1.stg_source_88_id = dep_49.int_transformed_2_id
    left join dep_50 on dep_1.stg_source_88_id = dep_50.int_transformed_70_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
