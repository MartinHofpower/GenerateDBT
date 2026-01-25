{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_74') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_3 as (
    select * from {{ ref('stg_source_130') }}
),

dep_4 as (
    select * from {{ ref('stg_source_105') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_112') }}
),

dep_6 as (
    select * from {{ ref('stg_source_51') }}
),

dep_7 as (
    select * from {{ ref('stg_source_49') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_150') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_155') }}
),

dep_10 as (
    select * from {{ ref('stg_source_66') }}
),

dep_11 as (
    select * from {{ ref('stg_source_134') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_13 as (
    select * from {{ ref('stg_source_160') }}
),

dep_14 as (
    select * from {{ ref('stg_source_78') }}
),

dep_15 as (
    select * from {{ ref('stg_source_55') }}
),

dep_16 as (
    select * from {{ ref('stg_source_135') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_18 as (
    select * from {{ ref('stg_source_72') }}
),

dep_19 as (
    select * from {{ ref('stg_source_107') }}
),

dep_20 as (
    select * from {{ ref('stg_source_119') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_22 as (
    select * from {{ ref('stg_source_123') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_61') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_25 as (
    select * from {{ ref('stg_source_59') }}
),

dep_26 as (
    select * from {{ ref('stg_source_161') }}
),

dep_27 as (
    select * from {{ ref('stg_source_42') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_141') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_33 as (
    select * from {{ ref('stg_source_70') }}
),

dep_34 as (
    select * from {{ ref('stg_source_5') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_47') }}
),

dep_38 as (
    select * from {{ ref('stg_source_69') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_5') }}
),

dep_40 as (
    select * from {{ ref('stg_source_3') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_42 as (
    select * from {{ ref('stg_source_26') }}
),

dep_43 as (
    select * from {{ ref('stg_source_44') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_45 as (
    select * from {{ ref('stg_source_52') }}
),

dep_46 as (
    select * from {{ ref('stg_source_46') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_35') }}
),

dep_49 as (
    select * from {{ ref('stg_source_140') }}
),

dep_50 as (
    select * from {{ ref('stg_source_35') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_74_id']) }} as surrogate_id,
        dep_1.int_transformed_74_id as fct_business_event_165_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_74_id = dep_2.int_transformed_56_id
    left join dep_3 on dep_1.int_transformed_74_id = dep_3.stg_source_130_id
    left join dep_4 on dep_1.int_transformed_74_id = dep_4.stg_source_105_id
    left join dep_5 on dep_1.int_transformed_74_id = dep_5.int_transformed_112_id
    left join dep_6 on dep_1.int_transformed_74_id = dep_6.stg_source_51_id
    left join dep_7 on dep_1.int_transformed_74_id = dep_7.stg_source_49_id
    left join dep_8 on dep_1.int_transformed_74_id = dep_8.int_transformed_150_id
    left join dep_9 on dep_1.int_transformed_74_id = dep_9.int_transformed_155_id
    left join dep_10 on dep_1.int_transformed_74_id = dep_10.stg_source_66_id
    left join dep_11 on dep_1.int_transformed_74_id = dep_11.stg_source_134_id
    left join dep_12 on dep_1.int_transformed_74_id = dep_12.int_transformed_48_id
    left join dep_13 on dep_1.int_transformed_74_id = dep_13.stg_source_160_id
    left join dep_14 on dep_1.int_transformed_74_id = dep_14.stg_source_78_id
    left join dep_15 on dep_1.int_transformed_74_id = dep_15.stg_source_55_id
    left join dep_16 on dep_1.int_transformed_74_id = dep_16.stg_source_135_id
    left join dep_17 on dep_1.int_transformed_74_id = dep_17.int_transformed_60_id
    left join dep_18 on dep_1.int_transformed_74_id = dep_18.stg_source_72_id
    left join dep_19 on dep_1.int_transformed_74_id = dep_19.stg_source_107_id
    left join dep_20 on dep_1.int_transformed_74_id = dep_20.stg_source_119_id
    left join dep_21 on dep_1.int_transformed_74_id = dep_21.int_transformed_70_id
    left join dep_22 on dep_1.int_transformed_74_id = dep_22.stg_source_123_id
    left join dep_23 on dep_1.int_transformed_74_id = dep_23.int_transformed_61_id
    left join dep_24 on dep_1.int_transformed_74_id = dep_24.int_transformed_67_id
    left join dep_25 on dep_1.int_transformed_74_id = dep_25.stg_source_59_id
    left join dep_26 on dep_1.int_transformed_74_id = dep_26.stg_source_161_id
    left join dep_27 on dep_1.int_transformed_74_id = dep_27.stg_source_42_id
    left join dep_28 on dep_1.int_transformed_74_id = dep_28.int_transformed_84_id
    left join dep_29 on dep_1.int_transformed_74_id = dep_29.int_transformed_117_id
    left join dep_30 on dep_1.int_transformed_74_id = dep_30.int_transformed_131_id
    left join dep_31 on dep_1.int_transformed_74_id = dep_31.int_transformed_141_id
    left join dep_32 on dep_1.int_transformed_74_id = dep_32.int_transformed_97_id
    left join dep_33 on dep_1.int_transformed_74_id = dep_33.stg_source_70_id
    left join dep_34 on dep_1.int_transformed_74_id = dep_34.stg_source_5_id
    left join dep_35 on dep_1.int_transformed_74_id = dep_35.int_transformed_4_id
    left join dep_36 on dep_1.int_transformed_74_id = dep_36.int_transformed_124_id
    left join dep_37 on dep_1.int_transformed_74_id = dep_37.int_transformed_47_id
    left join dep_38 on dep_1.int_transformed_74_id = dep_38.stg_source_69_id
    left join dep_39 on dep_1.int_transformed_74_id = dep_39.int_transformed_5_id
    left join dep_40 on dep_1.int_transformed_74_id = dep_40.stg_source_3_id
    left join dep_41 on dep_1.int_transformed_74_id = dep_41.int_transformed_85_id
    left join dep_42 on dep_1.int_transformed_74_id = dep_42.stg_source_26_id
    left join dep_43 on dep_1.int_transformed_74_id = dep_43.stg_source_44_id
    left join dep_44 on dep_1.int_transformed_74_id = dep_44.int_transformed_154_id
    left join dep_45 on dep_1.int_transformed_74_id = dep_45.stg_source_52_id
    left join dep_46 on dep_1.int_transformed_74_id = dep_46.stg_source_46_id
    left join dep_47 on dep_1.int_transformed_74_id = dep_47.int_transformed_54_id
    left join dep_48 on dep_1.int_transformed_74_id = dep_48.int_transformed_35_id
    left join dep_49 on dep_1.int_transformed_74_id = dep_49.stg_source_140_id
    left join dep_50 on dep_1.int_transformed_74_id = dep_50.stg_source_35_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
