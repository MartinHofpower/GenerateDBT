{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_37') }}
),

dep_2 as (
    select * from {{ ref('stg_source_6') }}
),

dep_3 as (
    select * from {{ ref('stg_source_46') }}
),

dep_4 as (
    select * from {{ ref('stg_source_31') }}
),

dep_5 as (
    select * from {{ ref('stg_source_39') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_34') }}
),

dep_7 as (
    select * from {{ ref('stg_source_50') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_9 as (
    select * from {{ ref('stg_source_137') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_30') }}
),

dep_12 as (
    select * from {{ ref('stg_source_35') }}
),

dep_13 as (
    select * from {{ ref('stg_source_28') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_89') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_17 as (
    select * from {{ ref('stg_source_21') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_87') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_20 as (
    select * from {{ ref('stg_source_110') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_120') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_23 as (
    select * from {{ ref('stg_source_108') }}
),

dep_24 as (
    select * from {{ ref('stg_source_82') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_103') }}
),

dep_26 as (
    select * from {{ ref('stg_source_105') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_59') }}
),

dep_28 as (
    select * from {{ ref('stg_source_76') }}
),

dep_29 as (
    select * from {{ ref('stg_source_34') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_31 as (
    select * from {{ ref('stg_source_91') }}
),

dep_32 as (
    select * from {{ ref('stg_source_18') }}
),

dep_33 as (
    select * from {{ ref('stg_source_133') }}
),

dep_34 as (
    select * from {{ ref('stg_source_154') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_36 as (
    select * from {{ ref('stg_source_64') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_38 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_39 as (
    select * from {{ ref('stg_source_145') }}
),

dep_40 as (
    select * from {{ ref('stg_source_36') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_62') }}
),

dep_44 as (
    select * from {{ ref('stg_source_86') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_46 as (
    select * from {{ ref('int_transformed_58') }}
),

dep_47 as (
    select * from {{ ref('stg_source_54') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_50 as (
    select * from {{ ref('stg_source_26') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_37_id']) }} as surrogate_id,
        dep_1.stg_source_37_id as fct_business_event_123_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_37_id = dep_2.stg_source_6_id
    left join dep_3 on dep_1.stg_source_37_id = dep_3.stg_source_46_id
    left join dep_4 on dep_1.stg_source_37_id = dep_4.stg_source_31_id
    left join dep_5 on dep_1.stg_source_37_id = dep_5.stg_source_39_id
    left join dep_6 on dep_1.stg_source_37_id = dep_6.int_transformed_34_id
    left join dep_7 on dep_1.stg_source_37_id = dep_7.stg_source_50_id
    left join dep_8 on dep_1.stg_source_37_id = dep_8.int_transformed_128_id
    left join dep_9 on dep_1.stg_source_37_id = dep_9.stg_source_137_id
    left join dep_10 on dep_1.stg_source_37_id = dep_10.int_transformed_131_id
    left join dep_11 on dep_1.stg_source_37_id = dep_11.int_transformed_30_id
    left join dep_12 on dep_1.stg_source_37_id = dep_12.stg_source_35_id
    left join dep_13 on dep_1.stg_source_37_id = dep_13.stg_source_28_id
    left join dep_14 on dep_1.stg_source_37_id = dep_14.int_transformed_89_id
    left join dep_15 on dep_1.stg_source_37_id = dep_15.int_transformed_161_id
    left join dep_16 on dep_1.stg_source_37_id = dep_16.int_transformed_107_id
    left join dep_17 on dep_1.stg_source_37_id = dep_17.stg_source_21_id
    left join dep_18 on dep_1.stg_source_37_id = dep_18.int_transformed_87_id
    left join dep_19 on dep_1.stg_source_37_id = dep_19.int_transformed_1_id
    left join dep_20 on dep_1.stg_source_37_id = dep_20.stg_source_110_id
    left join dep_21 on dep_1.stg_source_37_id = dep_21.int_transformed_120_id
    left join dep_22 on dep_1.stg_source_37_id = dep_22.int_transformed_70_id
    left join dep_23 on dep_1.stg_source_37_id = dep_23.stg_source_108_id
    left join dep_24 on dep_1.stg_source_37_id = dep_24.stg_source_82_id
    left join dep_25 on dep_1.stg_source_37_id = dep_25.int_transformed_103_id
    left join dep_26 on dep_1.stg_source_37_id = dep_26.stg_source_105_id
    left join dep_27 on dep_1.stg_source_37_id = dep_27.int_transformed_59_id
    left join dep_28 on dep_1.stg_source_37_id = dep_28.stg_source_76_id
    left join dep_29 on dep_1.stg_source_37_id = dep_29.stg_source_34_id
    left join dep_30 on dep_1.stg_source_37_id = dep_30.int_transformed_137_id
    left join dep_31 on dep_1.stg_source_37_id = dep_31.stg_source_91_id
    left join dep_32 on dep_1.stg_source_37_id = dep_32.stg_source_18_id
    left join dep_33 on dep_1.stg_source_37_id = dep_33.stg_source_133_id
    left join dep_34 on dep_1.stg_source_37_id = dep_34.stg_source_154_id
    left join dep_35 on dep_1.stg_source_37_id = dep_35.int_transformed_114_id
    left join dep_36 on dep_1.stg_source_37_id = dep_36.stg_source_64_id
    left join dep_37 on dep_1.stg_source_37_id = dep_37.int_transformed_55_id
    left join dep_38 on dep_1.stg_source_37_id = dep_38.int_transformed_136_id
    left join dep_39 on dep_1.stg_source_37_id = dep_39.stg_source_145_id
    left join dep_40 on dep_1.stg_source_37_id = dep_40.stg_source_36_id
    left join dep_41 on dep_1.stg_source_37_id = dep_41.int_transformed_159_id
    left join dep_42 on dep_1.stg_source_37_id = dep_42.int_transformed_144_id
    left join dep_43 on dep_1.stg_source_37_id = dep_43.int_transformed_62_id
    left join dep_44 on dep_1.stg_source_37_id = dep_44.stg_source_86_id
    left join dep_45 on dep_1.stg_source_37_id = dep_45.int_transformed_56_id
    left join dep_46 on dep_1.stg_source_37_id = dep_46.int_transformed_58_id
    left join dep_47 on dep_1.stg_source_37_id = dep_47.stg_source_54_id
    left join dep_48 on dep_1.stg_source_37_id = dep_48.int_transformed_17_id
    left join dep_49 on dep_1.stg_source_37_id = dep_49.int_transformed_88_id
    left join dep_50 on dep_1.stg_source_37_id = dep_50.stg_source_26_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
