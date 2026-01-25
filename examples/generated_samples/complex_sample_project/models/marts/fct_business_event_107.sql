{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_137') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_100') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_17') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_43') }}
),

dep_6 as (
    select * from {{ ref('stg_source_144') }}
),

dep_7 as (
    select * from {{ ref('stg_source_124') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_9 as (
    select * from {{ ref('stg_source_146') }}
),

dep_10 as (
    select * from {{ ref('int_transformed_108') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_151') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_121') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_45') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_15 as (
    select * from {{ ref('stg_source_150') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_68') }}
),

dep_18 as (
    select * from {{ ref('stg_source_113') }}
),

dep_19 as (
    select * from {{ ref('stg_source_162') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_164') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_2') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_133') }}
),

dep_28 as (
    select * from {{ ref('stg_source_82') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_22') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_34 as (
    select * from {{ ref('stg_source_15') }}
),

dep_35 as (
    select * from {{ ref('stg_source_60') }}
),

dep_36 as (
    select * from {{ ref('stg_source_111') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_38 as (
    select * from {{ ref('stg_source_14') }}
),

dep_39 as (
    select * from {{ ref('stg_source_36') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_41 as (
    select * from {{ ref('stg_source_47') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_45 as (
    select * from {{ ref('stg_source_110') }}
),

dep_46 as (
    select * from {{ ref('stg_source_121') }}
),

dep_47 as (
    select * from {{ ref('stg_source_84') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_153') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_149') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_137_id']) }} as surrogate_id,
        dep_1.int_transformed_137_id as fct_business_event_107_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_137_id = dep_2.int_transformed_128_id
    left join dep_3 on dep_1.int_transformed_137_id = dep_3.int_transformed_100_id
    left join dep_4 on dep_1.int_transformed_137_id = dep_4.int_transformed_17_id
    left join dep_5 on dep_1.int_transformed_137_id = dep_5.int_transformed_43_id
    left join dep_6 on dep_1.int_transformed_137_id = dep_6.stg_source_144_id
    left join dep_7 on dep_1.int_transformed_137_id = dep_7.stg_source_124_id
    left join dep_8 on dep_1.int_transformed_137_id = dep_8.int_transformed_75_id
    left join dep_9 on dep_1.int_transformed_137_id = dep_9.stg_source_146_id
    left join dep_10 on dep_1.int_transformed_137_id = dep_10.int_transformed_108_id
    left join dep_11 on dep_1.int_transformed_137_id = dep_11.int_transformed_151_id
    left join dep_12 on dep_1.int_transformed_137_id = dep_12.int_transformed_121_id
    left join dep_13 on dep_1.int_transformed_137_id = dep_13.int_transformed_45_id
    left join dep_14 on dep_1.int_transformed_137_id = dep_14.int_transformed_127_id
    left join dep_15 on dep_1.int_transformed_137_id = dep_15.stg_source_150_id
    left join dep_16 on dep_1.int_transformed_137_id = dep_16.int_transformed_56_id
    left join dep_17 on dep_1.int_transformed_137_id = dep_17.int_transformed_68_id
    left join dep_18 on dep_1.int_transformed_137_id = dep_18.stg_source_113_id
    left join dep_19 on dep_1.int_transformed_137_id = dep_19.stg_source_162_id
    left join dep_20 on dep_1.int_transformed_137_id = dep_20.int_transformed_55_id
    left join dep_21 on dep_1.int_transformed_137_id = dep_21.int_transformed_138_id
    left join dep_22 on dep_1.int_transformed_137_id = dep_22.int_transformed_132_id
    left join dep_23 on dep_1.int_transformed_137_id = dep_23.int_transformed_164_id
    left join dep_24 on dep_1.int_transformed_137_id = dep_24.int_transformed_2_id
    left join dep_25 on dep_1.int_transformed_137_id = dep_25.int_transformed_154_id
    left join dep_26 on dep_1.int_transformed_137_id = dep_26.int_transformed_81_id
    left join dep_27 on dep_1.int_transformed_137_id = dep_27.int_transformed_133_id
    left join dep_28 on dep_1.int_transformed_137_id = dep_28.stg_source_82_id
    left join dep_29 on dep_1.int_transformed_137_id = dep_29.int_transformed_22_id
    left join dep_30 on dep_1.int_transformed_137_id = dep_30.int_transformed_93_id
    left join dep_31 on dep_1.int_transformed_137_id = dep_31.int_transformed_54_id
    left join dep_32 on dep_1.int_transformed_137_id = dep_32.int_transformed_52_id
    left join dep_33 on dep_1.int_transformed_137_id = dep_33.int_transformed_19_id
    left join dep_34 on dep_1.int_transformed_137_id = dep_34.stg_source_15_id
    left join dep_35 on dep_1.int_transformed_137_id = dep_35.stg_source_60_id
    left join dep_36 on dep_1.int_transformed_137_id = dep_36.stg_source_111_id
    left join dep_37 on dep_1.int_transformed_137_id = dep_37.int_transformed_143_id
    left join dep_38 on dep_1.int_transformed_137_id = dep_38.stg_source_14_id
    left join dep_39 on dep_1.int_transformed_137_id = dep_39.stg_source_36_id
    left join dep_40 on dep_1.int_transformed_137_id = dep_40.int_transformed_14_id
    left join dep_41 on dep_1.int_transformed_137_id = dep_41.stg_source_47_id
    left join dep_42 on dep_1.int_transformed_137_id = dep_42.int_transformed_156_id
    left join dep_43 on dep_1.int_transformed_137_id = dep_43.int_transformed_118_id
    left join dep_44 on dep_1.int_transformed_137_id = dep_44.int_transformed_76_id
    left join dep_45 on dep_1.int_transformed_137_id = dep_45.stg_source_110_id
    left join dep_46 on dep_1.int_transformed_137_id = dep_46.stg_source_121_id
    left join dep_47 on dep_1.int_transformed_137_id = dep_47.stg_source_84_id
    left join dep_48 on dep_1.int_transformed_137_id = dep_48.int_transformed_110_id
    left join dep_49 on dep_1.int_transformed_137_id = dep_49.int_transformed_153_id
    left join dep_50 on dep_1.int_transformed_137_id = dep_50.int_transformed_149_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
