{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_100') }}
),

dep_2 as (
    select * from {{ ref('stg_source_149') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_5 as (
    select * from {{ ref('stg_source_141') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_7 as (
    select * from {{ ref('stg_source_8') }}
),

dep_8 as (
    select * from {{ ref('stg_source_14') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_113') }}
),

dep_10 as (
    select * from {{ ref('stg_source_109') }}
),

dep_11 as (
    select * from {{ ref('stg_source_143') }}
),

dep_12 as (
    select * from {{ ref('stg_source_147') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_23') }}
),

dep_14 as (
    select * from {{ ref('stg_source_104') }}
),

dep_15 as (
    select * from {{ ref('stg_source_139') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_106') }}
),

dep_17 as (
    select * from {{ ref('stg_source_54') }}
),

dep_18 as (
    select * from {{ ref('stg_source_152') }}
),

dep_19 as (
    select * from {{ ref('stg_source_137') }}
),

dep_20 as (
    select * from {{ ref('stg_source_71') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_138') }}
),

dep_22 as (
    select * from {{ ref('stg_source_89') }}
),

dep_23 as (
    select * from {{ ref('stg_source_12') }}
),

dep_24 as (
    select * from {{ ref('stg_source_60') }}
),

dep_25 as (
    select * from {{ ref('stg_source_165') }}
),

dep_26 as (
    select * from {{ ref('stg_source_127') }}
),

dep_27 as (
    select * from {{ ref('stg_source_122') }}
),

dep_28 as (
    select * from {{ ref('stg_source_50') }}
),

dep_29 as (
    select * from {{ ref('stg_source_116') }}
),

dep_30 as (
    select * from {{ ref('stg_source_153') }}
),

dep_31 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_32 as (
    select * from {{ ref('stg_source_103') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_34 as (
    select * from {{ ref('stg_source_5') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_149') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_9') }}
),

dep_37 as (
    select * from {{ ref('stg_source_138') }}
),

dep_38 as (
    select * from {{ ref('stg_source_81') }}
),

dep_39 as (
    select * from {{ ref('stg_source_83') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_41 as (
    select * from {{ ref('stg_source_148') }}
),

dep_42 as (
    select * from {{ ref('stg_source_120') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_45 as (
    select * from {{ ref('stg_source_107') }}
),

dep_46 as (
    select * from {{ ref('stg_source_92') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_4') }}
),

dep_48 as (
    select * from {{ ref('stg_source_131') }}
),

dep_49 as (
    select * from {{ ref('stg_source_21') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_132') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_100_id']) }} as surrogate_id,
        dep_1.stg_source_100_id as fct_business_event_67_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_100_id = dep_2.stg_source_149_id
    left join dep_3 on dep_1.stg_source_100_id = dep_3.int_transformed_162_id
    left join dep_4 on dep_1.stg_source_100_id = dep_4.int_transformed_13_id
    left join dep_5 on dep_1.stg_source_100_id = dep_5.stg_source_141_id
    left join dep_6 on dep_1.stg_source_100_id = dep_6.int_transformed_65_id
    left join dep_7 on dep_1.stg_source_100_id = dep_7.stg_source_8_id
    left join dep_8 on dep_1.stg_source_100_id = dep_8.stg_source_14_id
    left join dep_9 on dep_1.stg_source_100_id = dep_9.int_transformed_113_id
    left join dep_10 on dep_1.stg_source_100_id = dep_10.stg_source_109_id
    left join dep_11 on dep_1.stg_source_100_id = dep_11.stg_source_143_id
    left join dep_12 on dep_1.stg_source_100_id = dep_12.stg_source_147_id
    left join dep_13 on dep_1.stg_source_100_id = dep_13.int_transformed_23_id
    left join dep_14 on dep_1.stg_source_100_id = dep_14.stg_source_104_id
    left join dep_15 on dep_1.stg_source_100_id = dep_15.stg_source_139_id
    left join dep_16 on dep_1.stg_source_100_id = dep_16.int_transformed_106_id
    left join dep_17 on dep_1.stg_source_100_id = dep_17.stg_source_54_id
    left join dep_18 on dep_1.stg_source_100_id = dep_18.stg_source_152_id
    left join dep_19 on dep_1.stg_source_100_id = dep_19.stg_source_137_id
    left join dep_20 on dep_1.stg_source_100_id = dep_20.stg_source_71_id
    left join dep_21 on dep_1.stg_source_100_id = dep_21.int_transformed_138_id
    left join dep_22 on dep_1.stg_source_100_id = dep_22.stg_source_89_id
    left join dep_23 on dep_1.stg_source_100_id = dep_23.stg_source_12_id
    left join dep_24 on dep_1.stg_source_100_id = dep_24.stg_source_60_id
    left join dep_25 on dep_1.stg_source_100_id = dep_25.stg_source_165_id
    left join dep_26 on dep_1.stg_source_100_id = dep_26.stg_source_127_id
    left join dep_27 on dep_1.stg_source_100_id = dep_27.stg_source_122_id
    left join dep_28 on dep_1.stg_source_100_id = dep_28.stg_source_50_id
    left join dep_29 on dep_1.stg_source_100_id = dep_29.stg_source_116_id
    left join dep_30 on dep_1.stg_source_100_id = dep_30.stg_source_153_id
    left join dep_31 on dep_1.stg_source_100_id = dep_31.int_transformed_73_id
    left join dep_32 on dep_1.stg_source_100_id = dep_32.stg_source_103_id
    left join dep_33 on dep_1.stg_source_100_id = dep_33.int_transformed_29_id
    left join dep_34 on dep_1.stg_source_100_id = dep_34.stg_source_5_id
    left join dep_35 on dep_1.stg_source_100_id = dep_35.int_transformed_149_id
    left join dep_36 on dep_1.stg_source_100_id = dep_36.int_transformed_9_id
    left join dep_37 on dep_1.stg_source_100_id = dep_37.stg_source_138_id
    left join dep_38 on dep_1.stg_source_100_id = dep_38.stg_source_81_id
    left join dep_39 on dep_1.stg_source_100_id = dep_39.stg_source_83_id
    left join dep_40 on dep_1.stg_source_100_id = dep_40.int_transformed_159_id
    left join dep_41 on dep_1.stg_source_100_id = dep_41.stg_source_148_id
    left join dep_42 on dep_1.stg_source_100_id = dep_42.stg_source_120_id
    left join dep_43 on dep_1.stg_source_100_id = dep_43.int_transformed_126_id
    left join dep_44 on dep_1.stg_source_100_id = dep_44.int_transformed_166_id
    left join dep_45 on dep_1.stg_source_100_id = dep_45.stg_source_107_id
    left join dep_46 on dep_1.stg_source_100_id = dep_46.stg_source_92_id
    left join dep_47 on dep_1.stg_source_100_id = dep_47.int_transformed_4_id
    left join dep_48 on dep_1.stg_source_100_id = dep_48.stg_source_131_id
    left join dep_49 on dep_1.stg_source_100_id = dep_49.stg_source_21_id
    left join dep_50 on dep_1.stg_source_100_id = dep_50.int_transformed_132_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
