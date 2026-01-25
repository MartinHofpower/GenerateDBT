{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_161') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_80') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_84') }}
),

dep_7 as (
    select * from {{ ref('stg_source_14') }}
),

dep_8 as (
    select * from {{ ref('stg_source_145') }}
),

dep_9 as (
    select * from {{ ref('stg_source_60') }}
),

dep_10 as (
    select * from {{ ref('stg_source_56') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_46') }}
),

dep_12 as (
    select * from {{ ref('stg_source_166') }}
),

dep_13 as (
    select * from {{ ref('stg_source_79') }}
),

dep_14 as (
    select * from {{ ref('stg_source_107') }}
),

dep_15 as (
    select * from {{ ref('stg_source_123') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_81') }}
),

dep_17 as (
    select * from {{ ref('stg_source_104') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_90') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_8') }}
),

dep_24 as (
    select * from {{ ref('stg_source_64') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_85') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_98') }}
),

dep_27 as (
    select * from {{ ref('stg_source_90') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_162') }}
),

dep_29 as (
    select * from {{ ref('stg_source_34') }}
),

dep_30 as (
    select * from {{ ref('stg_source_105') }}
),

dep_31 as (
    select * from {{ ref('stg_source_154') }}
),

dep_32 as (
    select * from {{ ref('stg_source_52') }}
),

dep_33 as (
    select * from {{ ref('stg_source_156') }}
),

dep_34 as (
    select * from {{ ref('stg_source_151') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_36 as (
    select * from {{ ref('stg_source_54') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_123') }}
),

dep_38 as (
    select * from {{ ref('stg_source_58') }}
),

dep_39 as (
    select * from {{ ref('stg_source_129') }}
),

dep_40 as (
    select * from {{ ref('stg_source_7') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_71') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_163') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_28') }}
),

dep_45 as (
    select * from {{ ref('stg_source_146') }}
),

dep_46 as (
    select * from {{ ref('stg_source_126') }}
),

dep_47 as (
    select * from {{ ref('int_transformed_127') }}
),

dep_48 as (
    select * from {{ ref('stg_source_164') }}
),

dep_49 as (
    select * from {{ ref('stg_source_20') }}
),

dep_50 as (
    select * from {{ ref('stg_source_82') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.int_transformed_94_id']) }} as surrogate_id,
        dep_1.int_transformed_94_id as dim_entity_128_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.int_transformed_94_id = dep_2.int_transformed_161_id
    left join dep_3 on dep_1.int_transformed_94_id = dep_3.int_transformed_93_id
    left join dep_4 on dep_1.int_transformed_94_id = dep_4.int_transformed_80_id
    left join dep_5 on dep_1.int_transformed_94_id = dep_5.int_transformed_1_id
    left join dep_6 on dep_1.int_transformed_94_id = dep_6.int_transformed_84_id
    left join dep_7 on dep_1.int_transformed_94_id = dep_7.stg_source_14_id
    left join dep_8 on dep_1.int_transformed_94_id = dep_8.stg_source_145_id
    left join dep_9 on dep_1.int_transformed_94_id = dep_9.stg_source_60_id
    left join dep_10 on dep_1.int_transformed_94_id = dep_10.stg_source_56_id
    left join dep_11 on dep_1.int_transformed_94_id = dep_11.int_transformed_46_id
    left join dep_12 on dep_1.int_transformed_94_id = dep_12.stg_source_166_id
    left join dep_13 on dep_1.int_transformed_94_id = dep_13.stg_source_79_id
    left join dep_14 on dep_1.int_transformed_94_id = dep_14.stg_source_107_id
    left join dep_15 on dep_1.int_transformed_94_id = dep_15.stg_source_123_id
    left join dep_16 on dep_1.int_transformed_94_id = dep_16.int_transformed_81_id
    left join dep_17 on dep_1.int_transformed_94_id = dep_17.stg_source_104_id
    left join dep_18 on dep_1.int_transformed_94_id = dep_18.int_transformed_73_id
    left join dep_19 on dep_1.int_transformed_94_id = dep_19.int_transformed_118_id
    left join dep_20 on dep_1.int_transformed_94_id = dep_20.int_transformed_60_id
    left join dep_21 on dep_1.int_transformed_94_id = dep_21.int_transformed_90_id
    left join dep_22 on dep_1.int_transformed_94_id = dep_22.int_transformed_13_id
    left join dep_23 on dep_1.int_transformed_94_id = dep_23.int_transformed_8_id
    left join dep_24 on dep_1.int_transformed_94_id = dep_24.stg_source_64_id
    left join dep_25 on dep_1.int_transformed_94_id = dep_25.int_transformed_85_id
    left join dep_26 on dep_1.int_transformed_94_id = dep_26.int_transformed_98_id
    left join dep_27 on dep_1.int_transformed_94_id = dep_27.stg_source_90_id
    left join dep_28 on dep_1.int_transformed_94_id = dep_28.int_transformed_162_id
    left join dep_29 on dep_1.int_transformed_94_id = dep_29.stg_source_34_id
    left join dep_30 on dep_1.int_transformed_94_id = dep_30.stg_source_105_id
    left join dep_31 on dep_1.int_transformed_94_id = dep_31.stg_source_154_id
    left join dep_32 on dep_1.int_transformed_94_id = dep_32.stg_source_52_id
    left join dep_33 on dep_1.int_transformed_94_id = dep_33.stg_source_156_id
    left join dep_34 on dep_1.int_transformed_94_id = dep_34.stg_source_151_id
    left join dep_35 on dep_1.int_transformed_94_id = dep_35.int_transformed_156_id
    left join dep_36 on dep_1.int_transformed_94_id = dep_36.stg_source_54_id
    left join dep_37 on dep_1.int_transformed_94_id = dep_37.int_transformed_123_id
    left join dep_38 on dep_1.int_transformed_94_id = dep_38.stg_source_58_id
    left join dep_39 on dep_1.int_transformed_94_id = dep_39.stg_source_129_id
    left join dep_40 on dep_1.int_transformed_94_id = dep_40.stg_source_7_id
    left join dep_41 on dep_1.int_transformed_94_id = dep_41.int_transformed_71_id
    left join dep_42 on dep_1.int_transformed_94_id = dep_42.int_transformed_163_id
    left join dep_43 on dep_1.int_transformed_94_id = dep_43.int_transformed_148_id
    left join dep_44 on dep_1.int_transformed_94_id = dep_44.int_transformed_28_id
    left join dep_45 on dep_1.int_transformed_94_id = dep_45.stg_source_146_id
    left join dep_46 on dep_1.int_transformed_94_id = dep_46.stg_source_126_id
    left join dep_47 on dep_1.int_transformed_94_id = dep_47.int_transformed_127_id
    left join dep_48 on dep_1.int_transformed_94_id = dep_48.stg_source_164_id
    left join dep_49 on dep_1.int_transformed_94_id = dep_49.stg_source_20_id
    left join dep_50 on dep_1.int_transformed_94_id = dep_50.stg_source_82_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
