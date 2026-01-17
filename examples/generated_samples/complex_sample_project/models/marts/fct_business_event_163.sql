{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_136') }}
),

dep_2 as (
    select * from {{ ref('stg_source_148') }}
),

dep_3 as (
    select * from {{ ref('stg_source_122') }}
),

dep_4 as (
    select * from {{ ref('int_transformed_159') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_52') }}
),

dep_6 as (
    select * from {{ ref('int_transformed_131') }}
),

dep_7 as (
    select * from {{ ref('stg_source_164') }}
),

dep_8 as (
    select * from {{ ref('stg_source_163') }}
),

dep_9 as (
    select * from {{ ref('stg_source_55') }}
),

dep_10 as (
    select * from {{ ref('stg_source_33') }}
),

dep_11 as (
    select * from {{ ref('stg_source_116') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_20') }}
),

dep_13 as (
    select * from {{ ref('stg_source_38') }}
),

dep_14 as (
    select * from {{ ref('stg_source_50') }}
),

dep_15 as (
    select * from {{ ref('stg_source_109') }}
),

dep_16 as (
    select * from {{ ref('stg_source_25') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_18 as (
    select * from {{ ref('stg_source_142') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_6') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_119') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_99') }}
),

dep_22 as (
    select * from {{ ref('stg_source_49') }}
),

dep_23 as (
    select * from {{ ref('stg_source_110') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_70') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_132') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_31 as (
    select * from {{ ref('stg_source_121') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_33 as (
    select * from {{ ref('stg_source_127') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_166') }}
),

dep_35 as (
    select * from {{ ref('stg_source_165') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_37 as (
    select * from {{ ref('stg_source_143') }}
),

dep_38 as (
    select * from {{ ref('stg_source_71') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_39') }}
),

dep_40 as (
    select * from {{ ref('stg_source_43') }}
),

dep_41 as (
    select * from {{ ref('stg_source_7') }}
),

dep_42 as (
    select * from {{ ref('stg_source_30') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_15') }}
),

dep_44 as (
    select * from {{ ref('int_transformed_124') }}
),

dep_45 as (
    select * from {{ ref('stg_source_63') }}
),

dep_46 as (
    select * from {{ ref('stg_source_40') }}
),

dep_47 as (
    select * from {{ ref('stg_source_67') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_50 as (
    select * from {{ ref('int_transformed_66') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_136_id']) }} as surrogate_id,
        dep_1.stg_source_136_id as fct_business_event_163_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_136_id = dep_2.stg_source_148_id
    left join dep_3 on dep_1.stg_source_136_id = dep_3.stg_source_122_id
    left join dep_4 on dep_1.stg_source_136_id = dep_4.int_transformed_159_id
    left join dep_5 on dep_1.stg_source_136_id = dep_5.int_transformed_52_id
    left join dep_6 on dep_1.stg_source_136_id = dep_6.int_transformed_131_id
    left join dep_7 on dep_1.stg_source_136_id = dep_7.stg_source_164_id
    left join dep_8 on dep_1.stg_source_136_id = dep_8.stg_source_163_id
    left join dep_9 on dep_1.stg_source_136_id = dep_9.stg_source_55_id
    left join dep_10 on dep_1.stg_source_136_id = dep_10.stg_source_33_id
    left join dep_11 on dep_1.stg_source_136_id = dep_11.stg_source_116_id
    left join dep_12 on dep_1.stg_source_136_id = dep_12.int_transformed_20_id
    left join dep_13 on dep_1.stg_source_136_id = dep_13.stg_source_38_id
    left join dep_14 on dep_1.stg_source_136_id = dep_14.stg_source_50_id
    left join dep_15 on dep_1.stg_source_136_id = dep_15.stg_source_109_id
    left join dep_16 on dep_1.stg_source_136_id = dep_16.stg_source_25_id
    left join dep_17 on dep_1.stg_source_136_id = dep_17.int_transformed_42_id
    left join dep_18 on dep_1.stg_source_136_id = dep_18.stg_source_142_id
    left join dep_19 on dep_1.stg_source_136_id = dep_19.int_transformed_6_id
    left join dep_20 on dep_1.stg_source_136_id = dep_20.int_transformed_119_id
    left join dep_21 on dep_1.stg_source_136_id = dep_21.int_transformed_99_id
    left join dep_22 on dep_1.stg_source_136_id = dep_22.stg_source_49_id
    left join dep_23 on dep_1.stg_source_136_id = dep_23.stg_source_110_id
    left join dep_24 on dep_1.stg_source_136_id = dep_24.int_transformed_73_id
    left join dep_25 on dep_1.stg_source_136_id = dep_25.int_transformed_37_id
    left join dep_26 on dep_1.stg_source_136_id = dep_26.int_transformed_70_id
    left join dep_27 on dep_1.stg_source_136_id = dep_27.int_transformed_75_id
    left join dep_28 on dep_1.stg_source_136_id = dep_28.int_transformed_136_id
    left join dep_29 on dep_1.stg_source_136_id = dep_29.int_transformed_132_id
    left join dep_30 on dep_1.stg_source_136_id = dep_30.int_transformed_143_id
    left join dep_31 on dep_1.stg_source_136_id = dep_31.stg_source_121_id
    left join dep_32 on dep_1.stg_source_136_id = dep_32.int_transformed_126_id
    left join dep_33 on dep_1.stg_source_136_id = dep_33.stg_source_127_id
    left join dep_34 on dep_1.stg_source_136_id = dep_34.int_transformed_166_id
    left join dep_35 on dep_1.stg_source_136_id = dep_35.stg_source_165_id
    left join dep_36 on dep_1.stg_source_136_id = dep_36.int_transformed_101_id
    left join dep_37 on dep_1.stg_source_136_id = dep_37.stg_source_143_id
    left join dep_38 on dep_1.stg_source_136_id = dep_38.stg_source_71_id
    left join dep_39 on dep_1.stg_source_136_id = dep_39.int_transformed_39_id
    left join dep_40 on dep_1.stg_source_136_id = dep_40.stg_source_43_id
    left join dep_41 on dep_1.stg_source_136_id = dep_41.stg_source_7_id
    left join dep_42 on dep_1.stg_source_136_id = dep_42.stg_source_30_id
    left join dep_43 on dep_1.stg_source_136_id = dep_43.int_transformed_15_id
    left join dep_44 on dep_1.stg_source_136_id = dep_44.int_transformed_124_id
    left join dep_45 on dep_1.stg_source_136_id = dep_45.stg_source_63_id
    left join dep_46 on dep_1.stg_source_136_id = dep_46.stg_source_40_id
    left join dep_47 on dep_1.stg_source_136_id = dep_47.stg_source_67_id
    left join dep_48 on dep_1.stg_source_136_id = dep_48.int_transformed_144_id
    left join dep_49 on dep_1.stg_source_136_id = dep_49.int_transformed_110_id
    left join dep_50 on dep_1.stg_source_136_id = dep_50.int_transformed_66_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
