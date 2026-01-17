{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'fact', 'incremental']
    )
}}

-- Complex fact table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_42') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_65') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_12') }}
),

dep_4 as (
    select * from {{ ref('stg_source_165') }}
),

dep_5 as (
    select * from {{ ref('stg_source_55') }}
),

dep_6 as (
    select * from {{ ref('stg_source_159') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_97') }}
),

dep_8 as (
    select * from {{ ref('int_transformed_44') }}
),

dep_9 as (
    select * from {{ ref('stg_source_80') }}
),

dep_10 as (
    select * from {{ ref('stg_source_119') }}
),

dep_11 as (
    select * from {{ ref('stg_source_115') }}
),

dep_12 as (
    select * from {{ ref('stg_source_43') }}
),

dep_13 as (
    select * from {{ ref('int_transformed_118') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_19') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_16 as (
    select * from {{ ref('int_transformed_57') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_18 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_19 as (
    select * from {{ ref('stg_source_141') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_110') }}
),

dep_21 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_76') }}
),

dep_23 as (
    select * from {{ ref('stg_source_145') }}
),

dep_24 as (
    select * from {{ ref('int_transformed_154') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_1') }}
),

dep_27 as (
    select * from {{ ref('stg_source_39') }}
),

dep_28 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_94') }}
),

dep_30 as (
    select * from {{ ref('int_transformed_156') }}
),

dep_31 as (
    select * from {{ ref('stg_source_50') }}
),

dep_32 as (
    select * from {{ ref('stg_source_27') }}
),

dep_33 as (
    select * from {{ ref('stg_source_93') }}
),

dep_34 as (
    select * from {{ ref('stg_source_9') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_93') }}
),

dep_36 as (
    select * from {{ ref('stg_source_112') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_128') }}
),

dep_38 as (
    select * from {{ ref('stg_source_41') }}
),

dep_39 as (
    select * from {{ ref('stg_source_164') }}
),

dep_40 as (
    select * from {{ ref('stg_source_1') }}
),

dep_41 as (
    select * from {{ ref('int_transformed_114') }}
),

dep_42 as (
    select * from {{ ref('stg_source_31') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_44 as (
    select * from {{ ref('stg_source_97') }}
),

dep_45 as (
    select * from {{ ref('stg_source_116') }}
),

dep_46 as (
    select * from {{ ref('stg_source_10') }}
),

dep_47 as (
    select * from {{ ref('stg_source_28') }}
),

dep_48 as (
    select * from {{ ref('int_transformed_13') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_55') }}
),

dep_50 as (
    select * from {{ ref('stg_source_126') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_42_id']) }} as surrogate_id,
        dep_1.stg_source_42_id as fct_business_event_71_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_42_id = dep_2.int_transformed_65_id
    left join dep_3 on dep_1.stg_source_42_id = dep_3.int_transformed_12_id
    left join dep_4 on dep_1.stg_source_42_id = dep_4.stg_source_165_id
    left join dep_5 on dep_1.stg_source_42_id = dep_5.stg_source_55_id
    left join dep_6 on dep_1.stg_source_42_id = dep_6.stg_source_159_id
    left join dep_7 on dep_1.stg_source_42_id = dep_7.int_transformed_97_id
    left join dep_8 on dep_1.stg_source_42_id = dep_8.int_transformed_44_id
    left join dep_9 on dep_1.stg_source_42_id = dep_9.stg_source_80_id
    left join dep_10 on dep_1.stg_source_42_id = dep_10.stg_source_119_id
    left join dep_11 on dep_1.stg_source_42_id = dep_11.stg_source_115_id
    left join dep_12 on dep_1.stg_source_42_id = dep_12.stg_source_43_id
    left join dep_13 on dep_1.stg_source_42_id = dep_13.int_transformed_118_id
    left join dep_14 on dep_1.stg_source_42_id = dep_14.int_transformed_19_id
    left join dep_15 on dep_1.stg_source_42_id = dep_15.int_transformed_67_id
    left join dep_16 on dep_1.stg_source_42_id = dep_16.int_transformed_57_id
    left join dep_17 on dep_1.stg_source_42_id = dep_17.int_transformed_101_id
    left join dep_18 on dep_1.stg_source_42_id = dep_18.int_transformed_73_id
    left join dep_19 on dep_1.stg_source_42_id = dep_19.stg_source_141_id
    left join dep_20 on dep_1.stg_source_42_id = dep_20.int_transformed_110_id
    left join dep_21 on dep_1.stg_source_42_id = dep_21.int_transformed_143_id
    left join dep_22 on dep_1.stg_source_42_id = dep_22.int_transformed_76_id
    left join dep_23 on dep_1.stg_source_42_id = dep_23.stg_source_145_id
    left join dep_24 on dep_1.stg_source_42_id = dep_24.int_transformed_154_id
    left join dep_25 on dep_1.stg_source_42_id = dep_25.int_transformed_37_id
    left join dep_26 on dep_1.stg_source_42_id = dep_26.int_transformed_1_id
    left join dep_27 on dep_1.stg_source_42_id = dep_27.stg_source_39_id
    left join dep_28 on dep_1.stg_source_42_id = dep_28.int_transformed_144_id
    left join dep_29 on dep_1.stg_source_42_id = dep_29.int_transformed_94_id
    left join dep_30 on dep_1.stg_source_42_id = dep_30.int_transformed_156_id
    left join dep_31 on dep_1.stg_source_42_id = dep_31.stg_source_50_id
    left join dep_32 on dep_1.stg_source_42_id = dep_32.stg_source_27_id
    left join dep_33 on dep_1.stg_source_42_id = dep_33.stg_source_93_id
    left join dep_34 on dep_1.stg_source_42_id = dep_34.stg_source_9_id
    left join dep_35 on dep_1.stg_source_42_id = dep_35.int_transformed_93_id
    left join dep_36 on dep_1.stg_source_42_id = dep_36.stg_source_112_id
    left join dep_37 on dep_1.stg_source_42_id = dep_37.int_transformed_128_id
    left join dep_38 on dep_1.stg_source_42_id = dep_38.stg_source_41_id
    left join dep_39 on dep_1.stg_source_42_id = dep_39.stg_source_164_id
    left join dep_40 on dep_1.stg_source_42_id = dep_40.stg_source_1_id
    left join dep_41 on dep_1.stg_source_42_id = dep_41.int_transformed_114_id
    left join dep_42 on dep_1.stg_source_42_id = dep_42.stg_source_31_id
    left join dep_43 on dep_1.stg_source_42_id = dep_43.int_transformed_107_id
    left join dep_44 on dep_1.stg_source_42_id = dep_44.stg_source_97_id
    left join dep_45 on dep_1.stg_source_42_id = dep_45.stg_source_116_id
    left join dep_46 on dep_1.stg_source_42_id = dep_46.stg_source_10_id
    left join dep_47 on dep_1.stg_source_42_id = dep_47.stg_source_28_id
    left join dep_48 on dep_1.stg_source_42_id = dep_48.int_transformed_13_id
    left join dep_49 on dep_1.stg_source_42_id = dep_49.int_transformed_55_id
    left join dep_50 on dep_1.stg_source_42_id = dep_50.stg_source_126_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
