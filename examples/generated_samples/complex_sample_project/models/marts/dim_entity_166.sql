{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_37') }}
),

dep_2 as (
    select * from {{ ref('int_transformed_101') }}
),

dep_3 as (
    select * from {{ ref('stg_source_148') }}
),

dep_4 as (
    select * from {{ ref('stg_source_53') }}
),

dep_5 as (
    select * from {{ ref('int_transformed_144') }}
),

dep_6 as (
    select * from {{ ref('stg_source_109') }}
),

dep_7 as (
    select * from {{ ref('int_transformed_136') }}
),

dep_8 as (
    select * from {{ ref('stg_source_56') }}
),

dep_9 as (
    select * from {{ ref('stg_source_140') }}
),

dep_10 as (
    select * from {{ ref('stg_source_100') }}
),

dep_11 as (
    select * from {{ ref('stg_source_45') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_13 as (
    select * from {{ ref('stg_source_82') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_37') }}
),

dep_15 as (
    select * from {{ ref('int_transformed_14') }}
),

dep_16 as (
    select * from {{ ref('stg_source_43') }}
),

dep_17 as (
    select * from {{ ref('stg_source_121') }}
),

dep_18 as (
    select * from {{ ref('stg_source_49') }}
),

dep_19 as (
    select * from {{ ref('int_transformed_53') }}
),

dep_20 as (
    select * from {{ ref('stg_source_155') }}
),

dep_21 as (
    select * from {{ ref('stg_source_143') }}
),

dep_22 as (
    select * from {{ ref('int_transformed_129') }}
),

dep_23 as (
    select * from {{ ref('int_transformed_51') }}
),

dep_24 as (
    select * from {{ ref('stg_source_81') }}
),

dep_25 as (
    select * from {{ ref('stg_source_33') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_67') }}
),

dep_27 as (
    select * from {{ ref('stg_source_124') }}
),

dep_28 as (
    select * from {{ ref('stg_source_92') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_30 as (
    select * from {{ ref('stg_source_9') }}
),

dep_31 as (
    select * from {{ ref('stg_source_1') }}
),

dep_32 as (
    select * from {{ ref('stg_source_57') }}
),

dep_33 as (
    select * from {{ ref('stg_source_59') }}
),

dep_34 as (
    select * from {{ ref('int_transformed_73') }}
),

dep_35 as (
    select * from {{ ref('stg_source_38') }}
),

dep_36 as (
    select * from {{ ref('int_transformed_111') }}
),

dep_37 as (
    select * from {{ ref('stg_source_161') }}
),

dep_38 as (
    select * from {{ ref('stg_source_67') }}
),

dep_39 as (
    select * from {{ ref('stg_source_77') }}
),

dep_40 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_41 as (
    select * from {{ ref('stg_source_88') }}
),

dep_42 as (
    select * from {{ ref('int_transformed_116') }}
),

dep_43 as (
    select * from {{ ref('stg_source_127') }}
),

dep_44 as (
    select * from {{ ref('stg_source_41') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_60') }}
),

dep_46 as (
    select * from {{ ref('stg_source_73') }}
),

dep_47 as (
    select * from {{ ref('stg_source_126') }}
),

dep_48 as (
    select * from {{ ref('stg_source_157') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_48') }}
),

dep_50 as (
    select * from {{ ref('stg_source_19') }}
),

combined as (
    select
        {{ dbt.concat(['dep_1.stg_source_37_id']) }} as surrogate_id,
        dep_1.stg_source_37_id as dim_entity_166_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_37_id = dep_2.int_transformed_101_id
    left join dep_3 on dep_1.stg_source_37_id = dep_3.stg_source_148_id
    left join dep_4 on dep_1.stg_source_37_id = dep_4.stg_source_53_id
    left join dep_5 on dep_1.stg_source_37_id = dep_5.int_transformed_144_id
    left join dep_6 on dep_1.stg_source_37_id = dep_6.stg_source_109_id
    left join dep_7 on dep_1.stg_source_37_id = dep_7.int_transformed_136_id
    left join dep_8 on dep_1.stg_source_37_id = dep_8.stg_source_56_id
    left join dep_9 on dep_1.stg_source_37_id = dep_9.stg_source_140_id
    left join dep_10 on dep_1.stg_source_37_id = dep_10.stg_source_100_id
    left join dep_11 on dep_1.stg_source_37_id = dep_11.stg_source_45_id
    left join dep_12 on dep_1.stg_source_37_id = dep_12.int_transformed_122_id
    left join dep_13 on dep_1.stg_source_37_id = dep_13.stg_source_82_id
    left join dep_14 on dep_1.stg_source_37_id = dep_14.int_transformed_37_id
    left join dep_15 on dep_1.stg_source_37_id = dep_15.int_transformed_14_id
    left join dep_16 on dep_1.stg_source_37_id = dep_16.stg_source_43_id
    left join dep_17 on dep_1.stg_source_37_id = dep_17.stg_source_121_id
    left join dep_18 on dep_1.stg_source_37_id = dep_18.stg_source_49_id
    left join dep_19 on dep_1.stg_source_37_id = dep_19.int_transformed_53_id
    left join dep_20 on dep_1.stg_source_37_id = dep_20.stg_source_155_id
    left join dep_21 on dep_1.stg_source_37_id = dep_21.stg_source_143_id
    left join dep_22 on dep_1.stg_source_37_id = dep_22.int_transformed_129_id
    left join dep_23 on dep_1.stg_source_37_id = dep_23.int_transformed_51_id
    left join dep_24 on dep_1.stg_source_37_id = dep_24.stg_source_81_id
    left join dep_25 on dep_1.stg_source_37_id = dep_25.stg_source_33_id
    left join dep_26 on dep_1.stg_source_37_id = dep_26.int_transformed_67_id
    left join dep_27 on dep_1.stg_source_37_id = dep_27.stg_source_124_id
    left join dep_28 on dep_1.stg_source_37_id = dep_28.stg_source_92_id
    left join dep_29 on dep_1.stg_source_37_id = dep_29.int_transformed_148_id
    left join dep_30 on dep_1.stg_source_37_id = dep_30.stg_source_9_id
    left join dep_31 on dep_1.stg_source_37_id = dep_31.stg_source_1_id
    left join dep_32 on dep_1.stg_source_37_id = dep_32.stg_source_57_id
    left join dep_33 on dep_1.stg_source_37_id = dep_33.stg_source_59_id
    left join dep_34 on dep_1.stg_source_37_id = dep_34.int_transformed_73_id
    left join dep_35 on dep_1.stg_source_37_id = dep_35.stg_source_38_id
    left join dep_36 on dep_1.stg_source_37_id = dep_36.int_transformed_111_id
    left join dep_37 on dep_1.stg_source_37_id = dep_37.stg_source_161_id
    left join dep_38 on dep_1.stg_source_37_id = dep_38.stg_source_67_id
    left join dep_39 on dep_1.stg_source_37_id = dep_39.stg_source_77_id
    left join dep_40 on dep_1.stg_source_37_id = dep_40.int_transformed_56_id
    left join dep_41 on dep_1.stg_source_37_id = dep_41.stg_source_88_id
    left join dep_42 on dep_1.stg_source_37_id = dep_42.int_transformed_116_id
    left join dep_43 on dep_1.stg_source_37_id = dep_43.stg_source_127_id
    left join dep_44 on dep_1.stg_source_37_id = dep_44.stg_source_41_id
    left join dep_45 on dep_1.stg_source_37_id = dep_45.int_transformed_60_id
    left join dep_46 on dep_1.stg_source_37_id = dep_46.stg_source_73_id
    left join dep_47 on dep_1.stg_source_37_id = dep_47.stg_source_126_id
    left join dep_48 on dep_1.stg_source_37_id = dep_48.stg_source_157_id
    left join dep_49 on dep_1.stg_source_37_id = dep_49.int_transformed_48_id
    left join dep_50 on dep_1.stg_source_37_id = dep_50.stg_source_19_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
