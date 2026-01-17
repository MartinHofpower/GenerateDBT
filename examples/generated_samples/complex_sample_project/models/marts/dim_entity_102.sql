{{
    config(
        materialized='incremental',
        unique_key='surrogate_id',
        tags=['marts', 'dimension', 'incremental']
    )
}}

-- Complex dimension table with incremental loading
with dep_1 as (
    select * from {{ ref('stg_source_77') }}
),

dep_2 as (
    select * from {{ ref('stg_source_98') }}
),

dep_3 as (
    select * from {{ ref('int_transformed_122') }}
),

dep_4 as (
    select * from {{ ref('stg_source_120') }}
),

dep_5 as (
    select * from {{ ref('stg_source_136') }}
),

dep_6 as (
    select * from {{ ref('stg_source_128') }}
),

dep_7 as (
    select * from {{ ref('stg_source_135') }}
),

dep_8 as (
    select * from {{ ref('stg_source_157') }}
),

dep_9 as (
    select * from {{ ref('int_transformed_143') }}
),

dep_10 as (
    select * from {{ ref('stg_source_51') }}
),

dep_11 as (
    select * from {{ ref('int_transformed_54') }}
),

dep_12 as (
    select * from {{ ref('int_transformed_66') }}
),

dep_13 as (
    select * from {{ ref('stg_source_122') }}
),

dep_14 as (
    select * from {{ ref('int_transformed_126') }}
),

dep_15 as (
    select * from {{ ref('stg_source_105') }}
),

dep_16 as (
    select * from {{ ref('stg_source_4') }}
),

dep_17 as (
    select * from {{ ref('int_transformed_29') }}
),

dep_18 as (
    select * from {{ ref('stg_source_91') }}
),

dep_19 as (
    select * from {{ ref('stg_source_11') }}
),

dep_20 as (
    select * from {{ ref('int_transformed_38') }}
),

dep_21 as (
    select * from {{ ref('stg_source_2') }}
),

dep_22 as (
    select * from {{ ref('stg_source_48') }}
),

dep_23 as (
    select * from {{ ref('stg_source_42') }}
),

dep_24 as (
    select * from {{ ref('stg_source_6') }}
),

dep_25 as (
    select * from {{ ref('int_transformed_78') }}
),

dep_26 as (
    select * from {{ ref('int_transformed_157') }}
),

dep_27 as (
    select * from {{ ref('int_transformed_88') }}
),

dep_28 as (
    select * from {{ ref('stg_source_52') }}
),

dep_29 as (
    select * from {{ ref('int_transformed_148') }}
),

dep_30 as (
    select * from {{ ref('stg_source_123') }}
),

dep_31 as (
    select * from {{ ref('stg_source_84') }}
),

dep_32 as (
    select * from {{ ref('int_transformed_160') }}
),

dep_33 as (
    select * from {{ ref('int_transformed_75') }}
),

dep_34 as (
    select * from {{ ref('stg_source_147') }}
),

dep_35 as (
    select * from {{ ref('int_transformed_56') }}
),

dep_36 as (
    select * from {{ ref('stg_source_19') }}
),

dep_37 as (
    select * from {{ ref('int_transformed_104') }}
),

dep_38 as (
    select * from {{ ref('stg_source_119') }}
),

dep_39 as (
    select * from {{ ref('int_transformed_42') }}
),

dep_40 as (
    select * from {{ ref('stg_source_126') }}
),

dep_41 as (
    select * from {{ ref('stg_source_99') }}
),

dep_42 as (
    select * from {{ ref('stg_source_18') }}
),

dep_43 as (
    select * from {{ ref('int_transformed_107') }}
),

dep_44 as (
    select * from {{ ref('stg_source_47') }}
),

dep_45 as (
    select * from {{ ref('int_transformed_86') }}
),

dep_46 as (
    select * from {{ ref('stg_source_158') }}
),

dep_47 as (
    select * from {{ ref('stg_source_63') }}
),

dep_48 as (
    select * from {{ ref('stg_source_138') }}
),

dep_49 as (
    select * from {{ ref('int_transformed_117') }}
),

dep_50 as (
    select * from {{ ref('stg_source_53') }}
),

combined as (
    select
        {{ dbt_utils.generate_surrogate_key(['dep_1.stg_source_77_id']) }} as surrogate_id,
        dep_1.stg_source_77_id as dim_entity_102_id,
        dep_1.*,
        {{ dbt.current_timestamp() }} as inserted_at,
        {{ dbt.current_timestamp() }} as updated_at
    from dep_1
    
    left join dep_2 on dep_1.stg_source_77_id = dep_2.stg_source_98_id
    left join dep_3 on dep_1.stg_source_77_id = dep_3.int_transformed_122_id
    left join dep_4 on dep_1.stg_source_77_id = dep_4.stg_source_120_id
    left join dep_5 on dep_1.stg_source_77_id = dep_5.stg_source_136_id
    left join dep_6 on dep_1.stg_source_77_id = dep_6.stg_source_128_id
    left join dep_7 on dep_1.stg_source_77_id = dep_7.stg_source_135_id
    left join dep_8 on dep_1.stg_source_77_id = dep_8.stg_source_157_id
    left join dep_9 on dep_1.stg_source_77_id = dep_9.int_transformed_143_id
    left join dep_10 on dep_1.stg_source_77_id = dep_10.stg_source_51_id
    left join dep_11 on dep_1.stg_source_77_id = dep_11.int_transformed_54_id
    left join dep_12 on dep_1.stg_source_77_id = dep_12.int_transformed_66_id
    left join dep_13 on dep_1.stg_source_77_id = dep_13.stg_source_122_id
    left join dep_14 on dep_1.stg_source_77_id = dep_14.int_transformed_126_id
    left join dep_15 on dep_1.stg_source_77_id = dep_15.stg_source_105_id
    left join dep_16 on dep_1.stg_source_77_id = dep_16.stg_source_4_id
    left join dep_17 on dep_1.stg_source_77_id = dep_17.int_transformed_29_id
    left join dep_18 on dep_1.stg_source_77_id = dep_18.stg_source_91_id
    left join dep_19 on dep_1.stg_source_77_id = dep_19.stg_source_11_id
    left join dep_20 on dep_1.stg_source_77_id = dep_20.int_transformed_38_id
    left join dep_21 on dep_1.stg_source_77_id = dep_21.stg_source_2_id
    left join dep_22 on dep_1.stg_source_77_id = dep_22.stg_source_48_id
    left join dep_23 on dep_1.stg_source_77_id = dep_23.stg_source_42_id
    left join dep_24 on dep_1.stg_source_77_id = dep_24.stg_source_6_id
    left join dep_25 on dep_1.stg_source_77_id = dep_25.int_transformed_78_id
    left join dep_26 on dep_1.stg_source_77_id = dep_26.int_transformed_157_id
    left join dep_27 on dep_1.stg_source_77_id = dep_27.int_transformed_88_id
    left join dep_28 on dep_1.stg_source_77_id = dep_28.stg_source_52_id
    left join dep_29 on dep_1.stg_source_77_id = dep_29.int_transformed_148_id
    left join dep_30 on dep_1.stg_source_77_id = dep_30.stg_source_123_id
    left join dep_31 on dep_1.stg_source_77_id = dep_31.stg_source_84_id
    left join dep_32 on dep_1.stg_source_77_id = dep_32.int_transformed_160_id
    left join dep_33 on dep_1.stg_source_77_id = dep_33.int_transformed_75_id
    left join dep_34 on dep_1.stg_source_77_id = dep_34.stg_source_147_id
    left join dep_35 on dep_1.stg_source_77_id = dep_35.int_transformed_56_id
    left join dep_36 on dep_1.stg_source_77_id = dep_36.stg_source_19_id
    left join dep_37 on dep_1.stg_source_77_id = dep_37.int_transformed_104_id
    left join dep_38 on dep_1.stg_source_77_id = dep_38.stg_source_119_id
    left join dep_39 on dep_1.stg_source_77_id = dep_39.int_transformed_42_id
    left join dep_40 on dep_1.stg_source_77_id = dep_40.stg_source_126_id
    left join dep_41 on dep_1.stg_source_77_id = dep_41.stg_source_99_id
    left join dep_42 on dep_1.stg_source_77_id = dep_42.stg_source_18_id
    left join dep_43 on dep_1.stg_source_77_id = dep_43.int_transformed_107_id
    left join dep_44 on dep_1.stg_source_77_id = dep_44.stg_source_47_id
    left join dep_45 on dep_1.stg_source_77_id = dep_45.int_transformed_86_id
    left join dep_46 on dep_1.stg_source_77_id = dep_46.stg_source_158_id
    left join dep_47 on dep_1.stg_source_77_id = dep_47.stg_source_63_id
    left join dep_48 on dep_1.stg_source_77_id = dep_48.stg_source_138_id
    left join dep_49 on dep_1.stg_source_77_id = dep_49.int_transformed_117_id
    left join dep_50 on dep_1.stg_source_77_id = dep_50.stg_source_53_id
),

final as (
    select * from combined
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select * from final
