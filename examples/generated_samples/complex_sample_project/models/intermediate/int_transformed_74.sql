{{
    config(
        materialized='table',
        tags=['intermediate', 'daily']
    )
}}

-- Complex intermediate model with joins and window functions
with source_1 as (
    select * from {{ ref('stg_source_6') }}
),

source_2 as (
    select * from {{ ref('stg_source_9') }}
),

source_3 as (
    select * from {{ ref('stg_source_99') }}
),

source_4 as (
    select * from {{ ref('stg_source_47') }}
),

source_5 as (
    select * from {{ ref('stg_source_10') }}
),

source_6 as (
    select * from {{ ref('stg_source_88') }}
),

source_7 as (
    select * from {{ ref('stg_source_35') }}
),

source_8 as (
    select * from {{ ref('stg_source_102') }}
),

source_9 as (
    select * from {{ ref('stg_source_83') }}
),

source_10 as (
    select * from {{ ref('stg_source_92') }}
),

source_11 as (
    select * from {{ ref('stg_source_32') }}
),

source_12 as (
    select * from {{ ref('stg_source_40') }}
),

source_13 as (
    select * from {{ ref('stg_source_1') }}
),

source_14 as (
    select * from {{ ref('stg_source_133') }}
),

source_15 as (
    select * from {{ ref('stg_source_106') }}
),

source_16 as (
    select * from {{ ref('stg_source_71') }}
),

source_17 as (
    select * from {{ ref('stg_source_123') }}
),

source_18 as (
    select * from {{ ref('stg_source_143') }}
),

source_19 as (
    select * from {{ ref('stg_source_27') }}
),

source_20 as (
    select * from {{ ref('stg_source_18') }}
),

source_21 as (
    select * from {{ ref('stg_source_135') }}
),

source_22 as (
    select * from {{ ref('stg_source_56') }}
),

source_23 as (
    select * from {{ ref('stg_source_8') }}
),

source_24 as (
    select * from {{ ref('stg_source_50') }}
),

source_25 as (
    select * from {{ ref('stg_source_33') }}
),

source_26 as (
    select * from {{ ref('stg_source_43') }}
),

source_27 as (
    select * from {{ ref('stg_source_44') }}
),

source_28 as (
    select * from {{ ref('stg_source_93') }}
),

source_29 as (
    select * from {{ ref('stg_source_155') }}
),

source_30 as (
    select * from {{ ref('stg_source_82') }}
),

source_31 as (
    select * from {{ ref('stg_source_75') }}
),

source_32 as (
    select * from {{ ref('stg_source_112') }}
),

source_33 as (
    select * from {{ ref('stg_source_117') }}
),

source_34 as (
    select * from {{ ref('stg_source_132') }}
),

source_35 as (
    select * from {{ ref('stg_source_142') }}
),

source_36 as (
    select * from {{ ref('stg_source_148') }}
),

source_37 as (
    select * from {{ ref('stg_source_114') }}
),

source_38 as (
    select * from {{ ref('stg_source_84') }}
),

source_39 as (
    select * from {{ ref('stg_source_128') }}
),

source_40 as (
    select * from {{ ref('stg_source_15') }}
),

source_41 as (
    select * from {{ ref('stg_source_31') }}
),

source_42 as (
    select * from {{ ref('stg_source_59') }}
),

source_43 as (
    select * from {{ ref('stg_source_154') }}
),

source_44 as (
    select * from {{ ref('stg_source_150') }}
),

source_45 as (
    select * from {{ ref('stg_source_16') }}
),

source_46 as (
    select * from {{ ref('stg_source_162') }}
),

source_47 as (
    select * from {{ ref('stg_source_158') }}
),

source_48 as (
    select * from {{ ref('stg_source_86') }}
),

source_49 as (
    select * from {{ ref('stg_source_46') }}
),

source_50 as (
    select * from {{ ref('stg_source_61') }}
),

joined as (
    select
        source_1.stg_source_6_id as int_transformed_74_id,
        source_1.*,
        row_number() over (partition by source_1.stg_source_6_id order by source_1.stg_source_6_created_at desc) as row_num
    from source_1
    
    left join source_2 on source_1.stg_source_6_id = source_2.stg_source_9_id
    left join source_3 on source_1.stg_source_6_id = source_3.stg_source_99_id
    left join source_4 on source_1.stg_source_6_id = source_4.stg_source_47_id
    left join source_5 on source_1.stg_source_6_id = source_5.stg_source_10_id
    left join source_6 on source_1.stg_source_6_id = source_6.stg_source_88_id
    left join source_7 on source_1.stg_source_6_id = source_7.stg_source_35_id
    left join source_8 on source_1.stg_source_6_id = source_8.stg_source_102_id
    left join source_9 on source_1.stg_source_6_id = source_9.stg_source_83_id
    left join source_10 on source_1.stg_source_6_id = source_10.stg_source_92_id
    left join source_11 on source_1.stg_source_6_id = source_11.stg_source_32_id
    left join source_12 on source_1.stg_source_6_id = source_12.stg_source_40_id
    left join source_13 on source_1.stg_source_6_id = source_13.stg_source_1_id
    left join source_14 on source_1.stg_source_6_id = source_14.stg_source_133_id
    left join source_15 on source_1.stg_source_6_id = source_15.stg_source_106_id
    left join source_16 on source_1.stg_source_6_id = source_16.stg_source_71_id
    left join source_17 on source_1.stg_source_6_id = source_17.stg_source_123_id
    left join source_18 on source_1.stg_source_6_id = source_18.stg_source_143_id
    left join source_19 on source_1.stg_source_6_id = source_19.stg_source_27_id
    left join source_20 on source_1.stg_source_6_id = source_20.stg_source_18_id
    left join source_21 on source_1.stg_source_6_id = source_21.stg_source_135_id
    left join source_22 on source_1.stg_source_6_id = source_22.stg_source_56_id
    left join source_23 on source_1.stg_source_6_id = source_23.stg_source_8_id
    left join source_24 on source_1.stg_source_6_id = source_24.stg_source_50_id
    left join source_25 on source_1.stg_source_6_id = source_25.stg_source_33_id
    left join source_26 on source_1.stg_source_6_id = source_26.stg_source_43_id
    left join source_27 on source_1.stg_source_6_id = source_27.stg_source_44_id
    left join source_28 on source_1.stg_source_6_id = source_28.stg_source_93_id
    left join source_29 on source_1.stg_source_6_id = source_29.stg_source_155_id
    left join source_30 on source_1.stg_source_6_id = source_30.stg_source_82_id
    left join source_31 on source_1.stg_source_6_id = source_31.stg_source_75_id
    left join source_32 on source_1.stg_source_6_id = source_32.stg_source_112_id
    left join source_33 on source_1.stg_source_6_id = source_33.stg_source_117_id
    left join source_34 on source_1.stg_source_6_id = source_34.stg_source_132_id
    left join source_35 on source_1.stg_source_6_id = source_35.stg_source_142_id
    left join source_36 on source_1.stg_source_6_id = source_36.stg_source_148_id
    left join source_37 on source_1.stg_source_6_id = source_37.stg_source_114_id
    left join source_38 on source_1.stg_source_6_id = source_38.stg_source_84_id
    left join source_39 on source_1.stg_source_6_id = source_39.stg_source_128_id
    left join source_40 on source_1.stg_source_6_id = source_40.stg_source_15_id
    left join source_41 on source_1.stg_source_6_id = source_41.stg_source_31_id
    left join source_42 on source_1.stg_source_6_id = source_42.stg_source_59_id
    left join source_43 on source_1.stg_source_6_id = source_43.stg_source_154_id
    left join source_44 on source_1.stg_source_6_id = source_44.stg_source_150_id
    left join source_45 on source_1.stg_source_6_id = source_45.stg_source_16_id
    left join source_46 on source_1.stg_source_6_id = source_46.stg_source_162_id
    left join source_47 on source_1.stg_source_6_id = source_47.stg_source_158_id
    left join source_48 on source_1.stg_source_6_id = source_48.stg_source_86_id
    left join source_49 on source_1.stg_source_6_id = source_49.stg_source_46_id
    left join source_50 on source_1.stg_source_6_id = source_50.stg_source_61_id
),

final as (
    select
        *
    from joined
    where row_num = 1
)

select * from final
