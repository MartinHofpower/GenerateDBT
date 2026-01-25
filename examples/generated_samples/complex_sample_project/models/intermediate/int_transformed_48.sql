{{
    config(
        materialized='table',
        tags=['intermediate', 'daily']
    )
}}

-- Complex intermediate model with joins and window functions
with source_1 as (
    select * from {{ ref('stg_source_41') }}
),

source_2 as (
    select * from {{ ref('stg_source_78') }}
),

source_3 as (
    select * from {{ ref('stg_source_25') }}
),

source_4 as (
    select * from {{ ref('stg_source_98') }}
),

source_5 as (
    select * from {{ ref('stg_source_43') }}
),

source_6 as (
    select * from {{ ref('stg_source_57') }}
),

source_7 as (
    select * from {{ ref('stg_source_117') }}
),

source_8 as (
    select * from {{ ref('stg_source_48') }}
),

source_9 as (
    select * from {{ ref('stg_source_76') }}
),

source_10 as (
    select * from {{ ref('stg_source_56') }}
),

source_11 as (
    select * from {{ ref('stg_source_164') }}
),

source_12 as (
    select * from {{ ref('stg_source_125') }}
),

source_13 as (
    select * from {{ ref('stg_source_15') }}
),

source_14 as (
    select * from {{ ref('stg_source_2') }}
),

source_15 as (
    select * from {{ ref('stg_source_53') }}
),

source_16 as (
    select * from {{ ref('stg_source_74') }}
),

source_17 as (
    select * from {{ ref('stg_source_60') }}
),

source_18 as (
    select * from {{ ref('stg_source_153') }}
),

source_19 as (
    select * from {{ ref('stg_source_139') }}
),

source_20 as (
    select * from {{ ref('stg_source_101') }}
),

source_21 as (
    select * from {{ ref('stg_source_161') }}
),

source_22 as (
    select * from {{ ref('stg_source_138') }}
),

source_23 as (
    select * from {{ ref('stg_source_5') }}
),

source_24 as (
    select * from {{ ref('stg_source_8') }}
),

source_25 as (
    select * from {{ ref('stg_source_163') }}
),

source_26 as (
    select * from {{ ref('stg_source_118') }}
),

source_27 as (
    select * from {{ ref('stg_source_54') }}
),

source_28 as (
    select * from {{ ref('stg_source_143') }}
),

source_29 as (
    select * from {{ ref('stg_source_63') }}
),

source_30 as (
    select * from {{ ref('stg_source_83') }}
),

source_31 as (
    select * from {{ ref('stg_source_71') }}
),

source_32 as (
    select * from {{ ref('stg_source_144') }}
),

source_33 as (
    select * from {{ ref('stg_source_123') }}
),

source_34 as (
    select * from {{ ref('stg_source_127') }}
),

source_35 as (
    select * from {{ ref('stg_source_113') }}
),

source_36 as (
    select * from {{ ref('stg_source_109') }}
),

source_37 as (
    select * from {{ ref('stg_source_14') }}
),

source_38 as (
    select * from {{ ref('stg_source_95') }}
),

source_39 as (
    select * from {{ ref('stg_source_13') }}
),

source_40 as (
    select * from {{ ref('stg_source_85') }}
),

source_41 as (
    select * from {{ ref('stg_source_158') }}
),

source_42 as (
    select * from {{ ref('stg_source_80') }}
),

source_43 as (
    select * from {{ ref('stg_source_84') }}
),

source_44 as (
    select * from {{ ref('stg_source_68') }}
),

source_45 as (
    select * from {{ ref('stg_source_108') }}
),

source_46 as (
    select * from {{ ref('stg_source_45') }}
),

source_47 as (
    select * from {{ ref('stg_source_47') }}
),

source_48 as (
    select * from {{ ref('stg_source_159') }}
),

source_49 as (
    select * from {{ ref('stg_source_145') }}
),

source_50 as (
    select * from {{ ref('stg_source_34') }}
),

joined as (
    select
        source_1.stg_source_41_id as int_transformed_48_id,
        source_1.*,
        row_number() over (partition by source_1.stg_source_41_id order by source_1.stg_source_41_created_at desc) as row_num
    from source_1
    
    left join source_2 on source_1.stg_source_41_id = source_2.stg_source_78_id
    left join source_3 on source_1.stg_source_41_id = source_3.stg_source_25_id
    left join source_4 on source_1.stg_source_41_id = source_4.stg_source_98_id
    left join source_5 on source_1.stg_source_41_id = source_5.stg_source_43_id
    left join source_6 on source_1.stg_source_41_id = source_6.stg_source_57_id
    left join source_7 on source_1.stg_source_41_id = source_7.stg_source_117_id
    left join source_8 on source_1.stg_source_41_id = source_8.stg_source_48_id
    left join source_9 on source_1.stg_source_41_id = source_9.stg_source_76_id
    left join source_10 on source_1.stg_source_41_id = source_10.stg_source_56_id
    left join source_11 on source_1.stg_source_41_id = source_11.stg_source_164_id
    left join source_12 on source_1.stg_source_41_id = source_12.stg_source_125_id
    left join source_13 on source_1.stg_source_41_id = source_13.stg_source_15_id
    left join source_14 on source_1.stg_source_41_id = source_14.stg_source_2_id
    left join source_15 on source_1.stg_source_41_id = source_15.stg_source_53_id
    left join source_16 on source_1.stg_source_41_id = source_16.stg_source_74_id
    left join source_17 on source_1.stg_source_41_id = source_17.stg_source_60_id
    left join source_18 on source_1.stg_source_41_id = source_18.stg_source_153_id
    left join source_19 on source_1.stg_source_41_id = source_19.stg_source_139_id
    left join source_20 on source_1.stg_source_41_id = source_20.stg_source_101_id
    left join source_21 on source_1.stg_source_41_id = source_21.stg_source_161_id
    left join source_22 on source_1.stg_source_41_id = source_22.stg_source_138_id
    left join source_23 on source_1.stg_source_41_id = source_23.stg_source_5_id
    left join source_24 on source_1.stg_source_41_id = source_24.stg_source_8_id
    left join source_25 on source_1.stg_source_41_id = source_25.stg_source_163_id
    left join source_26 on source_1.stg_source_41_id = source_26.stg_source_118_id
    left join source_27 on source_1.stg_source_41_id = source_27.stg_source_54_id
    left join source_28 on source_1.stg_source_41_id = source_28.stg_source_143_id
    left join source_29 on source_1.stg_source_41_id = source_29.stg_source_63_id
    left join source_30 on source_1.stg_source_41_id = source_30.stg_source_83_id
    left join source_31 on source_1.stg_source_41_id = source_31.stg_source_71_id
    left join source_32 on source_1.stg_source_41_id = source_32.stg_source_144_id
    left join source_33 on source_1.stg_source_41_id = source_33.stg_source_123_id
    left join source_34 on source_1.stg_source_41_id = source_34.stg_source_127_id
    left join source_35 on source_1.stg_source_41_id = source_35.stg_source_113_id
    left join source_36 on source_1.stg_source_41_id = source_36.stg_source_109_id
    left join source_37 on source_1.stg_source_41_id = source_37.stg_source_14_id
    left join source_38 on source_1.stg_source_41_id = source_38.stg_source_95_id
    left join source_39 on source_1.stg_source_41_id = source_39.stg_source_13_id
    left join source_40 on source_1.stg_source_41_id = source_40.stg_source_85_id
    left join source_41 on source_1.stg_source_41_id = source_41.stg_source_158_id
    left join source_42 on source_1.stg_source_41_id = source_42.stg_source_80_id
    left join source_43 on source_1.stg_source_41_id = source_43.stg_source_84_id
    left join source_44 on source_1.stg_source_41_id = source_44.stg_source_68_id
    left join source_45 on source_1.stg_source_41_id = source_45.stg_source_108_id
    left join source_46 on source_1.stg_source_41_id = source_46.stg_source_45_id
    left join source_47 on source_1.stg_source_41_id = source_47.stg_source_47_id
    left join source_48 on source_1.stg_source_41_id = source_48.stg_source_159_id
    left join source_49 on source_1.stg_source_41_id = source_49.stg_source_145_id
    left join source_50 on source_1.stg_source_41_id = source_50.stg_source_34_id
),

final as (
    select
        *
    from joined
    where row_num = 1
)

select * from final
