{{
    config(
        materialized='table',
        tags=['intermediate', 'daily']
    )
}}

-- Complex intermediate model with joins and window functions
with source_1 as (
    select * from {{ ref('stg_source_137') }}
),

source_2 as (
    select * from {{ ref('stg_source_140') }}
),

source_3 as (
    select * from {{ ref('stg_source_60') }}
),

source_4 as (
    select * from {{ ref('stg_source_6') }}
),

source_5 as (
    select * from {{ ref('stg_source_92') }}
),

source_6 as (
    select * from {{ ref('stg_source_128') }}
),

source_7 as (
    select * from {{ ref('stg_source_112') }}
),

source_8 as (
    select * from {{ ref('stg_source_29') }}
),

source_9 as (
    select * from {{ ref('stg_source_145') }}
),

source_10 as (
    select * from {{ ref('stg_source_61') }}
),

source_11 as (
    select * from {{ ref('stg_source_7') }}
),

source_12 as (
    select * from {{ ref('stg_source_143') }}
),

source_13 as (
    select * from {{ ref('stg_source_101') }}
),

source_14 as (
    select * from {{ ref('stg_source_121') }}
),

source_15 as (
    select * from {{ ref('stg_source_161') }}
),

source_16 as (
    select * from {{ ref('stg_source_3') }}
),

source_17 as (
    select * from {{ ref('stg_source_156') }}
),

source_18 as (
    select * from {{ ref('stg_source_120') }}
),

source_19 as (
    select * from {{ ref('stg_source_23') }}
),

source_20 as (
    select * from {{ ref('stg_source_103') }}
),

source_21 as (
    select * from {{ ref('stg_source_68') }}
),

source_22 as (
    select * from {{ ref('stg_source_58') }}
),

source_23 as (
    select * from {{ ref('stg_source_90') }}
),

source_24 as (
    select * from {{ ref('stg_source_16') }}
),

source_25 as (
    select * from {{ ref('stg_source_18') }}
),

source_26 as (
    select * from {{ ref('stg_source_111') }}
),

source_27 as (
    select * from {{ ref('stg_source_4') }}
),

source_28 as (
    select * from {{ ref('stg_source_64') }}
),

source_29 as (
    select * from {{ ref('stg_source_53') }}
),

source_30 as (
    select * from {{ ref('stg_source_166') }}
),

source_31 as (
    select * from {{ ref('stg_source_141') }}
),

source_32 as (
    select * from {{ ref('stg_source_134') }}
),

source_33 as (
    select * from {{ ref('stg_source_66') }}
),

source_34 as (
    select * from {{ ref('stg_source_12') }}
),

source_35 as (
    select * from {{ ref('stg_source_70') }}
),

source_36 as (
    select * from {{ ref('stg_source_102') }}
),

source_37 as (
    select * from {{ ref('stg_source_148') }}
),

source_38 as (
    select * from {{ ref('stg_source_55') }}
),

source_39 as (
    select * from {{ ref('stg_source_28') }}
),

source_40 as (
    select * from {{ ref('stg_source_162') }}
),

source_41 as (
    select * from {{ ref('stg_source_49') }}
),

source_42 as (
    select * from {{ ref('stg_source_84') }}
),

source_43 as (
    select * from {{ ref('stg_source_115') }}
),

source_44 as (
    select * from {{ ref('stg_source_37') }}
),

source_45 as (
    select * from {{ ref('stg_source_79') }}
),

source_46 as (
    select * from {{ ref('stg_source_160') }}
),

source_47 as (
    select * from {{ ref('stg_source_125') }}
),

source_48 as (
    select * from {{ ref('stg_source_9') }}
),

source_49 as (
    select * from {{ ref('stg_source_27') }}
),

source_50 as (
    select * from {{ ref('stg_source_108') }}
),

joined as (
    select
        source_1.stg_source_137_id as int_transformed_150_id,
        source_1.*,
        row_number() over (partition by source_1.stg_source_137_id order by source_1.stg_source_137_created_at desc) as row_num
    from source_1
    
    left join source_2 on source_1.stg_source_137_id = source_2.stg_source_140_id
    left join source_3 on source_1.stg_source_137_id = source_3.stg_source_60_id
    left join source_4 on source_1.stg_source_137_id = source_4.stg_source_6_id
    left join source_5 on source_1.stg_source_137_id = source_5.stg_source_92_id
    left join source_6 on source_1.stg_source_137_id = source_6.stg_source_128_id
    left join source_7 on source_1.stg_source_137_id = source_7.stg_source_112_id
    left join source_8 on source_1.stg_source_137_id = source_8.stg_source_29_id
    left join source_9 on source_1.stg_source_137_id = source_9.stg_source_145_id
    left join source_10 on source_1.stg_source_137_id = source_10.stg_source_61_id
    left join source_11 on source_1.stg_source_137_id = source_11.stg_source_7_id
    left join source_12 on source_1.stg_source_137_id = source_12.stg_source_143_id
    left join source_13 on source_1.stg_source_137_id = source_13.stg_source_101_id
    left join source_14 on source_1.stg_source_137_id = source_14.stg_source_121_id
    left join source_15 on source_1.stg_source_137_id = source_15.stg_source_161_id
    left join source_16 on source_1.stg_source_137_id = source_16.stg_source_3_id
    left join source_17 on source_1.stg_source_137_id = source_17.stg_source_156_id
    left join source_18 on source_1.stg_source_137_id = source_18.stg_source_120_id
    left join source_19 on source_1.stg_source_137_id = source_19.stg_source_23_id
    left join source_20 on source_1.stg_source_137_id = source_20.stg_source_103_id
    left join source_21 on source_1.stg_source_137_id = source_21.stg_source_68_id
    left join source_22 on source_1.stg_source_137_id = source_22.stg_source_58_id
    left join source_23 on source_1.stg_source_137_id = source_23.stg_source_90_id
    left join source_24 on source_1.stg_source_137_id = source_24.stg_source_16_id
    left join source_25 on source_1.stg_source_137_id = source_25.stg_source_18_id
    left join source_26 on source_1.stg_source_137_id = source_26.stg_source_111_id
    left join source_27 on source_1.stg_source_137_id = source_27.stg_source_4_id
    left join source_28 on source_1.stg_source_137_id = source_28.stg_source_64_id
    left join source_29 on source_1.stg_source_137_id = source_29.stg_source_53_id
    left join source_30 on source_1.stg_source_137_id = source_30.stg_source_166_id
    left join source_31 on source_1.stg_source_137_id = source_31.stg_source_141_id
    left join source_32 on source_1.stg_source_137_id = source_32.stg_source_134_id
    left join source_33 on source_1.stg_source_137_id = source_33.stg_source_66_id
    left join source_34 on source_1.stg_source_137_id = source_34.stg_source_12_id
    left join source_35 on source_1.stg_source_137_id = source_35.stg_source_70_id
    left join source_36 on source_1.stg_source_137_id = source_36.stg_source_102_id
    left join source_37 on source_1.stg_source_137_id = source_37.stg_source_148_id
    left join source_38 on source_1.stg_source_137_id = source_38.stg_source_55_id
    left join source_39 on source_1.stg_source_137_id = source_39.stg_source_28_id
    left join source_40 on source_1.stg_source_137_id = source_40.stg_source_162_id
    left join source_41 on source_1.stg_source_137_id = source_41.stg_source_49_id
    left join source_42 on source_1.stg_source_137_id = source_42.stg_source_84_id
    left join source_43 on source_1.stg_source_137_id = source_43.stg_source_115_id
    left join source_44 on source_1.stg_source_137_id = source_44.stg_source_37_id
    left join source_45 on source_1.stg_source_137_id = source_45.stg_source_79_id
    left join source_46 on source_1.stg_source_137_id = source_46.stg_source_160_id
    left join source_47 on source_1.stg_source_137_id = source_47.stg_source_125_id
    left join source_48 on source_1.stg_source_137_id = source_48.stg_source_9_id
    left join source_49 on source_1.stg_source_137_id = source_49.stg_source_27_id
    left join source_50 on source_1.stg_source_137_id = source_50.stg_source_108_id
),

final as (
    select
        *
    from joined
    where row_num = 1
)

select * from final
