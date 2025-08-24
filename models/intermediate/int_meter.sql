with kristen_301924 as (
    select *
    from {{ ref('stg_kristen_301924_data') }}
),
kristen_301889 as (
    select *
    from {{ ref('stg_kristen_301889_data') }}
),
apollo_northside_water as (
    select *
    from {{ ref('stg_apollo_northside_water_a') }}
),
apollo_watergen_a as (
    select *
    from {{ ref('stg_apollo_watergen_a') }}
),
geus_fsti as (
    select *
    from {{ ref('stg_geus_fsti') }}
),
gvec_smiley_ps_kmci as (
    select *
    from {{ ref('stg_gvec_smiley_ps_kmci') }}
),
lhoist_data as (
    select *
    from {{ ref('stg_lhoist_data') }}
),
transportation_resources_loa_intervals as (
    select *
    from {{ ref('stg_transportation_resources_loa_intervals') }}
),


all_meters as (
    select meter_fk,
        meter_id,
        source_key
    from kristen_301924

    union all

    select meter_fk,
        meter_id,
        source_key
    from kristen_301889 

    union all

    select meter_fk,
        meter_id,
        source_key
    from apollo_northside_water

    union all 

    select meter_fk,
        meter_id,
        source_key
    from apollo_watergen_a

    union all

    select meter_fk,
        meter_id,
        source_key
    from geus_fsti

    union all

    select meter_fk,
        meter_id,
        source_key
    from gvec_smiley_ps_kmci

    union all 

    select meter_fk,
        meter_id,
        source_key
    from lhoist_data

    union all 

    select meter_fk,
        meter_id,
        source_key
    from transportation_resources_loa_intervals


)

select distinct * rename(meter_fk as meter_pk)
from all_meters