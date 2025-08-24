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
meters_to_sites as (
    select *
    from {{ ref('meters_to_sites') }}
),

all_meters as (
    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        kwh_in,
        source_key
    from kristen_301924

    union all

    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        null as kwh_in,
        source_key
    from kristen_301889    
    
    union all

    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        null as kwh_in,
        source_key
    from apollo_northside_water

    union all

    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        null as kwh_in,
        source_key
    from apollo_watergen_a

    union all
    
    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        null as kwh_in,
        source_key
    from geus_fsti

    union all

    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        null as kwh_in,
        source_key
    from gvec_smiley_ps_kmci

    union all

    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        null as kwh_in,
        source_key
    from lhoist_data

    union all 

    select meter_history_pk,
        meter_id,
        meter_fk,
        date_fk,
        time_interval_fk,
        kwh_out,
        null as kwh_in,
        source_key
    from transportation_resources_loa_intervals

)

select m.* exclude(meter_id), ms.site_fk
from all_meters m
left join meters_to_sites ms on m.meter_id = ms.meter_id