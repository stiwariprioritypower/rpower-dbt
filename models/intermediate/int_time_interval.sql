with base as (
    select *
    from {{ ref('stg_ercot_np6_905_cd') }}
),
intervals as (
    select distinct
        time_interval_fk as time_interval_pk,
        time_interval_id,
        date_hour,
        date_time_interval
    from base
),
generated_intervals as (
    select *
    from {{ ref('time_intervals') }}
)

select
    intervals.time_interval_pk,
    intervals.time_interval_id,
    intervals.date_hour,
    intervals.date_time_interval,
    generated_intervals.interval_start,
    generated_intervals.interval_end
from intervals
left join generated_intervals
    on generated_intervals.interval_id = intervals.time_interval_id