with base as (
    select *
    from {{ source('lhoist', 'lhoist_data') }}
),
renamed as (
    select 
        unique_meter_id as meter_history_id,
        'NBU LHoist' as meter_id,
        date as record_date,
        (date_part(hour, time)+1) as record_hour,
        case
            when date_part(minute, time)  = 0 then 1
            when date_part(minute, time)  = 15 then 2
            when date_part(minute, time)  = 30 then 3
            when date_part(minute, time)  = 45 then 4
        end as record_time_interval,
        cast(record_time_interval + ((record_hour-1) * 4) as integer) as time_interval_id,
        kwh as kwh_out,
        'LHOIST' as source_key
    from
        base
    where
        unique_meter_id is not null
),
keys as (
    select *,
        {{ hash_key_generator(['meter_id','record_date','record_hour','record_time_interval']) }} as meter_history_pk,
        {{ hash_key_generator(['meter_id']) }} as meter_fk,
        {{ hash_key_generator(['record_date']) }} as date_fk,
        {{ hash_key_generator(['time_interval_id']) }} as time_interval_fk
    from renamed
)

select *
from keys
-- qualify rank() over (partition by meter_history_pk order by last_update_date desc) = 1