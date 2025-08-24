with base as (
    select *
    from {{ source('ercot', 'np6_905_cd') }}
),
renamed as (
    select 
        delivery_date,
        delivery_hour as date_hour,
        delivery_interval as date_time_interval,
        delivery_interval + ((delivery_hour-1) * 4) as time_interval_id,
        settlement_point as site_id,
        settlement_point_type as site_type,
        dst_flag as daylight_savings_time_flag,
        'ERCOT' as source_key,
        insert_datetime as last_update_date,
        settlement_point_price as price_kwh
    from
        base
),
keys as (
    select *,
        {{ hash_key_generator(['delivery_date', 'date_hour', 'date_time_interval', 'site_id', 'site_type', 'daylight_savings_time_flag']) }} as site_price_history_pk,
        {{ hash_key_generator(['site_id', 'site_type']) }} as site_fk,
        {{ hash_key_generator(['delivery_date']) }} as date_fk,
        {{ hash_key_generator(['time_interval_id']) }} as time_interval_fk
    from renamed
)

select *
from keys
qualify rank() over (partition by site_price_history_pk order by last_update_date desc) = 1