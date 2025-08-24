with base as (
    select *
    from {{ source('scada', 'kristen_scada_export') }}
),
renamed as (
    select
        point_name,
        measurement,
        DATE(timestamp) as date,
        CASE
            WHEN EXTRACT(HOUR FROM timestamp) = 0 AND EXTRACT(MINUTE FROM timestamp) = 0 THEN 1
            ELSE FLOOR((EXTRACT(HOUR FROM timestamp) * 60 + EXTRACT(MINUTE FROM timestamp)) / 15) + 1
        END AS time_interval,
        'SCADA' as source_key,
        TIME(timestamp) as time,
        timestamp,
        SRC_FILENAME,
        SRC_FILE_MODIFIED_DATETIME as last_update_datetime,
        'LZ_HOUSTON' as site_id,
        'LZ' as site_type


    from
        base
),
keys as (
    select *,
        {{ hash_key_generator(['point_name','date','time']) }} as scada_export_pk,
        {{ hash_key_generator(['time_interval']) }} as time_interval_fk,
        {{ hash_key_generator(['date']) }} as date_fk,
        {{ hash_key_generator(['site_id', 'site_type']) }} as site_fk

    from renamed
)
select *
from keys
qualify row_number() over (partition by scada_export_pk order by last_update_datetime desc) = 1
