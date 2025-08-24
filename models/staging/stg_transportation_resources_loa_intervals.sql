with base as (
    select *
    from {{ source('transportation', 'transportation_resources_loa_intervals') }}
),
renamed as (
    select 
        esi_id as esi_id,
        cust_acct_id as customer_account_id,
        pmi_id as pmi_id,
        'Select Transport' as meter_id,
        intv_rec_dt as record_date,
        (left(lpad(intv_beg_tm,4,'0'),2) + 1) as record_hour, --add 1 to conform to ERCOT data which runs 1-24
        case
            when right(intv_beg_tm,2) = 00 then 1
            when right(intv_beg_tm,2) = 15 then 2
            when right(intv_beg_tm,2) = 30 then 3
            when right(intv_beg_tm,2) = 45 then 4
        end as record_time_interval,
        rev_yr_dt as revenue_year,
        rev_mo_dt as revenue_month,
        kvar_qt as kvar,
        kw_qt as kw_out,
        (kw_qt * .25) as kwh_out,
        power_factr_pc as power_factor,
        kva_qt as kva,
        cast(record_time_interval + ((record_hour-1) * 4) as integer) as time_interval_id,
        'TRANSPORTATION' as source_key

    from base
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