with base as (
    select *
    from {{ ref('stg_ercot_np6_905_cd') }}
),
pricing as (
    select distinct
    site_price_history_pk,
    date_fk,
    time_interval_fk,
    site_fk,
    price_kwh
    
    from base
)

select *
from pricing