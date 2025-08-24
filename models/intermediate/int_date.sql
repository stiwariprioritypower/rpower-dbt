
with base as (
    {{ dbt_date.get_date_dimension("2022-01-01", "2025-10-31") }}

),
dates as (
    select * , {{ hash_key_generator(['date_day']) }} as date_pk
    from base
    
)


select *
from dates

