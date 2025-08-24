with base as (
    select *
    from {{ ref('int_meter_usage_history') }}
)

select *
from base
