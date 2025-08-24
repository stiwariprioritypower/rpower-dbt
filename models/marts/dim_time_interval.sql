with base as (
    select *
    from {{ ref('int_time_interval') }}
)

select *
from base