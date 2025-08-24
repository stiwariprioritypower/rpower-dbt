with base as (
    select *
    from {{ ref('int_meter') }}
)

select *
from base
