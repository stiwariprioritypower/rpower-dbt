with base as (
    select *
    from {{ ref('int_date') }}
)


select *
from base