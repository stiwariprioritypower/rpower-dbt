with base as (
    select *
    from {{ ref('int_site') }}
)

select *
from base
