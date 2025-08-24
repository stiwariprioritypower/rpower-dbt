with base as (
    select *
    from {{ ref('int_site_price_history') }}
)

select *
from base
