with base as (
    select *
    from {{ ref('int_generator_production_history') }}
)

select *
from base