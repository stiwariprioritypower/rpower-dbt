with base as (
    select *
    from {{ ref('stg_ercot_np6_905_cd') }}
),
sites as (
    select distinct
        site_fk as site_pk,
        site_id,
        site_type
    from base
)

select *
from sites