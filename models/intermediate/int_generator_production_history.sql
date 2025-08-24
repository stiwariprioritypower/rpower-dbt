with base_kristen as (
    select 
        scada_export_pk,
        date_fk,
        site_fk,
        time_interval_fk,
        measurement as KWH,
        time,
        'KRISTEN' as source_key

    from {{ ref('stg_kristen_scada_export') }}
    where point_name = 'Kristen_All_Gens_Total_KWH'
),

base_metox as (
    select 
        scada_export_pk,
        date_fk,
        site_fk,
        time_interval_fk,
        measurement as KWH,
        time,
        'METOX' as source_key
    from {{ ref('stg_metox_scada_export') }}
    where point_name = 'Mains Total kW'
),

combined_data as (
    select * from base_kristen
    UNION ALL
    select * from base_metox
)

select * from combined_data