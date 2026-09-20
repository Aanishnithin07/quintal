{{ config(materialized='table') }}

-- Spans every date the archive has observed, so gaps in the fact table are
-- visible as absences rather than simply missing rows.

with span as (
    select
        least(min(arrival_date), min(ingest_date))  as d0,
        greatest(max(arrival_date), max(ingest_date)) as d1
    from {{ ref('stg_mandi_prices') }}
), days as (
    select unnest(generate_series(d0, d1, interval 1 day))::date as date_day
    from span
)

select
    date_day,
    cast(strftime(date_day, '%Y%m%d') as integer)   as date_key,
    year(date_day)                                  as year,
    quarter(date_day)                               as quarter,
    month(date_day)                                 as month,
    monthname(date_day)                             as month_name,
    day(date_day)                                   as day_of_month,
    dayname(date_day)                               as day_name,
    isodow(date_day)                                as iso_day_of_week,
    (isodow(date_day) >= 6)                         as is_weekend,
    -- Indian crop years run April-March; reporting and seasonality follow them.
    case when month(date_day) >= 4
         then year(date_day) else year(date_day) - 1 end as crop_year_start
from days
