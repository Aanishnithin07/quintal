{{ config(materialized='table') }}

-- One row per captured day: the archive's own health record.
--
-- This exists because the pipeline's real failure mode is not crashing -- it is
-- succeeding while quietly capturing less than it should. A snapshot that lands
-- with a third of its usual rows looks identical to a good one unless something
-- is explicitly watching the shape of each day.

with per_day as (

    select
        ingest_date,
        count(*)                                        as rows_captured,
        count(distinct market)                          as markets_reporting,
        count(distinct commodity)                       as commodities,
        count(distinct state)                           as states,
        count(distinct arrival_date)                    as distinct_arrival_dates,
        sum(case when is_missing_price then 1 else 0 end)        as missing_price_rows,
        sum(case when is_inverted_range then 1 else 0 end)       as inverted_range_rows,
        sum(case when is_modal_outside_range then 1 else 0 end)  as modal_outside_range_rows,
        sum(case when is_unparseable_date then 1 else 0 end)     as unparseable_date_rows,
        round(avg(reporting_lag_days), 2)               as avg_reporting_lag_days
    from {{ ref('stg_mandi_prices') }}
    group by ingest_date

)

select
    *,
    round(100.0 * missing_price_rows / nullif(rows_captured, 0), 2) as missing_price_pct,

    -- Compared against the trailing week rather than a fixed threshold, because
    -- the feed's normal volume drifts with season and market holidays.
    round(avg(rows_captured) over (
        order by ingest_date rows between 7 preceding and 1 preceding
    ), 0)                                                          as trailing_7d_avg_rows,

    case
        when count(*) over () < 3 then 'insufficient_history'
        when rows_captured < 0.6 * avg(rows_captured) over (
                 order by ingest_date rows between 7 preceding and 1 preceding)
            then 'suspiciously_low'
        when rows_captured > 1.8 * avg(rows_captured) over (
                 order by ingest_date rows between 7 preceding and 1 preceding)
            then 'suspiciously_high'
        else 'normal'
    end                                                            as volume_verdict
from per_day
order by ingest_date
