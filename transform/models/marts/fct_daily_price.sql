{{ config(materialized='table') }}

-- Grain: one price observation per market x commodity x day.
--
-- Partitioned on `arrival_date` (the market day), while `ingest_date` is kept
-- as a degenerate dimension. The two differ whenever a market reports late, and
-- collapsing them would quietly rewrite history every time that happened.

select
    p.price_key,

    md5(concat_ws('|', p.state, p.district, p.market))                as market_key,
    md5(concat_ws('|', p.commodity, coalesce(p.variety, ''),
                       coalesce(p.grade, '')))                        as commodity_key,
    cast(strftime(p.arrival_date, '%Y%m%d') as integer)               as date_key,

    p.arrival_date,
    p.ingest_date,
    p.reporting_lag_days,

    p.min_price,
    p.max_price,
    p.modal_price,

    -- Spread is the headline signal in this feed: a wide min-max band means
    -- quality dispersion or thin trading, both of which matter to a seller.
    p.max_price - p.min_price                                         as price_spread,
    case when p.min_price > 0
         then round((p.max_price - p.min_price) / p.min_price * 100, 2)
    end                                                               as price_spread_pct,

    p.is_missing_price,
    p.is_inverted_range,
    p.is_modal_outside_range,
    p.is_unparseable_date

from {{ ref('stg_mandi_prices') }} p
where p.arrival_date is not null
