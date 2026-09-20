{{ config(materialized='table') }}

-- Grain: one row per physical market.
--
-- There is no stable upstream market ID -- the name *is* the business key. That
-- means a genuine rename is indistinguishable from a new market, so this is
-- deliberately not an SCD2: modelling slowly-changing attributes on top of an
-- unstable key would manufacture false history. Instead we record observed
-- lifespan, which is both true and useful: it reveals markets that stop
-- reporting, which is the dominant data-quality issue in this feed.

with observed as (

    select
        state,
        district,
        market,
        min(arrival_date)                       as first_reported_on,
        max(arrival_date)                       as last_reported_on,
        count(distinct arrival_date)            as days_reported,
        count(distinct commodity)               as commodities_traded,
        count(*)                                as observations
    from {{ ref('stg_mandi_prices') }}
    where market is not null
    group by 1, 2, 3

), bounds as (
    select max(arrival_date) as feed_latest from {{ ref('stg_mandi_prices') }}
)

select
    md5(concat_ws('|', o.state, o.district, o.market))   as market_key,
    o.state,
    o.district,
    o.market,
    o.first_reported_on,
    o.last_reported_on,
    o.days_reported,
    o.commodities_traded,
    o.observations,
    date_diff('day', o.last_reported_on, b.feed_latest)  as days_since_last_report,
    -- A market silent for over a fortnight has effectively dropped out.
    (date_diff('day', o.last_reported_on, b.feed_latest) <= 14) as is_actively_reporting
from observed o
cross join bounds b
