{{ config(materialized='table') }}

-- The archive's actual product: "is today's price at this mandi unusual?"
--
-- Compared against the same market's own trailing 30 days, not against a
-- national average. A tomato price in Kolar is not meaningfully comparable to
-- one in Ludhiana -- different varieties, transport costs, and buyers - so a
-- cross-market benchmark would flag ordinary regional differences as anomalies
-- every single day and be ignored within a week.
--
-- Median rather than mean, because a single premium lot moves a mean enough to
-- mask the very spike this is meant to catch.

with base as (

    select
        f.arrival_date,
        f.market_key,
        f.commodity_key,
        f.modal_price,
        m.state,
        m.district,
        m.market,
        c.commodity,
        c.variety
    from {{ ref('fct_daily_price') }} f
    join {{ ref('dim_market') }}    m using (market_key)
    join {{ ref('dim_commodity') }} c using (commodity_key)
    where f.modal_price is not null

), windowed as (

    select
        *,
        quantile_cont(modal_price, 0.5) over w    as median_30d,
        count(*) over w                           as history_days
    from base
    window w as (
        partition by market_key, commodity_key
        order by arrival_date
        -- Excludes the current day: a baseline that contains the value being
        -- tested pulls itself toward that value and mutes real spikes.
        range between interval 30 days preceding and interval 1 day preceding
    )

)

select
    arrival_date,
    state,
    district,
    market,
    commodity,
    variety,
    modal_price,
    round(median_30d, 2)                                        as median_30d,
    history_days,
    round((modal_price - median_30d) / nullif(median_30d, 0) * 100, 2)
                                                                as deviation_pct,
    case
        -- Under a week of history, the median is noise wearing a number's
        -- clothes. Saying "not enough history" is more useful than a verdict
        -- nobody should act on.
        when history_days < 7 then 'insufficient_history'
        when (modal_price - median_30d) / nullif(median_30d, 0) >  0.40 then 'spike'
        when (modal_price - median_30d) / nullif(median_30d, 0) >  0.20 then 'elevated'
        when (modal_price - median_30d) / nullif(median_30d, 0) < -0.40 then 'collapse'
        when (modal_price - median_30d) / nullif(median_30d, 0) < -0.20 then 'depressed'
        else 'normal'
    end                                                         as verdict
from windowed
