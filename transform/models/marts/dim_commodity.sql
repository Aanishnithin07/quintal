{{ config(materialized='table') }}

-- Grain: one row per commodity x variety x grade as traded.
--
-- `commodity_raw` is carried alongside the normalised name so that upstream's
-- inconsistent spacing stays auditable -- if someone questions a figure, the
-- exact string the publisher sent is still recoverable from here and bronze.

select
    md5(concat_ws('|', commodity, coalesce(variety, ''), coalesce(grade, '')))
                                                as commodity_key,
    commodity,
    variety,
    grade,
    any_value(commodity_raw)                    as commodity_raw_example,
    count(distinct market)                      as markets_trading,
    count(distinct arrival_date)                as days_traded,
    count(*)                                    as observations,
    round(median(modal_price), 2)               as median_modal_price,
    min(modal_price)                            as min_modal_price,
    max(modal_price)                            as max_modal_price
from {{ ref('stg_mandi_prices') }}
where commodity is not null
group by commodity, variety, grade
