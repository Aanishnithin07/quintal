{{ config(materialized='table') }}

-- Materialised as a TABLE, not a view, on purpose. A view would re-resolve the
-- bronze glob every time anyone queried it, and that path is relative to the
-- caller's working directory -- so the warehouse would only be usable from
-- inside transform/. Building it once decouples the published warehouse from
-- wherever dbt happened to run.

-- Bronze -> Silver.
--
-- Bronze is a faithful record of what the publisher sent, including its
-- mistakes. This is the first layer allowed to correct anything, and every
-- correction below is a response to something actually observed in the feed.

with raw as (

    select *
    from read_json_auto(
        '{{ var("bronze_glob") }}',
        filename = true,
        union_by_name = true      -- tolerate upstream adding fields mid-history
    )

), parsed as (

    select
        -- The partition directory is the source of truth for ingest date;
        -- it is assigned in IST by the capture step.
        cast(
            regexp_extract(filename, 'ingest_date=(\d{4}-\d{2}-\d{2})', 1)
            as date
        )                                                   as ingest_date,

        -- Upstream sends DD/MM/YYYY, not ISO. Parsed strictly: a value that
        -- does not match becomes null rather than a silently wrong date.
        try_strptime(trim(arrival_date), '%d/%m/%Y')::date   as arrival_date,

        nullif(trim(state), '')                             as state,
        nullif(trim(district), '')                          as district,
        nullif(trim(market), '')                            as market,

        -- Upstream is inconsistent about spacing before parentheses, e.g.
        -- 'Pointed gourd(Parval)' in `commodity` but 'Pointed gourd (Parval)'
        -- in `variety` -- in the same row. Normalise so the two join, but keep
        -- the raw value so nothing is silently rewritten.
        nullif(trim(commodity), '')                         as commodity_raw,
        nullif(trim(variety), '')                           as variety_raw,
        regexp_replace(
            regexp_replace(trim(commodity), '\s*\(\s*', ' ('), '\s+', ' ', 'g'
        )                                                   as commodity,
        regexp_replace(
            regexp_replace(trim(variety), '\s*\(\s*', ' ('), '\s+', ' ', 'g'
        )                                                   as variety,
        nullif(trim(grade), '')                             as grade,

        -- Prices are rupees per quintal. Zero and negative are not real
        -- prices, they are missing data wearing a number's clothes.
        nullif(greatest(try_cast(min_price   as double), 0), 0) as min_price,
        nullif(greatest(try_cast(max_price   as double), 0), 0) as max_price,
        nullif(greatest(try_cast(modal_price as double), 0), 0) as modal_price

    from raw

)

select
    -- Deterministic surrogate key over the full grain. Inline md5 rather than
    -- dbt_utils, to keep the transform layer dependency-free too.
    md5(concat_ws('|',
        ingest_date::varchar, coalesce(arrival_date::varchar, ''),
        coalesce(state, ''), coalesce(district, ''), coalesce(market, ''),
        coalesce(commodity, ''), coalesce(variety, ''), coalesce(grade, '')
    ))                                                      as price_key,
    ingest_date,
    arrival_date,
    state,
    district,
    market,
    commodity,
    variety,
    grade,
    commodity_raw,
    variety_raw,
    min_price,
    max_price,
    modal_price,

    -- Flags, not filters. Downstream decides what to exclude; silver only
    -- describes what it sees.
    (arrival_date is null)                                  as is_unparseable_date,
    (modal_price is null)                                   as is_missing_price,
    (min_price is not null and max_price is not null
        and min_price > max_price)                          as is_inverted_range,
    (modal_price is not null and min_price is not null
        and max_price is not null
        and (modal_price < min_price or modal_price > max_price))
                                                            as is_modal_outside_range,
    date_diff('day', arrival_date, ingest_date)             as reporting_lag_days

from parsed

-- One row per market x commodity x variety x grade x arrival_date per snapshot.
-- Offset paging over a live endpoint can re-serve rows; capture.py drops exact
-- duplicates, but a row updated *between* pages arrives twice with different
-- prices. Keep the highest modal price deterministically.
qualify row_number() over (
    partition by ingest_date, state, district, market,
                 commodity, variety, grade, arrival_date
    order by modal_price desc nulls last
) = 1
