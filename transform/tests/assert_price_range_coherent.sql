-- min <= modal <= max must hold wherever all three are present.
-- Failures here are upstream's, not ours -- this test exists so that when the
-- number is wrong we can say precisely how many rows and on which days,
-- instead of discovering it in a chart.
select ingest_date, count(*) as incoherent_rows
from {{ ref('stg_mandi_prices') }}
where is_inverted_range or is_modal_outside_range
group by 1
having count(*) > 50    -- a handful is normal upstream noise; a wave is not
