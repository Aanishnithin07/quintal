-- A market cannot report a price for a day that has not happened yet.
-- Upstream has sent malformed dates before; this catches the class of error
-- where DD/MM/YYYY is misread as MM/DD/YYYY and lands in the future.
select ingest_date, arrival_date, count(*) as rows
from {{ ref('stg_mandi_prices') }}
where arrival_date > ingest_date + interval 1 day
group by 1, 2
