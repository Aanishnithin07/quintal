# Mandi Archive

**An open, continuous historical archive of Indian agricultural commodity prices —
rebuilt daily, because the official source keeps no history.**

[![Daily mandi snapshot](https://github.com/Aanishnithin07/mandi-archive/actions/workflows/capture.yml/badge.svg)](https://github.com/Aanishnithin07/mandi-archive/actions/workflows/capture.yml)

---

## Why this exists

The Government of India publishes daily prices from ~3,000 *mandis* (regulated
agricultural markets) through [data.gov.in][res]. It is genuinely valuable data:
roughly **9,200 price observations per day**, covering every state, hundreds of
commodities, and the min/max/modal price at each market.

It has one problem. **The endpoint is snapshot-only.** It serves today's prices
and offers no way to ask for yesterday's. There is no date range parameter, no
archive, no bulk download. When the publisher refreshes at ~18:30 IST, the
previous day is simply gone.

The consequence is that a dataset the public paid for, and which agricultural
economists, journalists, and food-policy researchers genuinely need, **does not
accumulate anywhere**. Everyone who wants a time series either scrapes it badly
themselves or does without.

This repository captures that snapshot every day and keeps it forever, in an
open format, with full provenance.

[res]: https://data.gov.in/resource/current-daily-price-various-commodities-various-markets-mandi

## What you get

| | |
|---|---|
| **Coverage begins** | 2026-09-20 |
| **Snapshot cadence** | Daily, ~9,200 observations |
| **Format** | Newline-delimited JSON, gzipped, date-partitioned |
| **Footprint** | ~270 KB/day compressed (~100 MB/year) |
| **Provenance** | SHA-256 + row counts + publisher timestamp per snapshot |
| **Licence** | [GODL-India][godl] (source), archive structure CC-BY-4.0 |

[godl]: https://data.gov.in/government-open-data-license-india

```
data/bronze/ingest_date=2026-09-20/
├── records.jsonl.gz     # exactly what the publisher returned, unmodified
└── _manifest.json       # checksum, row counts, quality report, run metadata
```

Bronze is **append-only and never rewritten**. If the publisher sends something
malformed, that is preserved too — the archive records what was actually
published, not a cleaned-up version of it. Corrections belong downstream.

## Architecture

```
  data.gov.in  (snapshot-only, no history)
       │
       │  ingest/capture.py ── stdlib only, zero dependencies
       ▼
  BRONZE   immutable daily snapshots + manifests        ← irreplaceable
       │
       │  dbt-duckdb ── parse, conform, dedupe, SCD2
       ▼
  SILVER   typed, deduplicated observations             ← rebuildable
       │
       │  dbt-duckdb ── star schema
       ▼
  GOLD     fct_daily_price · dim_market · dim_commodity
       │   dim_date · dim_geography
       ▼
  SERVE    Evidence.dev dashboard · published dataset
```

Orchestrated by GitHub Actions on a thrice-daily cron. The
[run history](../../actions/workflows/capture.yml) is the operational record —
every capture, every failure, publicly timestamped.

### Why the capture step has no dependencies

Every stage downstream of bronze can be re-run from bronze. The capture stage
cannot be re-run *at all* — miss a day and it is gone for everyone, permanently.

So `ingest/capture.py` imports nothing outside the Python standard library. No
`requests`, no `pandas`, no cloud SDK. A transitive dependency breaking its
release cannot cost the archive a day of history. The transform layer, which is
fully replayable, is free to use whatever is convenient.

This is the central design trade-off of the project: **robustness is spent where
loss is irreversible, and convenience is spent everywhere else.**

## Data dictionary

| Field | Type | Notes |
|---|---|---|
| `state`, `district`, `market` | string | Free text. Naming drifts over time — handled by SCD2 in `dim_market`. |
| `commodity`, `variety`, `grade` | string | Inconsistent spacing upstream, e.g. `Pointed gourd(Parval)` vs `Pointed gourd (Parval)`. |
| `arrival_date` | string | **`DD/MM/YYYY`**, not ISO. May lag the ingest date when a market reports late. |
| `min_price`, `max_price`, `modal_price` | number | ₹ per quintal (100 kg). Occasionally blank or zero. |

Partitioning is by **`ingest_date` (IST), not `arrival_date`** — a snapshot is a
record of *what was published on a given day*, and late-reporting markets mean
the two genuinely differ. Keeping both preserves the distinction.

## Using the archive

```bash
git clone https://github.com/Aanishnithin07/mandi-archive
cd mandi-archive

# Every observation ever captured, as one table
python3 -c "
import duckdb
duckdb.sql('''
  SELECT ingest_date, state, commodity, modal_price
  FROM read_json_auto('data/bronze/*/records.jsonl.gz', filename=true)
  LIMIT 10
''').show()"
```

## Running it yourself

```bash
python3 ingest/capture.py           # captures today; no-ops if already complete
python3 ingest/capture.py --force   # re-capture
```

The public demo key is capped at 10 rows per request, so a full snapshot takes
~10 minutes. A **free** registered key from [data.gov.in][key] (email only, no
payment details) lifts the cap and cuts this to seconds. Set it as the
`DATA_GOV_API_KEY` repository secret.

[key]: https://data.gov.in/user/register

## Status

- [x] Daily capture, scheduled and self-healing
- [x] Immutable bronze with per-snapshot integrity manifests
- [ ] dbt silver/gold star schema
- [ ] Price-anomaly detection and data-quality scorecard
- [ ] Public dashboard
- [ ] Mirrored as a Hugging Face dataset

## Attribution

Source data © Government of India, published under the
[Government Open Data Licence – India][godl] via data.gov.in. This project is an
independent archive and is not affiliated with or endorsed by any government body.
