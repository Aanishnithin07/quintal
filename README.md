# Quintal

**An open, continuous historical archive of Indian agricultural commodity prices —
rebuilt daily, because the official source keeps no history.**

*Named for the quintal (100 kg), the unit every price in this dataset is quoted in.*

[![Daily mandi snapshot](https://github.com/Aanishnithin07/quintal/actions/workflows/capture.yml/badge.svg)](https://github.com/Aanishnithin07/quintal/actions/workflows/capture.yml)
[![Build and test warehouse](https://github.com/Aanishnithin07/quintal/actions/workflows/transform.yml/badge.svg)](https://github.com/Aanishnithin07/quintal/actions/workflows/transform.yml)

**[View the dashboard →](https://aanishnithin07.github.io/quintal/)**

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

## How it is built

Full reasoning for every non-obvious choice — including four bugs found by
running the pipeline rather than reasoning about it — is in
**[docs/DECISIONS.md](docs/DECISIONS.md)**.

## Architecture

```
  data.gov.in  (snapshot-only, no history)
       │
       │  ingest/capture.py ── stdlib only, zero dependencies
       ▼
  BRONZE   immutable daily snapshots + manifests        ← irreplaceable
       │
       │  dbt-duckdb ── parse, conform, dedupe, flag
       ▼
  SILVER   typed, deduplicated observations             ← rebuildable
       │
       │  dbt-duckdb ── star schema
       ▼
  GOLD     fct_daily_price · fct_price_anomaly
       │   dim_market · dim_commodity · dim_date
       │   dq_snapshot_health
       ▼
  SERVE    static dashboard (GitHub Pages)
           │
           └── fabric/ ── same logic as PySpark + a Direct Lake
                          semantic model, for when a capacity exists
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
git clone https://github.com/Aanishnithin07/quintal
cd quintal

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

- [x] Daily capture, scheduled, resumable, self-healing
- [x] Immutable bronze with per-snapshot integrity manifests
- [x] dbt silver/gold star schema, 39 tests passing in CI
- [x] Price-anomaly detection and per-capture health scorecard
- [x] Public dashboard, rebuilt from bronze on every change
- [x] Fabric deployment path, verified against dbt by equivalence
- [ ] Mirrored as a Hugging Face dataset
- [ ] Registered API key (see below — currently capture is rate-limited)

### Known limitation: the API key

Captures currently run on data.gov.in's public demo key, which is capped at
**10 rows per request** and rate-limits after roughly 25 requests. The first
scheduled run collected 587 of 9,912 available rows in 20 minutes before
upstream stopped responding, then preserved what it had and marked the day
resumable — which is the designed behaviour, but it is not a complete day.

A free registered key (email only, no payment details) removes the cap. Until
one is set as the `DATA_GOV_API_KEY` secret, daily snapshots will be partial.
See [docs/SETUP.md](docs/SETUP.md).

## Attribution

Source data © Government of India, published under the
[Government Open Data Licence – India][godl] via data.gov.in. This project is an
independent archive and is not affiliated with or endorsed by any government body.
