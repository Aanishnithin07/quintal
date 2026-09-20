# Design decisions

Every entry below is a response to something observed in the live feed, not a
preference. Where a decision was made *because* an earlier version was wrong,
the failure is recorded rather than quietly fixed.

---

## 1. The capture step has no dependencies

**Decision.** `ingest/capture.py` imports only the Python standard library.

**Why.** The upstream resource is snapshot-only. Every other stage can be
re-run from bronze; this one cannot be re-run at all. A transitive dependency
breaking its release can cost a production outage anywhere else in software —
here it costs a day of history that no one can ever recover.

Robustness is spent where loss is irreversible. The transform layer, which is
fully replayable, uses dbt and DuckDB freely.

---

## 2. Completeness is exhaustion-based, not count-based

**Decision.** A capture is complete when upstream stops returning records — not
when the row count reaches the reported `total`.

**Why.** The first version compared `rows >= total`. Observation over one hour:

| Time | `total` |
|---|---|
| 13:00 UTC | 9,220 |
| ~13:40 UTC | **4,000** |
| 14:00 UTC | 9,359 |

`total` grows through the day as markets report in, and a read taken while the
publisher rebuilds its index transiently *deflates*. Had a capture been running
during that 4,000 reading with 4,500 rows already collected, it would have
declared the day complete, stopped, and reported success — losing roughly 5,000
rows permanently, with no error anywhere.

Silent truncation that reports success is the worst failure mode available to
an archive. `total` is now advisory only, ratcheted upward so a bad read cannot
lower it.

---

## 3. Partial progress is always kept

**Decision.** `capture()` never raises. Whatever was collected is written, and
the next scheduled run resumes from it.

**Why.** The first version raised on upstream failure, which discarded every row
already fetched. Observed in practice: a `429` after ~250 rows threw away all
250. For a source that cannot be re-read, a partial capture is worth
immeasurably more than a clean exception.

Rows re-served across a resume boundary are deduplicated at write time, since
those duplicates are an artefact of our retry process rather than something the
publisher did.

---

## 4. `429` is retryable; other 4xx are not

**Decision.** Three failure classes, handled differently: `4xx` (fatal), `429`
(retry with `Retry-After` and long waits), `5xx`/transport (ordinary backoff).

**Why.** The first version treated all `4xx` as non-retryable — correct for a
bad API key, wrong for a rate limit, which is a request to wait rather than a
refusal. A rate-limit wait does not consume a retry attempt, because doing so
conflates "the server is busy" with "the server is broken".

---

## 5. Silver flags problems; it does not filter them

**Decision.** `is_missing_price`, `is_inverted_range`, `is_modal_outside_range`
and `is_unparseable_date` are columns. No row is dropped in silver.

**Why.** Bronze records what the publisher said, including when it was wrong.
If silver filtered, the archive would quietly disagree with the official source
and no one could tell where the difference came from. The marts decide what to
exclude, and `dq_snapshot_health` reports how much was excluded and when.

---

## 6. `dim_market` is not an SCD2

**Decision.** Observed reporting lifespan (`first_reported_on`,
`last_reported_on`, `is_actively_reporting`) instead of type-2 history.

**Why.** There is no stable market identifier upstream — the name *is* the
business key. A renamed market and a brand-new market are therefore
indistinguishable, and an SCD2 over an unstable key manufactures history that
did not happen. A dimension that looks sophisticated while being wrong is worse
than a simpler one that is right.

Lifespan tracking also addresses the feed's actual dominant problem: markets
that stop reporting without announcement.

---

## 7. Staging is a table, not a view

**Decision.** `stg_mandi_prices` is materialised.

**Why.** As a view it re-resolved the bronze glob on every query, and that path
is relative to the *caller's* working directory — so the published warehouse
only worked when queried from inside `transform/`. Building once decouples the
warehouse from wherever dbt happened to run.

---

## 8. Anomalies are measured within a market, not across markets

**Decision.** Each observation is compared to its own market's trailing 30-day
median, excluding the current day.

**Why.** Two rejected alternatives:

- *Cross-market benchmark.* A tomato price in Kolar is not comparable to one in
  Ludhiana — different varieties, transport costs, buyers. That baseline flags
  ordinary regional variation as anomalous every day, and an alert that fires
  constantly gets muted within a week.
- *Including the current day in the window.* The baseline pulls toward the value
  being tested, muting exactly the spike the model exists to catch.

Median rather than mean, because a single premium lot moves a mean enough to
hide a real spike. Under 7 days of history the verdict is
`insufficient_history` rather than a number nobody should act on.

---

## 9. Fabric is a deployment target, not a dependency

**Decision.** The archive runs on DuckDB and GitHub Actions. Fabric artefacts
are maintained and verified by equivalence, but nothing depends on them.

**Why.** There is no free Fabric tier, and the 60-day trial needs a work tenant.
A project whose warehouse lived in a trial capacity would lose both the
warehouse and the daily capture when the trial expired — precisely when the
archive had finally accumulated enough history to be worth something.

`fabric/tests/test_spark_dbt_equivalence.py` executes the notebook's own source
over local Spark and requires byte-identical output to dbt's. It execs the real
notebook rather than a copy, because a copy drifts silently and would report
agreement between two things that are no longer the same code.
