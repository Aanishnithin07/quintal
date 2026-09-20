# Setup

## 1. Get a free data.gov.in API key (required)

The public demo key is capped at **10 rows per request** *and* rate-limited to
roughly 25 requests before returning `429`. A full daily snapshot is ~9,300 rows,
so the demo key cannot capture a complete day — it is only useful for smoke tests.

Registration is free, needs an email address only, and asks for **no payment
details**:

1. Register at <https://data.gov.in/user/register>
2. Confirm the email, sign in, open your profile
3. Copy the API key shown there

Then add it to the repository so the scheduled job can use it:

```bash
gh secret set DATA_GOV_API_KEY --body "<your-key>"
```

And for local runs:

```bash
export DATA_GOV_API_KEY="<your-key>"
python3 ingest/capture.py
```

## 2. Verify the schedule is live

```bash
gh workflow list
gh run list --workflow=capture.yml --limit 5
```

GitHub disables scheduled workflows after 60 days without repository activity.
This pipeline commits a snapshot every day, which counts as activity, so the
schedule sustains itself as long as captures keep succeeding.

## 3. Operational notes

| Observed behaviour | Consequence for the design |
|---|---|
| Publisher refreshes **hourly**, not daily | The job runs 3×/day and tops up an already-"complete" day |
| `total` grows through the day (9220 → 9359 observed in one hour) | `total` is advisory only; it is ratcheted upward and never trusted downward |
| `total` can transiently *deflate* mid-refresh (4000 observed) | Completeness is decided by **upstream exhaustion**, never by row count |
| Demo key returns `429` after ~25 requests | `429` is retried with `Retry-After`; partial progress is always preserved and resumed |
| Offset paging over live data re-serves rows | Exact-duplicate rows are dropped at write time and the count recorded |

## 4. Recovering a failed day

Captures resume automatically — the next scheduled run continues from the rows
already on disk. To push it manually:

```bash
gh workflow run capture.yml            # resume today
gh workflow run capture.yml -f force=true   # start today over
```

A day that was missed entirely **cannot be recovered.** The upstream resource
has no history. This is the constraint the whole project is built around.
