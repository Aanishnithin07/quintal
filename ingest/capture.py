#!/usr/bin/env python3
"""
Capture one daily snapshot of Indian mandi (agricultural market) prices.

WHY THIS FILE HAS NO DEPENDENCIES
---------------------------------
The upstream resource (data.gov.in 9ef84268-d588-465a-a308-a864a43d0070) is
snapshot-only: it exposes *today's* prices and offers no way to query the past.
A day we fail to capture is a day lost permanently, for everyone, forever.

Every other stage in this project can be re-run from the bronze files this
writes. This stage cannot be re-run at all. So it depends on nothing but the
Python standard library -- no requests, no pandas, no cloud SDK -- and it
degrades loudly rather than silently.

Output (append-only, never mutated):
    data/bronze/ingest_date=YYYY-MM-DD/records.jsonl.gz
    data/bronze/ingest_date=YYYY-MM-DD/_manifest.json
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

# Public sample key published by data.gov.in. Hard-capped at 10 rows/call.
# A free registered key (email only, no card) lifts the cap dramatically.
DEMO_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

# Mandi days are Indian days. Never let the runner's timezone decide which
# calendar day a snapshot belongs to.
IST = timezone(timedelta(hours=5, minutes=30))

# Runaway guard. A day is ~9k rows; if we ever page past this, something is
# badly wrong upstream and we should stop rather than loop forever.
HARD_OFFSET_CAP = 200_000

USER_AGENT = "mandi-archive/1.0 (open agricultural price archive; +https://github.com/Aanishnithin07)"

EXPECTED_FIELDS = (
    "state", "district", "market", "commodity", "variety", "grade",
    "arrival_date", "min_price", "max_price", "modal_price",
)


class CaptureError(RuntimeError):
    """Fatal, non-retryable problem with the capture run."""


class RateLimited(RuntimeError):
    """Upstream returned 429. Retryable, but only with real patience."""


def _get(url: str, timeout: int, attempts: int = 5,
         rl_backoff: tuple[int, ...] = (20, 45, 90)) -> dict:
    """GET with backoff.

    Three distinct failure classes, deliberately handled differently:
      * 4xx (not 429) -- our fault (bad key/resource). Retrying cannot help.
      * 429           -- rate limited. Retryable, but needs patience, not speed.
      * 5xx/transport -- transient. Ordinary exponential backoff.
    """
    last: Exception | None = None
    rl_hits = 0
    i = 0
    while i < attempts:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if rl_hits >= len(rl_backoff):
                    raise RateLimited(f"still rate limited after {rl_hits} waits") from e
                # Respect Retry-After when the server bothers to send one.
                try:
                    wait = int(e.headers.get("Retry-After") or 0)
                except (TypeError, ValueError):
                    wait = 0
                wait = max(wait, rl_backoff[rl_hits])
                rl_hits += 1
                print(f"  ! 429 rate limited; waiting {wait}s "
                      f"({rl_hits}/{len(rl_backoff)})", file=sys.stderr, flush=True)
                time.sleep(wait)
                continue  # a rate-limit wait must not consume a normal attempt
            if 400 <= e.code < 500:
                raise CaptureError(f"HTTP {e.code} from upstream: {e.reason}") from e
            last = e
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as e:
            last = e
        i += 1
        if i < attempts:
            delay = 2.0 * (2 ** i)
            print(f"  ! attempt {i}/{attempts} failed ({type(last).__name__}); "
                  f"retrying in {delay:.0f}s", file=sys.stderr, flush=True)
            time.sleep(delay)
    raise CaptureError(f"upstream unreachable after {attempts} attempts: {last!r}")


def _page_url(key: str, limit: int, offset: int) -> str:
    q = urllib.parse.urlencode(
        {"api-key": key, "format": "json", "limit": limit, "offset": offset}
    )
    return f"{BASE_URL}?{q}"


def _load_partial(out_dir: Path) -> list[dict]:
    """Re-read a previous incomplete attempt so we can continue it."""
    f = out_dir / "records.jsonl.gz"
    if not f.exists():
        return []
    try:
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
    except (OSError, json.JSONDecodeError) as e:
        print(f"  ! could not read prior partial ({e}); starting fresh", file=sys.stderr)
        return []


def _dedupe(records: list[dict]) -> tuple[list[dict], int]:
    """Drop exact duplicates, preserving first-seen order.

    Offset pagination over a live endpoint can re-serve rows across a resume
    boundary. Those duplicates are an artefact of *our* retry process, not
    something the publisher did, so removing them keeps bronze faithful.
    """
    seen, out = set(), []
    for r in records:
        k = json.dumps(r, sort_keys=True, ensure_ascii=False)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out, len(records) - len(out)


def capture(key: str, page_size: int, timeout: int, max_pages: int | None,
            delay: float, existing: list[dict]) -> tuple[list[dict], dict]:
    """Page through the snapshot, resuming from `existing`.

    This function does not raise on upstream failure. Whatever has been
    collected is always returned, because for a snapshot-only source a partial
    capture is worth immeasurably more than a clean exception.
    """
    started = time.monotonic()
    records = list(existing)
    stopped = None
    exhausted = False

    try:
        first = _get(_page_url(key, page_size, 0), timeout)
        total = int(first.get("total") or 0)
        head = list(first.get("records") or [])
        if not head:
            raise CaptureError("upstream returned zero records on the first page")

        # Trust what the server actually returned, not what we asked for: the
        # demo key silently caps at 10 however large a `limit` we send, and
        # assuming otherwise would skip most of the snapshot while appearing fine.
        effective = len(head)
        if effective < page_size:
            print(f"  i server capped page size at {effective} (asked {page_size})")
        print(f"  i upstream total={total}, publisher updated={first.get('updated_date')}")

        if records:
            print(f"  i resuming from {len(records)} rows captured earlier")
            offset = len(records)
        else:
            records = head
            offset = effective

        # Termination is by EXHAUSTION, never by row count.
        #
        # `total` is not a reliable target. It grows through the day as markets
        # report in (observed 9220 -> 9359 within an hour), and a read taken
        # while the publisher is rebuilding its index can transiently *deflate*
        # it (observed 4000). Stopping at `rows >= total` would therefore have
        # silently truncated the day and called it a success -- the worst
        # outcome available to us, since the loss is permanent.
        #
        # So we page until upstream stops handing us records, and treat `total`
        # as advisory only, ratcheted upward so a bad read cannot lower it.
        pages, empties = 0, 0
        while offset < HARD_OFFSET_CAP:
            if max_pages is not None and pages >= max_pages:
                stopped = f"--max-pages={max_pages}"
                break
            if delay:
                time.sleep(delay)
            page = _get(_page_url(key, page_size, offset), timeout)
            got = list(page.get("records") or [])
            pages += 1
            total = max(total, int(page.get("total") or 0))
            if not got:
                empties += 1
                if empties >= 2:
                    exhausted = True
                    print(f"  i upstream exhausted at offset={offset} "
                          f"(total reported {total})")
                    break
                offset += effective or 10
                continue
            empties = 0
            records.extend(got)
            offset += len(got)
            if pages % 25 == 0:
                pct = f"{100.0*offset/total:.0f}%" if total else "?"
                print(f"    .. {offset}/{total} ({pct})", flush=True)
        else:
            stopped = f"hit safety cap at offset={offset}"

    except (CaptureError, RateLimited) as e:
        # Keep everything fetched so far; the next scheduled run resumes it.
        stopped = f"{type(e).__name__}: {e}"
        print(f"  ! halted: {stopped}", file=sys.stderr)
        total = locals().get("total", 0)
        effective = locals().get("effective", 0)

    records, dupes = _dedupe(records)
    meta = {
        "total_reported_by_upstream": total,
        "rows_captured": len(records),
        "duplicates_removed": dupes,
        "publisher_updated_date": locals().get("first", {}).get("updated_date"),
        "effective_page_size": effective,
        "duration_seconds": round(time.monotonic() - started, 1),
        "max_total_seen": total,
        "stopped_because": stopped,
        # Complete means "upstream ran out of rows", not "we hit a number".
        "complete": exhausted and stopped is None,
    }
    return records, meta


def validate(records: list[dict]) -> dict:
    """Shape checks. We record problems rather than dropping rows -- bronze is
    a faithful record of what the publisher said, including when it was wrong."""
    missing_fields = sorted(
        {f for r in records for f in EXPECTED_FIELDS if f not in r}
    )
    unknown_fields = sorted(
        {k for r in records for k in r if k not in EXPECTED_FIELDS}
    )
    arrival_dates: dict[str, int] = {}
    nonnumeric = 0
    for r in records:
        arrival_dates[str(r.get("arrival_date"))] = arrival_dates.get(str(r.get("arrival_date")), 0) + 1
        for p in ("min_price", "max_price", "modal_price"):
            v = r.get(p)
            if v is None or str(v).strip() in ("", "NA", "-"):
                nonnumeric += 1
    return {
        "missing_expected_fields": missing_fields,
        "unexpected_new_fields": unknown_fields,   # schema drift alarm
        "distinct_arrival_dates": len(arrival_dates),
        "arrival_date_histogram": dict(sorted(arrival_dates.items(), key=lambda kv: -kv[1])[:10]),
        "blank_price_cells": nonnumeric,
        "distinct_states": len({r.get("state") for r in records}),
        "distinct_markets": len({(r.get("state"), r.get("market")) for r in records}),
        "distinct_commodities": len({r.get("commodity") for r in records}),
    }


def write_snapshot(root: Path, day: str, records: list[dict], meta: dict) -> Path:
    out_dir = root / "data" / "bronze" / f"ingest_date={day}"
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / "records.jsonl.gz"

    # Deterministic bytes: stable key order so re-running produces an identical
    # digest, which makes the checksum a real integrity check.
    payload = "\n".join(
        json.dumps(r, sort_keys=True, ensure_ascii=False) for r in records
    ).encode("utf-8")
    # mtime=0 so gzip framing does not change between runs either.
    with gzip.GzipFile(filename="", mode="wb", fileobj=open(data_path, "wb"), mtime=0) as gz:
        gz.write(payload)

    manifest = {
        "ingest_date_ist": day,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "resource_id": RESOURCE_ID,
        "schema_version": 1,
        "sha256_uncompressed": hashlib.sha256(payload).hexdigest(),
        "bytes_uncompressed": len(payload),
        "bytes_on_disk": data_path.stat().st_size,
        **meta,
        "quality": validate(records),
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return data_path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--page-size", type=int, default=1000)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--max-pages", type=int, default=None,
                    help="cap pages (testing only; marks snapshot incomplete)")
    ap.add_argument("--delay", type=float, default=0.4,
                    help="seconds between requests; politeness throttle")
    ap.add_argument("--force", action="store_true",
                    help="overwrite an existing complete snapshot for today")
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    args = ap.parse_args()

    root = Path(args.root)
    day = datetime.now(IST).strftime("%Y-%m-%d")
    existing = root / "data" / "bronze" / f"ingest_date={day}" / "_manifest.json"

    out_dir = root / "data" / "bronze" / f"ingest_date={day}"
    key = os.environ.get("DATA_GOV_API_KEY", "").strip() or DEMO_KEY
    mode = "registered" if key != DEMO_KEY else "demo(10-row cap)"

    prior_rows: list[dict] = []
    if existing.exists() and not args.force:
        prior = json.loads(existing.read_text())
        prior_rows = _load_partial(out_dir)
        if prior.get("complete"):
            # Even a complete snapshot is topped up while the day is still
            # running: markets report late, so the published set grows.
            # One cheap call decides whether there is anything new.
            try:
                now_total = int(_get(_page_url(key, 1, 0), args.timeout).get("total") or 0)
            except (CaptureError, RateLimited):
                now_total = 0
            if now_total and now_total <= prior.get("max_total_seen", 0):
                print(f"[skip] {day} complete at {prior.get('rows_captured')} rows; "
                      f"upstream has not grown (total={now_total})")
                return 0
            print(f"[top-up] upstream grew {prior.get('max_total_seen')} -> {now_total}; "
                  f"re-paging to collect late reporters")
            # Re-page from offset 0; _dedupe merges the result with what we
            # already hold, so late reporters are added without duplication.
            prior_rows = []
        else:
            print(f"[resume] {day} incomplete "
                  f"({prior.get('rows_captured')}/{prior.get('max_total_seen', '?')}); continuing")

    print(f"[capture] {day} IST  key={mode}  page_size={args.page_size}")

    records, meta = capture(key, args.page_size, args.timeout,
                            args.max_pages, args.delay, prior_rows)
    if not records:
        print("[FAIL] captured nothing at all", file=sys.stderr)
        return 1

    meta["api_key_mode"] = mode
    path = write_snapshot(root, day, records, meta)
    q = validate(records)

    print(f"[ok] {meta['rows_captured']} rows -> {path}")
    print(f"     {q['distinct_markets']} markets | {q['distinct_commodities']} commodities "
          f"| {q['distinct_states']} states | {meta['duration_seconds']}s")
    if q["unexpected_new_fields"]:
        print(f"[SCHEMA DRIFT] new upstream fields: {q['unexpected_new_fields']}", file=sys.stderr)
    if meta["duplicates_removed"]:
        print(f"     deduped {meta['duplicates_removed']} rows re-served across resume")
    if not meta["complete"]:
        print(f"[warn] snapshot INCOMPLETE "
              f"({meta['rows_captured']}/{meta['max_total_seen']}) "
              f"- {meta['stopped_because']}; next run resumes", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
