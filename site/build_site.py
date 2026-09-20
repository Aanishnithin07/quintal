#!/usr/bin/env python3
"""
Render the archive's public dashboard from the DuckDB warehouse.

Deliberately generates a single self-contained HTML file with inline SVG and no
chart library. The dashboard is the one part of this project that is purely
presentational -- it can be rebuilt from gold at any time -- but it is also the
part most likely to rot, because a JS toolchain left unattended for a year does
not still build. A static generator with no dependencies beyond duckdb will.

    python site/build_site.py            # -> site/index.html
"""

from __future__ import annotations

import datetime as dt
import html
import pathlib
import sys

import duckdb

ROOT = pathlib.Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
OUT = ROOT / "site" / "index.html"

# Palette roles. Light and dark are both *selected* steps, not an auto-flip.
PALETTE = {
    "series1":   ("#2a78d6", "#3987e5"),
    "good":      ("#0ca30c", "#0ca30c"),
    "warning":   ("#fab219", "#fab219"),
    "critical":  ("#d03b3b", "#d03b3b"),
    "surface":   ("#fcfcfb", "#1a1a19"),
    "plane":     ("#f9f9f7", "#0d0d0d"),
    "ink":       ("#0b0b0b", "#ffffff"),
    "ink2":      ("#52514e", "#c3c2b7"),
    "muted":     ("#898781", "#898781"),
    "grid":      ("#e1e0d9", "#2c2c2a"),
    "axis":      ("#c3c2b7", "#383835"),
    "border":    ("rgba(11,11,11,0.10)", "rgba(255,255,255,0.10)"),
}

VERDICT_STATUS = {
    "spike": "critical", "collapse": "critical",
    "elevated": "warning", "depressed": "warning",
    "normal": "good", "insufficient_history": "muted",
}


def e(x) -> str:
    return html.escape(str(x), quote=True)


def fmt(n, dp=0) -> str:
    if n is None:
        return "—"
    return f"{n:,.{dp}f}"


# ---------------------------------------------------------------- queries

def gather(con) -> dict:
    d = {}
    d["totals"] = con.sql("""
        select count(*)                       as observations,
               count(distinct arrival_date)   as market_days,
               count(distinct market_key)     as markets,
               count(distinct commodity_key)  as commodities
        from gold.fct_daily_price
    """).fetchone()

    d["captures"] = con.sql("""
        select ingest_date, rows_captured, markets_reporting,
               commodities, states, missing_price_pct, volume_verdict
        from gold.dq_snapshot_health order by ingest_date
    """).fetchall()

    d["spread_hist"] = con.sql("""
        with b as (
          select case
            when price_spread_pct = 0 then '0%'
            when price_spread_pct <= 5 then '0-5%'
            when price_spread_pct <= 10 then '5-10%'
            when price_spread_pct <= 20 then '10-20%'
            when price_spread_pct <= 35 then '20-35%'
            when price_spread_pct <= 50 then '35-50%'
            else '50%+' end as bucket,
            case
            when price_spread_pct = 0 then 0
            when price_spread_pct <= 5 then 1
            when price_spread_pct <= 10 then 2
            when price_spread_pct <= 20 then 3
            when price_spread_pct <= 35 then 4
            when price_spread_pct <= 50 then 5
            else 6 end as ord
          from gold.fct_daily_price where price_spread_pct is not null
        )
        select bucket, count(*) as n from b group by bucket, ord order by ord
    """).fetchall()

    d["top_commodities"] = con.sql("""
        select c.commodity, count(distinct f.market_key) as markets,
               round(median(f.modal_price)) as median_price
        from gold.fct_daily_price f join gold.dim_commodity c using (commodity_key)
        where f.modal_price is not null
        group by c.commodity order by markets desc, median_price desc limit 10
    """).fetchall()

    d["anomalies"] = con.sql("""
        select arrival_date, state, market, commodity, modal_price,
               median_30d, deviation_pct, verdict
        from gold.fct_price_anomaly
        where verdict not in ('normal','insufficient_history')
        order by abs(deviation_pct) desc limit 20
    """).fetchall()

    d["anomaly_state"] = con.sql("""
        select verdict, count(*) from gold.fct_price_anomaly group by 1
    """).fetchall()

    d["widest"] = con.sql("""
        select m.state, m.market, c.commodity, f.min_price, f.max_price, f.price_spread_pct
        from gold.fct_daily_price f
        join gold.dim_market m using (market_key)
        join gold.dim_commodity c using (commodity_key)
        where f.price_spread_pct is not null
        order by f.price_spread_pct desc limit 10
    """).fetchall()
    return d


# ---------------------------------------------------------------- charts

def bar_chart(rows, label_i, value_i, *, width=720, bar_h=28, gap=10,
              value_fmt=lambda v: fmt(v), status_i=None) -> str:
    """Horizontal bars. Horizontal because the category labels are long market
    and commodity names -- rotated x-axis labels are the single most common way
    a chart like this becomes unreadable."""
    if not rows:
        return '<p class="empty">No data yet.</p>'
    label_w, pad_r = 190, 92
    plot_w = width - label_w - pad_r
    vmax = max((r[value_i] or 0) for r in rows) or 1
    h = len(rows) * (bar_h + gap)
    out = [f'<svg viewBox="0 0 {width} {h}" width="100%" height="{h}" role="img" '
           f'aria-label="Bar chart">']
    for i, r in enumerate(rows):
        y = i * (bar_h + gap)
        v = r[value_i] or 0
        w = max(2, plot_w * v / vmax)
        colour = "var(--series1)"
        if status_i is not None:
            colour = f"var(--{VERDICT_STATUS.get(r[status_i], 'muted')})"
        out.append(
            f'<text x="{label_w - 12}" y="{y + bar_h/2 + 4}" text-anchor="end" '
            f'class="cat">{e(str(r[label_i])[:30])}</text>'
            # 4px rounded data-end, anchored square to the baseline.
            f'<rect x="{label_w}" y="{y}" width="{w:.1f}" height="{bar_h}" rx="4" '
            f'fill="{colour}"><title>{e(r[label_i])}: {e(value_fmt(v))}</title></rect>'
            f'<text x="{label_w + w + 10}" y="{y + bar_h/2 + 4}" class="val">'
            f'{e(value_fmt(v))}</text>')
    out.append("</svg>")
    return "".join(out)


def capture_timeline(captures) -> str:
    """Rows captured per day. Under three days there is nothing a time axis can
    show that the number itself does not, so say so instead of drawing one."""
    if len(captures) < 3:
        n = len(captures)
        day = "day" if n == 1 else "days"
        return (f'<p class="empty"><strong>{n} {day} of history.</strong> '
                f'This chart appears once the archive has three days to compare. '
                f'The source publishes no history, so it can only accumulate '
                f'forward from first capture.</p>')
    w, h, pad_l, pad_b, pad_t = 720, 220, 48, 28, 12
    vals = [c[1] for c in captures]
    vmax = max(vals) or 1
    n = len(captures)
    bw = (w - pad_l - 8) / n
    out = [f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" role="img" '
           f'aria-label="Rows captured per day">']
    for g in range(5):
        gv = vmax * g / 4
        gy = h - pad_b - (h - pad_b - pad_t) * g / 4
        out.append(f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{w-8}" y2="{gy:.1f}" class="grid"/>'
                   f'<text x="{pad_l-8}" y="{gy+4:.1f}" text-anchor="end" class="tick">'
                   f'{fmt(gv)}</text>')
    flagged = 0
    for i, c in enumerate(captures):
        bh = (h - pad_b - pad_t) * (c[1] / vmax)
        x = pad_l + i * bw
        odd = c[6] not in ("normal", "insufficient_history")
        colour = "var(--warning)" if odd else "var(--series1)"
        out.append(f'<rect x="{x+1.5:.1f}" y="{h-pad_b-bh:.1f}" width="{max(2,bw-3):.1f}" '
                   f'height="{bh:.1f}" rx="4" fill="{colour}">'
                   f'<title>{e(c[0])}: {fmt(c[1])} rows, {e(c[6])}</title></rect>')
        if odd:
            # A flagged day must not be distinguished by colour alone: the
            # warning step is sub-3:1 on the light surface, and a colourblind
            # reader would see an ordinary bar. The caret carries the meaning.
            flagged += 1
            out.append(f'<text x="{x + bw/2:.1f}" y="{h-pad_b-bh-6:.1f}" '
                       f'text-anchor="middle" class="flag">\u25B2</text>')
    out.append(f'<line x1="{pad_l}" y1="{h-pad_b}" x2="{w-8}" y2="{h-pad_b}" class="axis"/>')
    out.append(f'<text x="{pad_l}" y="{h-8}" class="tick">{e(captures[0][0])}</text>'
               f'<text x="{w-8}" y="{h-8}" text-anchor="end" class="tick">{e(captures[-1][0])}</text>')
    out.append("</svg>")
    if flagged:
        out.append(
            '<p class="legend"><span class="badge warning">\u25B2 volume outside '
            'trailing 7-day range</span> — the capture succeeded but collected '
            'an unusual number of rows.</p>')
    return "".join(out)


def table(headers, rows, cell_fmt=None) -> str:
    if not rows:
        return '<p class="empty">Nothing to show yet.</p>'
    th = "".join(f"<th>{e(x)}</th>" for x in headers)
    trs = []
    for r in rows:
        tds = []
        for i, v in enumerate(r):
            tds.append(cell_fmt(i, v) if cell_fmt else f"<td>{e(v)}</td>")
        trs.append("<tr>" + "".join(tds) + "</tr>")
    return f'<div class="tw"><table><thead><tr>{th}</tr></thead><tbody>{"".join(trs)}</tbody></table></div>'


# ---------------------------------------------------------------- page

def css() -> str:
    light = "".join(f"  --{k}: {v[0]};\n" for k, v in PALETTE.items())
    dark = "".join(f"  --{k}: {v[1]};\n" for k, v in PALETTE.items())
    return f"""
:root {{
  color-scheme: light;
{light}}}
/* Dark values are selected steps for the dark surface, not an automatic flip.
   Declared under both scopes: the media query follows the OS, the data-theme
   scope follows an explicit choice and must win either way. */
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    color-scheme: dark;
{dark}  }}
}}
:root[data-theme="dark"] {{
  color-scheme: dark;
{dark}}}

* {{ box-sizing: border-box; }}
body {{
  margin: 0; background: var(--plane); color: var(--ink);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  font-size: 15px; line-height: 1.55;
}}
.wrap {{ max-width: 960px; margin: 0 auto; padding: 40px 16px 72px; }}
header h1 {{ font-size: 1.9rem; margin: 0 0 6px; letter-spacing: -0.02em; }}
header p {{ margin: 0; color: var(--ink2); max-width: 62ch; }}
.stamp {{ font-size: 0.82rem; color: var(--muted); margin-top: 12px; }}

.kpis {{ display: grid; gap: 12px; margin: 32px 0 8px;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); }}
.kpi {{ background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 16px 18px; }}
.kpi .v {{ font-size: 1.85rem; font-weight: 600; letter-spacing: -0.02em; }}
.kpi .l {{ font-size: 0.8rem; color: var(--ink2); margin-top: 2px; }}

section {{ background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 22px 20px; margin-top: 20px; }}
section h2 {{ font-size: 1.05rem; margin: 0 0 4px; }}
section .sub {{ font-size: 0.85rem; color: var(--ink2); margin: 0 0 18px; max-width: 68ch; }}

svg text.cat {{ font-size: 12px; fill: var(--ink2); }}
svg text.val {{ font-size: 12px; fill: var(--ink2); font-variant-numeric: tabular-nums; }}
svg text.tick {{ font-size: 11px; fill: var(--muted); font-variant-numeric: tabular-nums; }}
svg text.flag {{ font-size: 10px; fill: var(--warning); }}
.legend {{ font-size: 0.82rem; color: var(--ink2); margin: 10px 0 0; }}
svg line.grid {{ stroke: var(--grid); stroke-width: 1; }}
svg line.axis {{ stroke: var(--axis); stroke-width: 1; }}
svg rect {{ transition: opacity .12s; }}
svg rect:hover {{ opacity: .78; }}

.tw {{ overflow-x: auto; }}
table {{ border-collapse: collapse; width: 100%; font-size: 0.87rem; }}
th {{ text-align: left; font-weight: 600; color: var(--ink2);
  border-bottom: 1px solid var(--axis); padding: 8px 10px; white-space: nowrap; }}
td {{ padding: 8px 10px; border-bottom: 1px solid var(--grid);
  font-variant-numeric: tabular-nums; }}
td.num {{ text-align: right; }}

.badge {{ display: inline-flex; align-items: center; gap: 5px;
  font-size: 0.78rem; font-weight: 600; padding: 2px 8px; border-radius: 999px;
  border: 1px solid var(--border); white-space: nowrap; }}
.badge::before {{ content: ""; width: 7px; height: 7px; border-radius: 50%;
  background: currentColor; }}
.badge.good {{ color: var(--good); }}
.badge.warning {{ color: var(--warning); }}
.badge.critical {{ color: var(--critical); }}
.badge.muted {{ color: var(--muted); }}

.empty {{ color: var(--ink2); background: var(--plane);
  border: 1px dashed var(--axis); border-radius: 8px; padding: 16px 18px;
  margin: 0; font-size: 0.9rem; }}
footer {{ margin-top: 36px; font-size: 0.8rem; color: var(--muted); }}
footer a {{ color: inherit; }}
@media (max-width: 560px) {{ .wrap {{ padding: 24px 16px 56px; }} header h1 {{ font-size: 1.5rem; }} }}
"""


def build(d: dict, generated: str) -> str:
    obs, mdays, markets, commodities = d["totals"]
    captures = d["captures"]
    latest = captures[-1] if captures else None

    def plural(n, one, many):
        return one if n == 1 else many

    kpis = [
        (fmt(obs), plural(obs, "price observation", "price observations")),
        (fmt(mdays), plural(mdays, "market-day archived", "market-days archived")),
        (fmt(markets), plural(markets, "market covered", "markets covered")),
        (fmt(commodities), plural(commodities, "commodity tracked", "commodities tracked")),
        (fmt(len(captures)), plural(len(captures), "daily capture", "daily captures")),
    ]
    kpi_html = "".join(
        f'<div class="kpi"><div class="v">{v}</div><div class="l">{e(l)}</div></div>'
        for v, l in kpis)

    def anomaly_cells(i, v):
        if i == 7:
            s = VERDICT_STATUS.get(v, "muted")
            return f'<td><span class="badge {s}">{e(v)}</span></td>'
        if i in (4, 5):                       # price, 30d median: whole rupees
            return f'<td class="num">{e(fmt(v) if v is not None else "—")}</td>'
        if i == 6:                            # deviation %: one decimal
            return f'<td class="num">{e(fmt(v, 1) if v is not None else "—")}</td>'
        return f"<td>{e(v)}</td>"

    anomaly_section = table(
        ["Date", "State", "Market", "Commodity", "Price", "30d median", "Deviation %", "Verdict"],
        d["anomalies"], anomaly_cells)
    if not d["anomalies"]:
        counts = dict(d["anomaly_state"])
        insuf = counts.get("insufficient_history", 0)
        anomaly_section = (
            f'<p class="empty">No anomalies flagged. '
            f'{fmt(insuf)} observations are marked <strong>insufficient_history</strong> — '
            f'each market needs seven days of its own prices before a deviation means '
            f'anything. The model reports that rather than inventing a verdict.</p>')

    def widest_cells(i, v):
        if i in (3, 4):                       # min/max price: whole rupees
            return f'<td class="num">{e(fmt(v))}</td>'
        if i == 5:                            # spread %: one decimal
            return f'<td class="num">{e(fmt(v, 1))}</td>'
        return f"<td>{e(v)}</td>"

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Quintal — Indian mandi prices</title>
<meta name="description" content="Open daily archive of Indian agricultural commodity prices.">
<style>{css()}</style>
</head>
<body>
<div class="wrap">
<header>
  <h1>Quintal</h1>
  <p>An open, continuous record of Indian agricultural commodity prices. The
     official source publishes only today's prices and keeps no history, so this
     captures that snapshot every day and preserves it.</p>
  <p class="stamp">Generated {e(generated)} ·
     latest capture {e(latest[0]) if latest else "—"} ·
     <a href="https://github.com/Aanishnithin07/quintal">source</a></p>
</header>

<div class="kpis">{kpi_html}</div>

<section>
  <h2>Capture history</h2>
  <p class="sub">Rows captured per day. The archive's real failure mode is not
     crashing but succeeding while quietly collecting less than it should, so
     each day is compared against its trailing seven-day average.</p>
  {capture_timeline(captures)}
</section>

<section>
  <h2>Price spread distribution</h2>
  <p class="sub">The gap between the lowest and highest price paid at the same
     market on the same day. A wide band means quality dispersion or thin
     trading — both matter to someone deciding where to sell.</p>
  {bar_chart(d["spread_hist"], 0, 1, value_fmt=lambda v: f"{fmt(v)} obs")}
</section>

<section>
  <h2>Most widely traded commodities</h2>
  <p class="sub">By number of distinct markets reporting a price.</p>
  {bar_chart(d["top_commodities"], 0, 1, value_fmt=lambda v: f"{fmt(v)} mkts")}
</section>

<section>
  <h2>Price anomalies</h2>
  <p class="sub">Each price against its own market's trailing 30-day median.
     Markets are never compared to each other — regional price differences are
     normal, and flagging them would bury the real signal.</p>
  {anomaly_section}
</section>

<section>
  <h2>Widest price bands today</h2>
  <p class="sub">Largest min-to-max gap at a single market, as a percentage of
     the minimum price.</p>
  {table(["State", "Market", "Commodity", "Min ₹", "Max ₹", "Spread %"],
         d["widest"], widest_cells)}
</section>

<footer>
  Source data © Government of India, published under the
  <a href="https://data.gov.in/government-open-data-license-india">Government Open
  Data Licence – India</a> via data.gov.in. This is an independent archive and is
  not affiliated with or endorsed by any government body. Prices are rupees per
  quintal (100&nbsp;kg), as published.
</footer>
</div>
</body>
</html>
"""


def main() -> int:
    if not WAREHOUSE.exists():
        print("warehouse missing -- run `cd transform && dbt build` first", file=sys.stderr)
        return 1
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    d = gather(con)
    stamp = dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))) \
              .strftime("%d %b %Y %H:%M IST")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(build(d, stamp), encoding="utf-8")
    print(f"wrote {OUT} ({OUT.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
