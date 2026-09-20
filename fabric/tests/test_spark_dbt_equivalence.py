#!/usr/bin/env python3
"""
Prove the Fabric notebook and the dbt model produce the same silver table.

The archive has no Fabric capacity to test against, so the Spark path cannot be
verified by running it in Fabric. It *can* be verified by equivalence: run the
notebook's own source on local Spark over the same bronze files, run dbt over
those same files, and require the two outputs to match exactly.

This executes the real notebook rather than a copy of it. A copy would drift
from the notebook silently, which would make the test worse than useless -- it
would report agreement between two things that are no longer the same code.

    .venv/bin/python fabric/tests/test_spark_dbt_equivalence.py
"""

from __future__ import annotations

import hashlib
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "fabric" / "notebooks" / "01_bronze_to_silver.py"
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
BRONZE_GLOB = str(ROOT / "data" / "bronze" / "ingest_date=*" / "records.jsonl.gz")

# Columns compared. Excludes price_key: both sides build it from the same
# inputs, so including it would let a shared upstream error cancel out.
COMPARE = [
    "ingest_date", "arrival_date", "state", "district", "market",
    "commodity", "variety", "grade", "min_price", "max_price", "modal_price",
    "is_unparseable_date", "is_missing_price", "is_inverted_range",
    "is_modal_outside_range", "reporting_lag_days",
]


def run_notebook_silver():
    """Exec the notebook's cells, minus the cell that writes to Delta."""
    cells = NOTEBOOK.read_text().split("# CELL ********************")
    ns: dict = {}
    for cell in cells:
        if ".saveAsTable(" in cell:      # skip persistence; we want the DataFrame
            continue
        # Point the notebook at local bronze instead of the Lakehouse path.
        cell = cell.replace(
            'BRONZE = "Files/bronze/ingest_date=*/records.jsonl.gz"',
            f'BRONZE = {BRONZE_GLOB!r}',
        )
        exec(compile(cell, str(NOTEBOOK), "exec"), ns)
    return ns["silver"]


def normalise(rows) -> list[tuple]:
    """Canonical form so the two engines' type quirks do not cause false diffs."""
    out = []
    for r in rows:
        norm = []
        for v in r:
            if v is None:
                norm.append("")
            elif isinstance(v, bool):
                norm.append("1" if v else "0")
            elif isinstance(v, float):
                norm.append(f"{v:.4f}")
            else:
                norm.append(str(v))
        out.append(tuple(norm))
    return sorted(out)


def digest(rows) -> str:
    h = hashlib.sha256()
    for r in rows:
        h.update(("\x1f".join(r) + "\x1e").encode("utf-8"))
    return h.hexdigest()


def main() -> int:
    import duckdb

    if not WAREHOUSE.exists():
        print("warehouse missing -- run `cd transform && dbt build` first", file=sys.stderr)
        return 1

    print("running dbt output ...")
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    dbt_rows = normalise(con.sql(f"select {', '.join(COMPARE)} from silver.stg_mandi_prices").fetchall())

    print("running notebook on local Spark ...")
    sdf = run_notebook_silver()
    spark_rows = normalise(sdf.select(*COMPARE).collect())

    print(f"\n  dbt   : {len(dbt_rows):,} rows  sha256 {digest(dbt_rows)[:16]}")
    print(f"  spark : {len(spark_rows):,} rows  sha256 {digest(spark_rows)[:16]}")

    if digest(dbt_rows) == digest(spark_rows):
        print("\nEQUIVALENT -- notebook and dbt model produce identical silver output")
        return 0

    print("\nMISMATCH", file=sys.stderr)
    only_dbt = set(dbt_rows) - set(spark_rows)
    only_spark = set(spark_rows) - set(dbt_rows)
    print(f"  rows only in dbt   : {len(only_dbt)}", file=sys.stderr)
    print(f"  rows only in spark : {len(only_spark)}", file=sys.stderr)
    for label, rows in (("dbt", only_dbt), ("spark", only_spark)):
        for r in list(rows)[:3]:
            print(f"    [{label}] {dict(zip(COMPARE, r))}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
