# Fabric deployment path

## Status: logic verified on Spark, not deployed to a tenant

Two different claims, kept apart deliberately.

**Verified.** `notebooks/01_bronze_to_silver.py` produces *byte-identical*
output to `transform/models/staging/stg_mandi_prices.sql`. Not "equivalent by
inspection" — the same SHA-256 over the same bronze files, asserted on every
push by [`.github/workflows/equivalence.yml`](../.github/workflows/equivalence.yml),
running Spark 3.5 on Java 17: the versions Fabric Runtime 1.3 ships.

The test executes the notebook's *own source* rather than a copy, because a
copy drifts silently and would end up reporting agreement between two things
that are no longer the same code.

**Not verified.** None of this has run in a Fabric workspace. The archive is
built without a Fabric capacity — there is no free tier, and the 60-day trial
requires a work or school tenant. Direct Lake binding, the TMDL semantic model,
and the `fabric-cicd` deployment are untested against the real service.

So: the transformations are proven correct on Spark. Their behaviour *inside
Fabric* is a reasonable expectation, not a demonstrated fact.

## Why the project is built this way

Fabric is assembled from open components — Delta Lake tables in OneLake, Spark
for transformation, a star schema behind a semantic model. None of those require
Fabric. So the archive runs the same architecture on DuckDB and GitHub Actions at
zero cost, and treats Fabric as a target rather than a dependency.

The practical consequence is that the pipeline cannot be stranded. A 60-day
trial that expires mid-project would take the warehouse with it, and with it the
daily capture the whole archive depends on. Here, the trial expiring changes
nothing: bronze is untouched, dbt keeps building, and the Fabric artefacts go
back to waiting.

## Mapping

| This repo | Fabric equivalent |
|---|---|
| `data/bronze/ingest_date=*/` | Lakehouse `Files/bronze/` |
| `ingest/capture.py` on GitHub Actions cron | Data Factory pipeline → Notebook activity |
| `transform/models/staging/` (dbt) | `notebooks/01_bronze_to_silver.py` → Delta |
| `transform/models/marts/` (dbt) | `notebooks/02_silver_to_gold.py` → Delta |
| dbt tests | Notebook assertions / Purview data quality rules |
| DuckDB `gold.*` | Lakehouse SQL analytics endpoint |
| Evidence dashboard | Direct Lake semantic model + Power BI report |
| `semantic_model/definition/*.tmdl` | Deployed semantic model |

## Deploying, when a capacity exists

```bash
pip install fabric-cicd
```

```python
from fabric_cicd import FabricWorkspace, publish_all_items

ws = FabricWorkspace(
    workspace_id="<workspace-guid>",
    repository_directory="fabric",
    item_type_in_scope=["Notebook", "SemanticModel", "DataPipeline"],
)
publish_all_items(ws)
```

Order matters: the lakehouse and its `Files/bronze` content must exist before
the notebooks run, and the gold Delta tables must exist before the semantic
model can bind to them in Direct Lake mode.

## Notes on the modelling choices

**Direct Lake, not Import.** The semantic model reads the same Delta files the
notebooks write, so there is no refresh step to schedule, fail, or pay for. The
cost is strictness: Direct Lake falls back to DirectQuery if the table layout
degrades, which is why `02_silver_to_gold.py` ends with `OPTIMIZE` and `VACUUM`.
Daily appends generate many small files, and past a threshold the fallback
becomes permanent and the model quietly gets slower.

**`discourageImplicitMeasures`.** Prices are rupees per quintal. Dragging
`modal_price` onto a visual would sum them, producing a number with no meaning
that still looks like a number. Every price figure goes through an explicit
measure that states its aggregation.

**Median rather than average.** Mandi prices are strongly right-skewed; a few
premium lots pull the mean well above what a typical seller receives.

**Crop year, not calendar year.** Indian crop years run April–March. Calendar
grouping splits one growing season across two buckets and makes seasonality
unreadable.

**No SCD2 on `dim_market`.** There is no stable market identifier upstream — the
name is the key. A rename is therefore indistinguishable from a new market, and
a type-2 dimension over an unstable key would manufacture history that did not
happen. Observed reporting lifespan is recorded instead.
