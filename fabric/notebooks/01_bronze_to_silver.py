# Fabric notebook -- Bronze to Silver
#
# A direct PySpark translation of transform/models/staging/stg_mandi_prices.sql.
# The two are kept deliberately equivalent: DuckDB runs the archive today at
# zero cost, and this runs the identical logic on Fabric Spark when a capacity
# is available. Bronze is the same immutable JSONL either way, so neither
# engine is load-bearing and moving between them loses nothing.
#
# Lakehouse layout assumed:
#   Files/bronze/ingest_date=YYYY-MM-DD/records.jsonl.gz
#   Tables/silver_mandi_prices   (Delta)

# CELL ********************

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

BRONZE = "Files/bronze/ingest_date=*/records.jsonl.gz"
SILVER = "silver_mandi_prices"

# CELL ********************

# Explicit schema rather than inference. Inference samples the file, so a
# column that is numeric in the sample and alphanumeric later would silently
# flip type between runs -- and this table is append-only across years.
bronze_schema = StructType([
    StructField("state",        StringType()),
    StructField("district",     StringType()),
    StructField("market",       StringType()),
    StructField("commodity",    StringType()),
    StructField("variety",      StringType()),
    StructField("grade",        StringType()),
    StructField("arrival_date", StringType()),
    StructField("min_price",    StringType()),
    StructField("max_price",    StringType()),
    StructField("modal_price",  StringType()),
])

raw = (
    spark.read
         .schema(bronze_schema)
         .json(BRONZE)
         .withColumn("_file", F.input_file_name())
)

# CELL ********************

def blank_to_null(name):
    """Trim, and treat an empty string as null.

    Deliberately not F.nullif: that landed in PySpark 3.5 (Fabric Runtime 1.3),
    and Runtime 1.2 still ships Spark 3.4, where this notebook would fail on
    import rather than on a row. This form works on both.
    """
    t = F.trim(F.col(name))
    return F.when(t != "", t)


def clean_name(col):
    """Normalise ' (' spacing and collapse runs of whitespace.

    Upstream sends 'Pointed gourd(Parval)' in commodity and
    'Pointed gourd (Parval)' in variety -- in the same row -- so without this
    the two will not join.
    """
    return F.regexp_replace(F.regexp_replace(F.trim(col), r"\s*\(\s*", " ("), r"\s+", " ")


def positive_price(col):
    """Zero and negative are missing data wearing a number's clothes."""
    return F.when(F.col(col).cast("double") > 0, F.col(col).cast("double"))


parsed = raw.select(
    F.to_date(F.regexp_extract("_file", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)).alias("ingest_date"),
    # DD/MM/YYYY, not ISO. Spark 3 needs CORRECTED parsing to reject rather
    # than silently coerce malformed dates.
    F.to_date(F.trim("arrival_date"), "dd/MM/yyyy").alias("arrival_date"),
    blank_to_null("state").alias("state"),
    blank_to_null("district").alias("district"),
    blank_to_null("market").alias("market"),
    blank_to_null("commodity").alias("commodity_raw"),
    blank_to_null("variety").alias("variety_raw"),
    clean_name(F.col("commodity")).alias("commodity"),
    clean_name(F.col("variety")).alias("variety"),
    blank_to_null("grade").alias("grade"),
    positive_price("min_price").alias("min_price"),
    positive_price("max_price").alias("max_price"),
    positive_price("modal_price").alias("modal_price"),
)

# CELL ********************

GRAIN = ["ingest_date", "state", "district", "market",
         "commodity", "variety", "grade", "arrival_date"]

silver = (
    parsed
    .withColumn("price_key", F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in GRAIN])))
    # Flags, not filters: silver records what the publisher sent, including
    # when it was wrong. The marts decide what to exclude.
    .withColumn("is_unparseable_date", F.col("arrival_date").isNull())
    .withColumn("is_missing_price", F.col("modal_price").isNull())
    .withColumn("is_inverted_range",
                F.col("min_price").isNotNull() & F.col("max_price").isNotNull()
                & (F.col("min_price") > F.col("max_price")))
    .withColumn("is_modal_outside_range",
                F.col("modal_price").isNotNull() & F.col("min_price").isNotNull()
                & F.col("max_price").isNotNull()
                & ((F.col("modal_price") < F.col("min_price"))
                   | (F.col("modal_price") > F.col("max_price"))))
    .withColumn("reporting_lag_days", F.datediff("ingest_date", "arrival_date"))
    # Offset paging over a live endpoint can re-serve a row that changed
    # between pages. Keep one deterministically.
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy(*GRAIN).orderBy(F.col("modal_price").desc_nulls_last())))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_file")
)

# CELL ********************

# Partitioned by ingest_date to match bronze, so a re-captured day rewrites
# exactly one partition and leaves the rest of the archive untouched.
(silver.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("ingest_date")
       .option("overwriteSchema", "true")
       .saveAsTable(SILVER))

print(f"{silver.count():,} rows written to {SILVER}")
