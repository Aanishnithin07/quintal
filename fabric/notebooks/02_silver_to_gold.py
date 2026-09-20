# Fabric notebook -- Silver to Gold (star schema)
#
# Mirrors transform/models/marts/*.sql. Writes Delta tables sized and shaped
# for Direct Lake: a narrow fact with integer/hash keys, and small dimensions
# that stay resident in memory.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()
silver = spark.read.table("silver_mandi_prices")

def market_key(df):
    return F.md5(F.concat_ws("|", F.coalesce(df.state, F.lit("")),
                             F.coalesce(df.district, F.lit("")),
                             F.coalesce(df.market, F.lit(""))))

def commodity_key(df):
    return F.md5(F.concat_ws("|", F.coalesce(df.commodity, F.lit("")),
                             F.coalesce(df.variety, F.lit("")),
                             F.coalesce(df.grade, F.lit(""))))

# CELL ********************

# dim_market -- not an SCD2, and deliberately so. The market name IS the
# business key here; there is no stable upstream ID. A rename is therefore
# indistinguishable from a new market, and tracking "changes" against an
# unstable key would fabricate history. Observed lifespan is recorded instead,
# which is both true and useful: it surfaces markets that stop reporting.
feed_latest = silver.agg(F.max("arrival_date")).first()[0]

dim_market = (
    silver.filter(F.col("market").isNotNull())
          .groupBy("state", "district", "market")
          .agg(F.min("arrival_date").alias("first_reported_on"),
               F.max("arrival_date").alias("last_reported_on"),
               F.countDistinct("arrival_date").alias("days_reported"),
               F.countDistinct("commodity").alias("commodities_traded"),
               F.count("*").alias("observations"))
)
dim_market = (
    dim_market
    .withColumn("market_key", market_key(dim_market))
    .withColumn("days_since_last_report", F.datediff(F.lit(feed_latest), F.col("last_reported_on")))
    .withColumn("is_actively_reporting", F.col("days_since_last_report") <= 14)
)
dim_market.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_market")

# CELL ********************

dim_commodity = (
    silver.filter(F.col("commodity").isNotNull())
          .groupBy("commodity", "variety", "grade")
          .agg(F.first("commodity_raw", ignorenulls=True).alias("commodity_raw_example"),
               F.countDistinct("market").alias("markets_trading"),
               F.countDistinct("arrival_date").alias("days_traded"),
               F.count("*").alias("observations"),
               F.expr("percentile_approx(modal_price, 0.5)").alias("median_modal_price"),
               F.min("modal_price").alias("min_modal_price"),
               F.max("modal_price").alias("max_modal_price"))
)
dim_commodity = dim_commodity.withColumn("commodity_key", commodity_key(dim_commodity))
dim_commodity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_commodity")

# CELL ********************

bounds = silver.agg(F.min("arrival_date").alias("d0"), F.max("arrival_date").alias("d1")).first()
dim_date = (
    spark.sql(f"select explode(sequence(to_date('{bounds.d0}'), to_date('{bounds.d1}'), interval 1 day)) as date_day")
         .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
         .withColumn("year", F.year("date_day"))
         .withColumn("quarter", F.quarter("date_day"))
         .withColumn("month", F.month("date_day"))
         .withColumn("month_name", F.date_format("date_day", "MMMM"))
         .withColumn("day_name", F.date_format("date_day", "EEEE"))
         .withColumn("is_weekend", F.dayofweek("date_day").isin(1, 7))
         # Indian crop years run April-March; seasonality follows them.
         .withColumn("crop_year_start",
                     F.when(F.month("date_day") >= 4, F.year("date_day")).otherwise(F.year("date_day") - 1))
)
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")

# CELL ********************

fct = (
    silver.filter(F.col("arrival_date").isNotNull())
)
fct = (
    fct.withColumn("market_key", market_key(fct))
       .withColumn("commodity_key", commodity_key(fct))
       .withColumn("date_key", F.date_format("arrival_date", "yyyyMMdd").cast("int"))
       .withColumn("price_spread", F.col("max_price") - F.col("min_price"))
       .withColumn("price_spread_pct",
                   F.when(F.col("min_price") > 0,
                          F.round((F.col("max_price") - F.col("min_price")) / F.col("min_price") * 100, 2)))
       .select("price_key", "market_key", "commodity_key", "date_key",
               "arrival_date", "ingest_date", "reporting_lag_days",
               "min_price", "max_price", "modal_price",
               "price_spread", "price_spread_pct",
               "is_missing_price", "is_inverted_range",
               "is_modal_outside_range", "is_unparseable_date")
)

(fct.write.format("delta").mode("overwrite")
    .partitionBy("arrival_date")
    .option("overwriteSchema", "true")
    .saveAsTable("fct_daily_price"))

# CELL ********************

# Direct Lake reads Delta directly, so file layout is the performance story.
# Daily appends produce many small files; without compaction, scan time grows
# with the number of files rather than the volume of data, and the semantic
# model eventually falls back to DirectQuery.
for t in ("fct_daily_price", "silver_mandi_prices"):
    DeltaTable.forName(spark, t).optimize().executeCompaction()
    spark.sql(f"VACUUM {t} RETAIN 168 HOURS")

print("gold rebuilt and compacted")
