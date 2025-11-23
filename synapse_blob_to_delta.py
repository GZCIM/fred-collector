"""
Azure Synapse Notebook - Blob to Delta Transformation
Converts raw FRED parquet files to optimized Delta Lake table + Direct Analysis

Run in: Azure Synapse Spark Pool
Resources: Medium (4-8 cores recommended)

Features:
- Time-based partitioning (year, month) for optimal query performance
- Z-ordering on series_id and release_id
- Comprehensive quality control checks
- Performance benchmarking
- Direct series analysis (trends, statistics, coverage)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, substring, year, month, count, countDistinct,
    min as spark_min, max as spark_max, avg, stddev,
    when, datediff, to_date, lag, first, last
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import time

# Configuration - PARAMETERIZED (can be overridden by orchestrator)
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account") if "dbutils" in dir() else "gzcstorageaccount"
CONTAINER = dbutils.widgets.get("container") if "dbutils" in dir() else "macroeconomic-maintained-series"
VINTAGE_DATE = dbutils.widgets.get("vintage_date") if "dbutils" in dir() else "2025-11-21"
RELEASE_ID = dbutils.widgets.get("release_id") if "dbutils" in dir() else None

BLOB_PREFIX = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/US_Fred_Data/raw/{VINTAGE_DATE}/"
DELTA_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/US_Fred_Data/series_observations"

# Stats tracking
stats = {
    "files_read": 0,
    "total_rows": 0,
    "duplicates_removed": 0,
    "rows_written": 0
}

print("="*80)
print("SYNAPSE BLOB-TO-DELTA TRANSFORMER")
print("="*80)
print(f"Vintage: {VINTAGE_DATE}")
print(f"Source: {BLOB_PREFIX}")
print(f"Target: {DELTA_PATH}")
print()

# Step 1: Read all parquet files from blob storage
print("Step 1: Reading blob files...")
start_time = time.time()

try:
    # Read all parquet files with wildcard
    df = spark.read.parquet(f"{BLOB_PREFIX}*.parquet")

    stats["total_rows"] = df.count()
    print(f"✅ Read {stats['total_rows']:,} rows from blob storage")
    print(f"   Columns: {df.columns}")

except Exception as e:
    print(f"❌ Error reading blobs: {e}")
    raise

# Step 2: Add transformations
print("\nStep 2: Transforming data...")

# Ensure release_date column exists (add null if missing)
if "release_date" not in df.columns:
    df = df.withColumn("release_date", lit(None))

# Add collection vintage and partition columns
transformed_df = df.withColumn("collection_vintage", lit(VINTAGE_DATE)) \
    .withColumn("year", substring(col("date"), 1, 4).cast("int")) \
    .withColumn("month", substring(col("date"), 6, 2).cast("int"))

print(f"✅ Added transformation columns")
print(f"   Schema: {transformed_df.schema}")

# Step 3: Deduplicate (series_id + date)
print("\nStep 3: Deduplicating...")
before_dedup = transformed_df.count()
deduped_df = transformed_df.dropDuplicates(["series_id", "date"])
after_dedup = deduped_df.count()
stats["duplicates_removed"] = before_dedup - after_dedup

print(f"✅ Removed {stats['duplicates_removed']:,} duplicates")
print(f"   Final rows: {after_dedup:,}")

# Step 4: Quality Control Checks
print("\nStep 4: Quality control checks...")
qc_passed = True

# Check 1: Required columns
required_cols = ["series_id", "date", "value", "vintage_date", "collected_at",
                 "release_id", "release_date", "collection_vintage", "year", "month"]
missing_cols = [c for c in required_cols if c not in deduped_df.columns]
if missing_cols:
    print(f"❌ Missing columns: {missing_cols}")
    qc_passed = False
else:
    print(f"✅ Schema validation passed")

# Check 2: No nulls in key columns
null_checks = deduped_df.select(
    col("series_id").isNull().cast("int").alias("series_id_nulls"),
    col("date").isNull().cast("int").alias("date_nulls"),
    col("release_id").isNull().cast("int").alias("release_id_nulls")
).groupBy().sum().collect()[0]

has_nulls = any(null_checks[i] > 0 for i in range(3))
if has_nulls:
    print(f"❌ Found nulls in key columns: {null_checks}")
    qc_passed = False
else:
    print(f"✅ Null check passed")

# Check 3: Date format validation
invalid_dates = deduped_df.filter(~col("date").rlike(r"^\d{4}-\d{2}-\d{2}$")).count()
if invalid_dates > 0:
    print(f"❌ Found {invalid_dates} invalid date formats")
    qc_passed = False
else:
    print(f"✅ Date format validation passed")

if not qc_passed:
    raise Exception("Quality control failed!")

print(f"\n✅ All QC checks passed")

# Step 5: Write to Delta Lake
print(f"\nStep 5: Writing to Delta Lake...")
print(f"Target: {DELTA_PATH}")
print(f"Partitioning by: (year, month)")

write_start = time.time()

# Check if table exists and what partition it has
try:
    existing_table = DeltaTable.forPath(spark, DELTA_PATH)
    print(f"⚠️  Table exists - will OVERWRITE")

    # Write with overwrite mode
    deduped_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .option("overwriteSchema", "true") \
        .save(DELTA_PATH)

except Exception as e:
    # Table doesn't exist, create new
    print(f"Table doesn't exist - creating new")
    deduped_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(DELTA_PATH)

write_duration = time.time() - write_start
stats["rows_written"] = after_dedup

print(f"✅ Write complete in {write_duration:.2f}s")
print(f"   Rows written: {stats['rows_written']:,}")

# Step 6: Optimize with Z-ordering
print(f"\nStep 6: Optimizing Delta table...")

try:
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)

    # Z-order by series_id and release_id
    delta_table.optimize().executeZOrderBy("series_id", "release_id")

    print(f"✅ Z-ordering complete (series_id, release_id)")
except Exception as e:
    print(f"⚠️  Z-ordering skipped: {e}")

# Step 7: Benchmark queries
print(f"\nStep 7: Benchmarking queries...")

# Query 1: Single series
q1_start = time.time()
single_series = spark.read.format("delta").load(DELTA_PATH) \
    .filter(col("series_id") == "UNRATE") \
    .count()
q1_duration = (time.time() - q1_start) * 1000
print(f"  Single series query: {q1_duration:.0f}ms ({single_series} rows)")

# Query 2: Release query
q2_start = time.time()
release_data = spark.read.format("delta").load(DELTA_PATH) \
    .filter(col("release_id") == 10) \
    .select("series_id", "date", "value") \
    .count()
q2_duration = (time.time() - q2_start) * 1000
print(f"  Release query: {q2_duration:.0f}ms ({release_data} rows)")

# Query 3: Time range
q3_start = time.time()
time_range = spark.read.format("delta").load(DELTA_PATH) \
    .filter((col("year") == 2023) & (col("month").isin([1, 2, 3]))) \
    .select("series_id", "date", "value") \
    .count()
q3_duration = (time.time() - q3_start) * 1000
print(f"  Time range query: {q3_duration:.0f}ms ({time_range} rows)")

# Query 4: Count
q4_start = time.time()
total_count = spark.read.format("delta").load(DELTA_PATH).count()
q4_duration = (time.time() - q4_start) * 1000
print(f"  Count query: {q4_duration:.0f}ms ({total_count:,} rows)")

# Step 8: Direct Series Analysis (preparing "half the job")
print(f"\nStep 8: Running direct series analysis...")
analysis_start = time.time()

delta_df = spark.read.format("delta").load(DELTA_PATH)

# Analysis 1: Data Coverage Statistics
print("\n  Analysis 1: Data Coverage Statistics")
coverage_stats = delta_df.groupBy("series_id").agg(
    spark_min("date").alias("first_date"),
    spark_max("date").alias("last_date"),
    count("*").alias("observation_count"),
    countDistinct("release_id").alias("release_count")
).cache()

print(f"    Total series analyzed: {coverage_stats.count():,}")

# Sample coverage stats
coverage_sample = coverage_stats.orderBy(col("observation_count").desc()).limit(10)
print(f"    Top 10 series by observation count:")
for row in coverage_sample.collect():
    print(f"      {row.series_id}: {row.observation_count:,} obs, {row.first_date} to {row.last_date}")

# Analysis 2: Temporal Distribution
print("\n  Analysis 2: Temporal Distribution")
temporal_dist = delta_df.groupBy("year", "month").agg(
    count("*").alias("observation_count"),
    countDistinct("series_id").alias("unique_series")
).orderBy("year", "month")

recent_months = temporal_dist.filter(col("year") >= 2020).collect()
if recent_months:
    print(f"    Recent months distribution (2020+):")
    for row in recent_months[-12:]:  # Last 12 months
        print(f"      {row.year}-{row.month:02d}: {row.observation_count:,} obs, {row.unique_series:,} series")

# Analysis 3: Release Statistics
print("\n  Analysis 3: Release Statistics")
release_stats = delta_df.groupBy("release_id").agg(
    countDistinct("series_id").alias("series_count"),
    count("*").alias("observation_count"),
    spark_min("date").alias("earliest_data"),
    spark_max("date").alias("latest_data")
).orderBy(col("series_count").desc())

print(f"    Total releases: {release_stats.count()}")
print(f"    Top 5 releases by series count:")
for row in release_stats.limit(5).collect():
    print(f"      Release {row.release_id}: {row.series_count:,} series, {row.observation_count:,} obs")

# Analysis 4: Value Statistics (basic descriptive stats)
print("\n  Analysis 4: Value Statistics")
value_stats = delta_df.select("series_id", "value").groupBy("series_id").agg(
    count("*").alias("n_obs"),
    avg("value").alias("mean_value"),
    stddev("value").alias("stddev_value"),
    spark_min("value").alias("min_value"),
    spark_max("value").alias("max_value")
).cache()

print(f"    Computed statistics for {value_stats.count():,} series")

# Sample some volatile series (high stddev/mean ratio)
volatile_series = value_stats.filter(
    (col("stddev_value").isNotNull()) &
    (col("mean_value") != 0) &
    (col("n_obs") > 100)
).withColumn(
    "cv", col("stddev_value") / col("mean_value")
).orderBy(col("cv").desc()).limit(10)

print(f"    Top 10 most volatile series (coefficient of variation):")
for row in volatile_series.collect():
    print(f"      {row.series_id}: CV={row.cv:.2f}, mean={row.mean_value:.2f}, stddev={row.stddev_value:.2f}")

# Save analysis results to separate Delta tables for later use
print("\n  Saving analysis results...")
ANALYSIS_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/US_Fred_Data/analysis"

# Save coverage stats
coverage_stats.write.format("delta").mode("overwrite") \
    .save(f"{ANALYSIS_PATH}/series_coverage")
print(f"    ✅ Coverage stats saved to {ANALYSIS_PATH}/series_coverage")

# Save value statistics
value_stats.write.format("delta").mode("overwrite") \
    .save(f"{ANALYSIS_PATH}/value_statistics")
print(f"    ✅ Value statistics saved to {ANALYSIS_PATH}/value_statistics")

# Save release statistics
release_stats.write.format("delta").mode("overwrite") \
    .save(f"{ANALYSIS_PATH}/release_statistics")
print(f"    ✅ Release statistics saved to {ANALYSIS_PATH}/release_statistics")

analysis_duration = time.time() - analysis_start
print(f"\n  ✅ Analysis complete in {analysis_duration:.2f}s")
print(f"  Analysis results available at: {ANALYSIS_PATH}")

# Final summary
total_duration = time.time() - start_time

print("\n" + "="*80)
print("SYNAPSE BLOB-TO-DELTA TRANSFORMATION + ANALYSIS COMPLETE")
print("="*80)
print(f"Vintage: {VINTAGE_DATE}")
print(f"Total rows read: {stats['total_rows']:,}")
print(f"Duplicates removed: {stats['duplicates_removed']:,}")
print(f"Total rows written: {stats['rows_written']:,}")
print(f"Total duration: {total_duration:.2f}s")
print()
print("Query Performance:")
print(f"  Single series: {q1_duration:.0f}ms")
print(f"  Release: {q2_duration:.0f}ms")
print(f"  Time range: {q3_duration:.0f}ms")
print(f"  Count: {q4_duration:.0f}ms")
print()
print("Analysis Results:")
print(f"  Coverage stats: {ANALYSIS_PATH}/series_coverage")
print(f"  Value statistics: {ANALYSIS_PATH}/value_statistics")
print(f"  Release statistics: {ANALYSIS_PATH}/release_statistics")
print("="*80)
print(f"\n✅ Delta Lake ready at: {DELTA_PATH}")
print(f"✅ Analysis tables ready at: {ANALYSIS_PATH}")
print(f"\n📊 Half the analysis job is now done - ready for downstream insights!")
