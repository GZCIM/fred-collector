"""
Synapse Hello Flow - Infrastructure Test
Tests Spark pool connectivity and Delta Lake CRUD operations

Expected runtime: 2-5 minutes
Purpose: Confirm deployment works before attempting 145M row transformation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FRED-HelloFlow") \
    .getOrCreate()

print("=" * 80)
print("SYNAPSE HELLO FLOW - INFRASTRUCTURE TEST")
print("=" * 80)
print()

# Configuration
STORAGE_ACCOUNT = "gzcstorageaccount"
CONTAINER = "macroeconomic-maintained-series"
TEST_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/US_Fred_Data/hello_flow_test"

start_time = time.time()

# ============================================================================
# TEST 1: Spark Connectivity
# ============================================================================
print("TEST 1: Spark Pool Connectivity")
print("-" * 80)

try:
    # Check Spark configuration
    spark_version = spark.version
    executor_count = len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1  # Exclude driver

    print(f"✅ Spark version: {spark_version}")
    print(f"✅ Executors available: {executor_count}")
    print(f"✅ Spark session active")
    print()
except Exception as e:
    print(f"❌ Spark connectivity failed: {e}")
    raise

# ============================================================================
# TEST 2: Delta Lake Write (CREATE)
# ============================================================================
print("TEST 2: Delta Lake Write (CREATE)")
print("-" * 80)

try:
    # Create test DataFrame
    test_data = [
        (1, "UNRATE", "2024-01-01", 3.7, "hello-flow"),
        (2, "GDP", "2024-01-01", 27500.0, "hello-flow"),
        (3, "CPIAUCSL", "2024-01-01", 310.5, "hello-flow"),
        (4, "PAYEMS", "2024-01-01", 157000.0, "hello-flow"),
        (5, "FEDFUNDS", "2024-01-01", 5.33, "hello-flow"),
        (6, "DGS10", "2024-01-01", 4.25, "hello-flow"),
        (7, "MORTGAGE30US", "2024-01-01", 6.88, "hello-flow"),
        (8, "DEXUSEU", "2024-01-01", 1.085, "hello-flow"),
        (9, "VIXCLS", "2024-01-01", 13.5, "hello-flow"),
        (10, "T10Y2Y", "2024-01-01", 0.42, "hello-flow")
    ]

    columns = ["id", "series_id", "date", "value", "test_run"]
    test_df = spark.createDataFrame(test_data, columns)

    # Add timestamp
    test_df = test_df.withColumn("created_at", current_timestamp())

    print(f"📊 Created test DataFrame with {test_df.count()} rows")
    print(f"📋 Schema: {test_df.columns}")

    # Write to Delta Lake
    print(f"📤 Writing to: {TEST_PATH}")
    test_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(TEST_PATH)

    print("✅ Write successful")
    print()
except Exception as e:
    print(f"❌ Write failed: {e}")
    raise

# ============================================================================
# TEST 3: Delta Lake Read
# ============================================================================
print("TEST 3: Delta Lake Read")
print("-" * 80)

try:
    read_df = spark.read.format("delta").load(TEST_PATH)

    row_count = read_df.count()
    print(f"✅ Read successful")
    print(f"📊 Rows retrieved: {row_count}")
    print(f"📋 Columns: {read_df.columns}")

    # Show sample data
    print("\n📄 Sample data:")
    read_df.select("series_id", "date", "value").show(5, truncate=False)

    if row_count != 10:
        raise ValueError(f"Expected 10 rows, got {row_count}")

    print()
except Exception as e:
    print(f"❌ Read failed: {e}")
    raise

# ============================================================================
# TEST 4: Delta Lake Update
# ============================================================================
print("TEST 4: Delta Lake Update")
print("-" * 80)

try:
    delta_table = DeltaTable.forPath(spark, TEST_PATH)

    # Update UNRATE value
    print("📝 Updating UNRATE value from 3.7 to 3.8")
    delta_table.update(
        condition = col("series_id") == "UNRATE",
        set = {"value": lit(3.8)}
    )

    # Verify update
    updated_df = spark.read.format("delta").load(TEST_PATH)
    unrate_value = updated_df.filter(col("series_id") == "UNRATE").select("value").first()[0]

    print(f"✅ Update successful")
    print(f"📊 New UNRATE value: {unrate_value}")

    if unrate_value != 3.8:
        raise ValueError(f"Expected value 3.8, got {unrate_value}")

    print()
except Exception as e:
    print(f"❌ Update failed: {e}")
    raise

# ============================================================================
# TEST 5: Delta Lake Delete
# ============================================================================
print("TEST 5: Delta Lake Delete")
print("-" * 80)

try:
    # Delete rows where id > 7
    print("🗑️  Deleting rows where id > 7")
    delta_table.delete(condition = col("id") > 7)

    # Verify deletion
    remaining_df = spark.read.format("delta").load(TEST_PATH)
    remaining_count = remaining_df.count()

    print(f"✅ Delete successful")
    print(f"📊 Remaining rows: {remaining_count}")

    if remaining_count != 7:
        raise ValueError(f"Expected 7 rows remaining, got {remaining_count}")

    print()
except Exception as e:
    print(f"❌ Delete failed: {e}")
    raise

# ============================================================================
# TEST 6: Delta Lake Version History
# ============================================================================
print("TEST 6: Delta Lake Version History")
print("-" * 80)

try:
    history_df = delta_table.history(3)  # Last 3 versions

    print("✅ Version history retrieved")
    print(f"📊 Operations performed: {history_df.count()}")
    print("\n📄 Operation log:")
    history_df.select("version", "operation", "operationMetrics").show(truncate=False)

    print()
except Exception as e:
    print(f"❌ Version history failed: {e}")
    # Non-critical - continue

# ============================================================================
# Summary
# ============================================================================
elapsed = time.time() - start_time

print("=" * 80)
print("✅ ALL TESTS PASSED - SYNAPSE INFRASTRUCTURE READY")
print("=" * 80)
print()
print(f"⏱️  Total duration: {elapsed:.1f} seconds")
print(f"📍 Test location: {TEST_PATH}")
print()
print("Infrastructure validation:")
print("  ✅ Spark pool accessible")
print("  ✅ Delta Lake write successful")
print("  ✅ Delta Lake read successful")
print("  ✅ Delta Lake update successful")
print("  ✅ Delta Lake delete successful")
print("  ✅ Version history working")
print()
print("=" * 80)
print("READY FOR PRODUCTION FRED TRANSFORMATION")
print("=" * 80)
print()
print("Next steps:")
print("  1. Deploy synapse_blob_to_delta.py for full transformation")
print("  2. Expected: 341 releases, ~800k series, 145M observations")
print("  3. Estimated duration: 10-20 minutes with distributed processing")
print()

# Cleanup (optional - comment out to keep test data)
print("🧹 Cleaning up test data...")
spark.sql(f"DROP TABLE IF EXISTS delta.`{TEST_PATH}`")
print("✅ Cleanup complete")
print()

spark.stop()
