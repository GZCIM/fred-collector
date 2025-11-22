"""
FRED Spark Processor - Phase 2: Parallel Delta Lake Processing
Reads raw Parquet from blob storage and merges into Delta Lake with MASSIVE parallelism
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, current_timestamp, when,
    coalesce, broadcast, count, sum as spark_sum
)
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
from datetime import datetime
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FREDSparkProcessor:
    """
    High-performance Spark processor for FRED data
    - Reads all Parquet files from blob storage in parallel
    - Merges into Delta Lake with optimized partitioning
    - Massive parallelism - processes 100M+ rows efficiently
    """

    def __init__(
        self,
        spark: SparkSession,
        storage_account: str = "gzcstorageaccount",
        container: str = "macroeconomic-maintained-series",
        raw_path: str = "US_Fred_Data/raw",
        delta_path: str = "US_Fred_Data/series_observations"
    ):
        self.spark = spark
        self.storage_account = storage_account
        self.container = container
        self.raw_path = raw_path
        self.delta_path = delta_path

        self.base_uri = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

    def process_vintage(self, vintage_date: str) -> dict:
        """
        Process all raw Parquet files for a vintage date into Delta Lake

        Args:
            vintage_date: The collection vintage date (YYYY-MM-DD)

        Returns:
            Processing statistics
        """
        start_time = datetime.now()
        raw_uri = f"{self.base_uri}/{self.raw_path}/{vintage_date}/"
        delta_uri = f"{self.base_uri}/{self.delta_path}"

        logger.info("=" * 60)
        logger.info("SPARK PARALLEL PROCESSOR")
        logger.info(f"  Vintage: {vintage_date}")
        logger.info(f"  Source: {raw_uri}")
        logger.info(f"  Target: {delta_uri}")
        logger.info("=" * 60)

        # Read all Parquet files in parallel
        logger.info("Reading raw Parquet files...")
        raw_df = self.spark.read.parquet(f"{raw_uri}*.parquet")

        # Get initial counts
        raw_count = raw_df.count()
        logger.info(f"  Raw observations: {raw_count:,}")

        # Transform to Delta schema
        logger.info("Transforming data...")
        transformed_df = self._transform_to_delta_schema(raw_df, vintage_date)

        # Repartition for parallel writing (optimize based on data size)
        num_partitions = max(200, raw_count // 500000)  # ~500K rows per partition
        transformed_df = transformed_df.repartition(num_partitions, "series_id")
        logger.info(f"  Repartitioned to {num_partitions} partitions")

        # Merge into Delta Lake
        logger.info("Merging into Delta Lake...")
        stats = self._merge_to_delta(transformed_df, delta_uri, vintage_date)

        duration = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("PROCESSING COMPLETE")
        logger.info(f"  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
        logger.info(f"  Rows processed: {raw_count:,}")
        logger.info(f"  Inserted: {stats['inserted']:,}")
        logger.info(f"  Updated: {stats['updated']:,}")
        logger.info(f"  Throughput: {raw_count/duration:,.0f} rows/sec")
        logger.info("=" * 60)

        return {
            "vintage_date": vintage_date,
            "raw_count": raw_count,
            "duration_seconds": duration,
            "throughput_per_second": raw_count / duration,
            **stats
        }

    def _transform_to_delta_schema(self, df, vintage_date: str):
        """Transform raw data to Delta Lake schema"""
        return df.select(
            col("series_id"),
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            when(col("value") == ".", None)
                .otherwise(col("value").cast(DoubleType()))
                .alias("value"),
            col("value").alias("value_str"),
            lit(vintage_date).alias("vintage_date"),
            to_date(lit(vintage_date), "yyyy-MM-dd").alias("realtime_start"),
            to_date(lit(vintage_date), "yyyy-MM-dd").alias("realtime_end"),
            col("release_id").cast("int"),
            current_timestamp().alias("etl_updated_at"),
        )

    def _merge_to_delta(self, new_data, delta_uri: str, vintage_date: str) -> dict:
        """
        Optimized Delta merge with parallel processing

        Uses:
        - Broadcast join for small lookup tables
        - Z-ordering for query optimization
        - Optimize writes for parallel I/O
        """
        # Check if Delta table exists
        try:
            delta_table = DeltaTable.forPath(self.spark, delta_uri)
            logger.info("  Delta table exists, performing MERGE...")

            # Perform MERGE with optimized conditions
            merge_result = (
                delta_table.alias("target")
                .merge(
                    new_data.alias("source"),
                    """
                    target.series_id = source.series_id AND
                    target.date = source.date AND
                    target.vintage_date = source.vintage_date
                    """
                )
                .whenMatchedUpdate(
                    condition="target.value <> source.value OR target.value IS NULL",
                    set={
                        "value": "source.value",
                        "value_str": "source.value_str",
                        "etl_updated_at": "source.etl_updated_at",
                    }
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            # Get merge stats from metrics
            stats = {
                "inserted": merge_result.numTargetRowsInserted if hasattr(merge_result, 'numTargetRowsInserted') else 0,
                "updated": merge_result.numTargetRowsUpdated if hasattr(merge_result, 'numTargetRowsUpdated') else 0,
            }

        except Exception as e:
            if "is not a Delta table" in str(e) or "doesn't exist" in str(e):
                logger.info("  Delta table doesn't exist, creating...")
                new_data.write.format("delta").mode("overwrite").save(delta_uri)
                stats = {"inserted": new_data.count(), "updated": 0}
            else:
                raise

        # Optimize table after large writes
        logger.info("  Optimizing Delta table...")
        self.spark.sql(f"OPTIMIZE delta.`{delta_uri}` ZORDER BY (series_id, date)")

        return stats

    def process_all_vintages(self, vintage_dates: list = None) -> dict:
        """
        Process multiple vintage dates

        If no dates provided, discovers available vintages from blob storage
        """
        if vintage_dates is None:
            # Discover available vintages
            raw_uri = f"{self.base_uri}/{self.raw_path}/"
            # List directories (would need additional logic)
            logger.warning("Auto-discovery not implemented, please provide vintage_dates")
            return {}

        all_stats = []
        total_start = datetime.now()

        for vintage_date in vintage_dates:
            try:
                stats = self.process_vintage(vintage_date)
                all_stats.append(stats)
            except Exception as e:
                logger.error(f"Failed to process {vintage_date}: {e}")
                all_stats.append({
                    "vintage_date": vintage_date,
                    "status": "failed",
                    "error": str(e)
                })

        total_duration = (datetime.now() - total_start).total_seconds()

        return {
            "total_vintages": len(vintage_dates),
            "total_duration_seconds": total_duration,
            "vintages": all_stats
        }


def get_spark_session(storage_account: str) -> SparkSession:
    """Create optimized Spark session for parallel processing"""
    return (
        SparkSession.builder
        .appName("FRED-Spark-Processor")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Azure Storage config
        .config(f"spark.hadoop.fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
        .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
        # Performance tuning for parallel processing
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Memory tuning
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )


def main():
    """Main entry point"""
    import sys

    # Get vintage date from args or use today
    vintage_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")

    storage_account = "gzcstorageaccount"

    logger.info("Initializing Spark session...")
    spark = get_spark_session(storage_account)

    processor = FREDSparkProcessor(spark=spark, storage_account=storage_account)

    try:
        stats = processor.process_vintage(vintage_date)
        print(f"\nProcessing complete!")
        print(f"  Throughput: {stats['throughput_per_second']:,.0f} rows/second")
        print(f"  Total time: {stats['duration_seconds']:.1f} seconds")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
