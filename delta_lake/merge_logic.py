"""
Delta Lake Merge/Upsert Logic for FRED Data
Handles incremental updates, vintage tracking, and data revisions
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce, hash, expr
)
from delta.tables import DeltaTable
from datetime import datetime
from typing import Dict, List
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
import logging

logger = logging.getLogger(__name__)


# Collection History Schema
COLLECTION_HISTORY_SCHEMA = StructType([
    StructField("etl_run_id", StringType(), nullable=False),
    StructField("release_id", IntegerType(), nullable=False),
    StructField("started_at", TimestampType(), nullable=False),
    StructField("completed_at", TimestampType(), nullable=True),
    StructField("duration_seconds", DoubleType(), nullable=True),
    StructField("series_count", IntegerType(), nullable=True),
    StructField("observations_added", IntegerType(), nullable=True),
    StructField("observations_updated", IntegerType(), nullable=True),
    StructField("observations_deleted", IntegerType(), nullable=True),
    StructField("api_calls", IntegerType(), nullable=True),
    StructField("api_key_used", StringType(), nullable=True),
    StructField("status", StringType(), nullable=False),
    StructField("error_message", StringType(), nullable=True),
    StructField("error_stack_trace", StringType(), nullable=True),
    StructField("triggered_by", StringType(), nullable=True),
    StructField("kubernetes_pod", StringType(), nullable=True),
    StructField("api_version", StringType(), nullable=True),
])


class FREDDeltaMerger:
    """
    Handles all merge/upsert operations for FRED Delta Lake tables
    Implements vintage tracking and revision detection
    """

    def __init__(self, spark: SparkSession, storage_account: str, container: str):
        """
        Initialize merger

        Args:
            spark: Active SparkSession
            storage_account: Azure storage account name
            container: Container name
        """
        self.spark = spark
        self.storage_account = storage_account
        self.container = container
        self.base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/US_Fred_Data"

    def _get_table_path(self, table_name: str) -> str:
        """Get full path to Delta table"""
        return f"{self.base_path}/{table_name}"

    def _table_exists(self, table_path: str) -> bool:
        """Check if Delta table exists at path"""
        try:
            DeltaTable.forPath(self.spark, table_path)
            return True
        except Exception as e:
            # Log for debugging but return False
            logger.debug(f"Table check failed for {table_path}: {e}")
            return False

    def _ensure_table_exists(self, table_name: str):
        """Ensure Delta table exists, create if it doesn't"""
        table_path = self._get_table_path(table_name)

        if self._table_exists(table_path):
            return  # Table exists, nothing to do

        logger.info(f"Creating Delta table: {table_name} at {table_path}")

        # Import schemas
        from schemas import get_schema, SERIES_OBSERVATIONS_PARTITION_COLUMNS

        # Get schema for table
        schema = get_schema(table_name)

        # Create empty DataFrame with schema
        empty_df = self.spark.createDataFrame([], schema=schema)

        # Write as Delta table with partitioning
        # NOTE: Only use OSS Delta Lake properties, NOT Databricks-specific ones
        writer = empty_df.write.format("delta")

        # Add partitioning for series_observations
        if table_name == "series_observations":
            writer = writer.partitionBy(*SERIES_OBSERVATIONS_PARTITION_COLUMNS)

        # Only use OSS-compatible Delta properties (no autoOptimize/autoCompact)
        writer = writer.option("delta.logRetentionDuration", "interval 30 days")
        writer = writer.option("delta.deletedFileRetentionDuration", "interval 7 days")

        # Save table
        writer.save(table_path)

        logger.info(f"✅ Created Delta table: {table_name}")

    def merge_series_observations(
        self,
        new_data: DataFrame,
        vintage_date: str,
        release_id: int,
        etl_run_id: str,
        api_version: str = "v1"
    ) -> Dict[str, int]:
        """
        Merge new observations into series_observations table with vintage tracking

        This function:
        1. Detects new observations (inserts)
        2. Detects revised observations (updates)
        3. Detects deleted observations (soft deletes)
        4. Preserves all historical vintages

        Args:
            new_data: DataFrame with new observations (must have: series_id, date, value)
            vintage_date: Date when data was collected (YYYY-MM-DD)
            release_id: FRED release ID
            etl_run_id: Unique ID for this ETL run
            api_version: API version used (v1 or v2)

        Returns:
            Dictionary with counts: {'inserted': N, 'updated': N, 'deleted': N}
        """
        # Ensure table exists
        self._ensure_table_exists("series_observations")

        table_path = self._get_table_path("series_observations")
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Ensure date column is DateType (fix for string/date mismatch)
        from pyspark.sql.functions import to_date
        if "date" in new_data.columns:
            date_type = str(new_data.schema["date"].dataType)
            if "String" in date_type:
                new_data = new_data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

        # Add metadata columns to new data
        new_data_enriched = (
            new_data
            .withColumn("vintage_date", lit(vintage_date).cast("date"))
            .withColumn("release_id", lit(release_id))
            .withColumn("last_updated", current_timestamp())
            .withColumn("_etl_run_id", lit(etl_run_id))
            .withColumn("_source_api_version", lit(api_version))
        )

        # Read current data for this release to detect changes
        current_data = (
            self.spark.read.format("delta").load(table_path)
            .filter(col("release_id") == release_id)
            .filter(col("_change_type") != "delete")
            # Get latest vintage for each series_id + date
            .groupBy("series_id", "date")
            .agg(
                expr("max(vintage_date)").alias("max_vintage"),
                expr("first(value)").alias("current_value")
            )
        )

        # Join to detect changes
        comparison = (
            new_data_enriched
            .alias("new")
            .join(
                current_data.alias("curr"),
                (col("new.series_id") == col("curr.series_id")) &
                (col("new.date") == col("curr.date")),
                "full_outer"
            )
            .select(
                coalesce(col("new.series_id"), col("curr.series_id")).alias("series_id"),
                coalesce(col("new.date"), col("curr.date")).alias("date"),
                col("new.value").alias("new_value"),
                col("curr.current_value").alias("old_value"),
                col("new.vintage_date").alias("vintage_date"),
                col("new.release_id").alias("release_id"),
                col("new.last_updated").alias("last_updated"),
                col("new._etl_run_id").alias("_etl_run_id"),
                col("new._source_api_version").alias("_source_api_version"),
            )
        )

        # Classify changes
        # Use vintage_date to detect new records (it's set by us, not null for new data)
        # old_value is null when record doesn't exist in current table
        classified = comparison.withColumn(
            "_change_type",
            # Insert: new record (vintage_date not null means from new_data, old_value null means not in table)
            when(col("vintage_date").isNotNull() & col("old_value").isNull(), lit("insert"))
            # Delete: record exists but not in new data (vintage_date null means not from new_data)
            .when(col("vintage_date").isNull() & col("old_value").isNotNull(), lit("delete"))
            # Update: both exist but values differ (handles null != null correctly via eqNullSafe)
            .when(~col("old_value").eqNullSafe(col("new_value")), lit("update"))
            .otherwise(lit("unchanged"))
        )

        # Filter out unchanged records (no need to create new vintage)
        changes_only = classified.filter(col("_change_type") != "unchanged")

        # For deletes, set value to null but keep the record
        final_data = changes_only.withColumn(
            "value",
            when(col("_change_type") == "delete", lit(None))
            .otherwise(col("new_value"))
        ).select(
            "series_id", "date", "value", "vintage_date", "release_id",
            "last_updated", "_change_type", "_etl_run_id", "_source_api_version"
        )

        # Append all changes (Delta Lake handles deduplication)
        final_data.write.format("delta").mode("append").save(table_path)

        # Calculate statistics
        stats = {
            "inserted": changes_only.filter(col("_change_type") == "insert").count(),
            "updated": changes_only.filter(col("_change_type") == "update").count(),
            "deleted": changes_only.filter(col("_change_type") == "delete").count(),
            "total_processed": new_data.count(),
        }

        return stats

    def merge_series_metadata(
        self,
        new_metadata: DataFrame,
        etl_run_id: str
    ) -> Dict[str, int]:
        """
        Merge series metadata (upsert based on series_id)

        Args:
            new_metadata: DataFrame with series metadata
            etl_run_id: Unique ID for this ETL run

        Returns:
            Dictionary with counts: {'inserted': N, 'updated': N}
        """
        # Ensure table exists
        self._ensure_table_exists("series_metadata")

        table_path = self._get_table_path("series_metadata")
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Add metadata
        new_metadata_enriched = (
            new_metadata
            .withColumn("last_updated", current_timestamp())
            .withColumn("_etl_run_id", lit(etl_run_id))
        )

        # Track counts before merge
        before_count = delta_table.toDF().count()

        # Merge (upsert on series_id)
        (
            delta_table.alias("target")
            .merge(
                new_metadata_enriched.alias("source"),
                "target.series_id = source.series_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Calculate statistics
        after_count = delta_table.toDF().count()
        inserted = after_count - before_count
        updated = new_metadata_enriched.count() - inserted

        return {
            "inserted": inserted,
            "updated": updated,
            "total_processed": new_metadata_enriched.count(),
        }

    def merge_release_metadata(
        self,
        new_release_metadata: DataFrame,
        etl_run_id: str
    ) -> Dict[str, int]:
        """
        Merge release metadata (upsert based on release_id)

        Args:
            new_release_metadata: DataFrame with release metadata
            etl_run_id: Unique ID for this ETL run

        Returns:
            Dictionary with counts: {'inserted': N, 'updated': N}
        """
        # Ensure table exists
        self._ensure_table_exists("release_metadata")

        table_path = self._get_table_path("release_metadata")
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Add metadata
        new_release_enriched = (
            new_release_metadata
            .withColumn("_etl_run_id", lit(etl_run_id))
        )

        # Track counts before merge
        before_count = delta_table.toDF().count()

        # Get columns from source to only update those (avoid schema mismatch)
        source_columns = new_release_enriched.columns
        update_dict = {col_name: f"source.{col_name}" for col_name in source_columns if col_name != "release_id"}
        insert_dict = {col_name: f"source.{col_name}" for col_name in source_columns}

        # Merge (upsert on release_id) - only update columns present in source
        (
            delta_table.alias("target")
            .merge(
                new_release_enriched.alias("source"),
                "target.release_id = source.release_id"
            )
            .whenMatchedUpdate(set=update_dict)
            .whenNotMatchedInsert(values=insert_dict)
            .execute()
        )

        # Calculate statistics
        after_count = delta_table.toDF().count()
        inserted = after_count - before_count
        updated = new_release_enriched.count() - inserted

        return {
            "inserted": inserted,
            "updated": updated,
            "total_processed": new_release_enriched.count(),
        }

    def log_collection_run(
        self,
        etl_run_id: str,
        release_id: int,
        started_at: datetime,
        completed_at: datetime = None,
        status: str = "running",
        metrics: Dict = None,
        error_info: Dict = None,
        context: Dict = None
    ):
        """
        Log collection run to collection_history table

        Args:
            etl_run_id: Unique run ID
            release_id: FRED release ID
            started_at: When collection started
            completed_at: When collection completed (None if still running)
            status: 'running', 'success', or 'failed'
            metrics: Dictionary with collection metrics
            error_info: Dictionary with error details if failed
            context: Dictionary with execution context (pod name, trigger type, etc.)
        """
        # Ensure table exists
        self._ensure_table_exists("collection_history")

        table_path = self._get_table_path("collection_history")

        # Build record
        record = {
            "etl_run_id": etl_run_id,
            "release_id": release_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "status": status,
        }

        # Add duration if completed
        if completed_at:
            duration = (completed_at - started_at).total_seconds()
            record["duration_seconds"] = duration

        # Add metrics
        if metrics:
            record.update({
                "series_count": metrics.get("series_count"),
                "observations_added": metrics.get("inserted", 0),
                "observations_updated": metrics.get("updated", 0),
                "observations_deleted": metrics.get("deleted", 0),
                "api_calls": metrics.get("api_calls"),
                "api_key_used": metrics.get("api_key_used"),
            })

        # Add error info
        if error_info:
            record.update({
                "error_message": error_info.get("message"),
                "error_stack_trace": error_info.get("stack_trace"),
            })

        # Add context
        if context:
            record.update({
                "triggered_by": context.get("triggered_by"),
                "kubernetes_pod": context.get("pod_name"),
                "api_version": context.get("api_version"),
            })

        # Create DataFrame with explicit schema and append
        df = self.spark.createDataFrame([record], schema=COLLECTION_HISTORY_SCHEMA)
        df.write.format("delta").mode("append").save(table_path)

    def get_latest_vintage(self, series_id: str) -> DataFrame:
        """
        Get latest vintage for a specific series

        Args:
            series_id: FRED series ID

        Returns:
            DataFrame with latest observations
        """
        table_path = self._get_table_path("series_observations")

        return (
            self.spark.read.format("delta").load(table_path)
            .filter(col("series_id") == series_id)
            .filter(col("_change_type") != "delete")
            .groupBy("series_id", "date")
            .agg(
                expr("max(vintage_date)").alias("vintage_date"),
                expr("first(value)").alias("value")
            )
            .orderBy("date")
        )

    def get_vintage_at_date(self, series_id: str, vintage_date: str) -> DataFrame:
        """
        Get specific vintage for a series (time travel)

        Args:
            series_id: FRED series ID
            vintage_date: Vintage date (YYYY-MM-DD)

        Returns:
            DataFrame with observations as of vintage_date
        """
        table_path = self._get_table_path("series_observations")

        return (
            self.spark.read.format("delta").load(table_path)
            .filter(col("series_id") == series_id)
            .filter(col("vintage_date") <= vintage_date)
            .filter(col("_change_type") != "delete")
            .groupBy("series_id", "date")
            .agg(
                expr("max(vintage_date)").alias("vintage_date"),
                expr("first(value)").alias("value")
            )
            .orderBy("date")
        )

    def get_revisions_for_series(
        self,
        series_id: str,
        observation_date: str
    ) -> DataFrame:
        """
        Get all revisions for a specific observation (all vintages)

        Args:
            series_id: FRED series ID
            observation_date: Observation date (YYYY-MM-DD)

        Returns:
            DataFrame with all vintages for this observation
        """
        table_path = self._get_table_path("series_observations")

        return (
            self.spark.read.format("delta").load(table_path)
            .filter(col("series_id") == series_id)
            .filter(col("date") == observation_date)
            .orderBy("vintage_date")
        )

    def optimize_tables(self, table_names: List[str] = None):
        """
        Run OPTIMIZE on Delta tables with Z-ordering

        Args:
            table_names: List of table names to optimize (None = all tables)
        """
        from schemas import Z_ORDER_COLUMNS

        if table_names is None:
            table_names = ["series_observations", "series_metadata", "release_metadata", "collection_history"]

        for table_name in table_names:
            print(f"\nOptimizing {table_name}...")

            table_path = self._get_table_path(table_name)
            delta_table = DeltaTable.forPath(self.spark, table_path)

            # Run OPTIMIZE
            if table_name in Z_ORDER_COLUMNS:
                z_cols = Z_ORDER_COLUMNS[table_name]
                print(f"  Z-ordering on: {z_cols}")
                delta_table.optimize().executeZOrderBy(z_cols)
            else:
                delta_table.optimize().executeCompaction()

            print(f"  ✅ {table_name} optimized")

    def vacuum_tables(self, retention_hours: int = 168, table_names: List[str] = None):
        """
        Vacuum old versions from Delta tables

        Args:
            retention_hours: Hours to retain (default 168 = 7 days)
            table_names: List of table names (None = all tables)
        """
        if table_names is None:
            table_names = ["series_observations", "series_metadata", "release_metadata", "collection_history"]

        for table_name in table_names:
            print(f"\nVacuuming {table_name} (retention: {retention_hours}h)...")

            table_path = self._get_table_path(table_name)
            delta_table = DeltaTable.forPath(self.spark, table_path)

            delta_table.vacuum(retention_hours)

            print(f"  ✅ {table_name} vacuumed")
