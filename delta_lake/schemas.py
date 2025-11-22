"""
Delta Lake Schema Definitions for FRED Data
Defines table schemas, partitioning, and version tracking
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
    TimestampType,
    BooleanType,
)


# =============================================================================
# SERIES OBSERVATIONS TABLE
# =============================================================================
# Main fact table containing all FRED time series observations
# Partitioned by series_id for optimal query performance
# Tracks vintages and revisions over time

SERIES_OBSERVATIONS_SCHEMA = StructType([
    # Primary Keys
    StructField("series_id", StringType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("vintage_date", DateType(), nullable=False),

    # Observation Data
    StructField("value", DoubleType(), nullable=True),  # Can be null for deleted obs
    StructField("value_str", StringType(), nullable=True),  # Original string value

    # Metadata
    StructField("release_id", IntegerType(), nullable=False),
    StructField("realtime_start", DateType(), nullable=True),
    StructField("realtime_end", DateType(), nullable=True),

    # ETL Tracking
    StructField("last_updated", TimestampType(), nullable=False),
    StructField("_change_type", StringType(), nullable=False),  # 'insert', 'update', 'delete'
    StructField("_etl_run_id", StringType(), nullable=True),
    StructField("_source_api_version", StringType(), nullable=True),  # 'v1' or 'v2'
])

# Partitioning: Hash partition by series_id
# Expected: ~90K series -> 1000 partitions = ~90 series per partition
SERIES_OBSERVATIONS_PARTITION_COLUMNS = ["series_id"]


# =============================================================================
# SERIES METADATA TABLE
# =============================================================================
# Dimension table containing FRED series metadata
# One row per series (no versioning needed for metadata)

SERIES_METADATA_SCHEMA = StructType([
    # Primary Key
    StructField("series_id", StringType(), nullable=False),

    # Core Metadata
    StructField("title", StringType(), nullable=True),
    StructField("observation_start", DateType(), nullable=True),
    StructField("observation_end", DateType(), nullable=True),
    StructField("frequency", StringType(), nullable=True),
    StructField("frequency_short", StringType(), nullable=True),
    StructField("units", StringType(), nullable=True),
    StructField("units_short", StringType(), nullable=True),
    StructField("seasonal_adjustment", StringType(), nullable=True),
    StructField("seasonal_adjustment_short", StringType(), nullable=True),

    # Classification
    StructField("release_id", IntegerType(), nullable=True),
    StructField("popularity", IntegerType(), nullable=True),
    StructField("group_popularity", IntegerType(), nullable=True),

    # Timestamps
    StructField("realtime_start", DateType(), nullable=True),
    StructField("realtime_end", DateType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),

    # Status
    StructField("notes", StringType(), nullable=True),
    StructField("_etl_run_id", StringType(), nullable=True),
])


# =============================================================================
# RELEASE METADATA TABLE
# =============================================================================
# Dimension table containing FRED release metadata
# Tracks release calendar and collection history

RELEASE_METADATA_SCHEMA = StructType([
    # Primary Key
    StructField("release_id", IntegerType(), nullable=False),

    # Core Metadata
    StructField("release_name", StringType(), nullable=False),
    StructField("press_release", BooleanType(), nullable=True),
    StructField("link", StringType(), nullable=True),

    # Latest Collection Stats
    StructField("series_count", IntegerType(), nullable=True),
    StructField("observations_count", IntegerType(), nullable=True),
    StructField("last_collected", TimestampType(), nullable=True),
    StructField("last_updated_by_fred", DateType(), nullable=True),

    # Collection Tracking
    StructField("collection_status", StringType(), nullable=True),  # 'success', 'failed', 'pending'
    StructField("collection_error", StringType(), nullable=True),
    StructField("_etl_run_id", StringType(), nullable=True),
])


# =============================================================================
# COLLECTION HISTORY TABLE
# =============================================================================
# Audit log of all collection runs
# Tracks performance metrics and errors

COLLECTION_HISTORY_SCHEMA = StructType([
    # Primary Keys
    StructField("etl_run_id", StringType(), nullable=False),
    StructField("release_id", IntegerType(), nullable=False),

    # Timing
    StructField("started_at", TimestampType(), nullable=False),
    StructField("completed_at", TimestampType(), nullable=True),
    StructField("duration_seconds", DoubleType(), nullable=True),

    # Metrics
    StructField("series_count", IntegerType(), nullable=True),
    StructField("observations_added", IntegerType(), nullable=True),
    StructField("observations_updated", IntegerType(), nullable=True),
    StructField("observations_deleted", IntegerType(), nullable=True),
    StructField("api_calls", IntegerType(), nullable=True),
    StructField("api_key_used", StringType(), nullable=True),

    # Status
    StructField("status", StringType(), nullable=False),  # 'success', 'failed', 'running'
    StructField("error_message", StringType(), nullable=True),
    StructField("error_stack_trace", StringType(), nullable=True),

    # Context
    StructField("triggered_by", StringType(), nullable=True),  # 'schedule', 'event', 'manual'
    StructField("kubernetes_pod", StringType(), nullable=True),
    StructField("api_version", StringType(), nullable=True),
])


# =============================================================================
# TABLE PROPERTIES
# =============================================================================

# Delta Lake properties for all tables
# Note: Using only open-source Delta Lake properties (not Databricks-specific)
DELTA_TABLE_PROPERTIES = {
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
    # Removed Databricks-only properties (autoOptimize.optimizeWrite, autoOptimize.autoCompact)
}

# Z-ordering columns for performance
Z_ORDER_COLUMNS = {
    "series_observations": ["date", "vintage_date"],
    "series_metadata": ["release_id", "frequency"],
    "release_metadata": ["last_updated_by_fred"],
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_table_path(storage_account: str, container: str, table_name: str) -> str:
    """
    Get ABFSS path for Delta table

    Args:
        storage_account: Azure storage account name
        container: Container name (e.g., 'macroeconomic-maintained-series')
        table_name: Table name

    Returns:
        Full ABFSS path
    """
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/US_Fred_Data/{table_name}"


def get_schema(table_name: str) -> StructType:
    """Get schema for table"""
    schemas = {
        "series_observations": SERIES_OBSERVATIONS_SCHEMA,
        "series_metadata": SERIES_METADATA_SCHEMA,
        "release_metadata": RELEASE_METADATA_SCHEMA,
        "collection_history": COLLECTION_HISTORY_SCHEMA,
    }

    if table_name not in schemas:
        raise ValueError(f"Unknown table: {table_name}")

    return schemas[table_name]
