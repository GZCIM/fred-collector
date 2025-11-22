"""
FRED Collector Service - Main Collection Logic
Collects FRED releases using V2 bulk API and writes to Delta Lake
"""

import httpx
import asyncio
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import to_date, col
import yaml
import logging

# Import Delta Lake components (assuming they're in the infrastructure package)
import sys
sys.path.append("../../infrastructure/delta-lake")
from merge_logic import FREDDeltaMerger


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FREDCollectorV2:
    """
    FRED data collector using V2 bulk API
    Collects releases and writes to Delta Lake with vintage tracking
    """

    def __init__(
        self,
        api_keys: List[str],
        spark: SparkSession,
        delta_merger: FREDDeltaMerger,
        base_url: str = "https://api.stlouisfed.org/fred"
    ):
        """
        Initialize FRED collector

        Args:
            api_keys: List of FRED API keys for parallel collection
            spark: Active SparkSession
            delta_merger: FREDDeltaMerger instance
            base_url: FRED API base URL
        """
        self.api_keys = api_keys
        self.spark = spark
        self.delta_merger = delta_merger
        self.base_url = base_url
        self.current_key_index = 0

        logger.info(f"Initialized FRED Collector with {len(api_keys)} API keys")

    def _get_next_api_key(self) -> str:
        """Round-robin API key selection"""
        key = self.api_keys[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return key

    async def collect_release(
        self,
        release_id: int,
        api_key: Optional[str] = None
    ) -> Dict:
        """
        Collect all observations for a FRED release using V2 BULK API

        V2 API returns ALL series and observations for a release in one call!
        Uses cursor pagination for large releases (500K obs per page).

        Args:
            release_id: FRED release ID
            api_key: Specific API key to use (optional, will round-robin if None)

        Returns:
            Dictionary with release data and metadata
        """
        if api_key is None:
            api_key = self._get_next_api_key()

        logger.info(f"Collecting release {release_id} using V2 BULK API...")

        all_observations = []
        series_metadata = {}
        api_calls = 0
        next_cursor = None

        try:
            async with httpx.AsyncClient(timeout=300.0) as client:  # Longer timeout for bulk
                # V2 Bulk API - gets ALL observations in one call (paginated by cursor)
                while True:
                    # Build request with API key in header (V2 requires Bearer token)
                    headers = {"Authorization": f"Bearer {api_key}"}
                    params = {
                        "release_id": release_id,
                        "format": "json",
                        "limit": 500000,  # Max allowed by V2
                    }
                    if next_cursor:
                        params["next_cursor"] = next_cursor

                    response = await client.get(
                        f"{self.base_url}/v2/release/observations",
                        headers=headers,
                        params=params,
                    )
                    api_calls += 1

                    if response.status_code == 429:
                        # Rate limited - wait and retry
                        logger.warning(f"Rate limited on release {release_id}, waiting 10s...")
                        await asyncio.sleep(10)
                        response = await client.get(
                            f"{self.base_url}/v2/release/observations",
                            headers=headers,
                            params=params,
                        )
                        api_calls += 1

                    if response.status_code != 200:
                        raise Exception(f"V2 API error {response.status_code}: {response.text}")

                    data = response.json()

                    # Process series with their observations
                    for series in data.get("series", []):
                        series_id = series["series_id"]
                        series_metadata[series_id] = {
                            "title": series.get("title"),
                            "frequency": series.get("frequency"),
                            "units": series.get("units"),
                        }

                        # Add observations with series_id
                        for obs in series.get("observations", []):
                            all_observations.append({
                                "series_id": series_id,
                                "date": obs["date"],
                                "value": obs["value"],
                                "realtime_start": None,  # V2 doesn't return realtime
                                "realtime_end": None,
                            })

                    # Check for more pages
                    if data.get("has_more", False):
                        next_cursor = data.get("next_cursor")
                        logger.info(f"  Release {release_id}: Fetched page, {len(all_observations):,} obs so far...")
                        await asyncio.sleep(0.5)  # Brief pause between pages
                    else:
                        break

            # Calculate statistics
            series_count = len(series_metadata)

            logger.info(
                f"✅ Release {release_id}: {len(all_observations):,} observations, "
                f"{series_count:,} series, {api_calls} API calls (V2 BULK)"
            )

            return {
                "release_id": release_id,
                "observations": all_observations,
                "series_count": series_count,
                "observations_count": len(all_observations),
                "api_calls": api_calls,
                "collected_at": datetime.now(),
                "api_key_used": api_key[-8:],
            }

        except Exception as e:
            logger.error(f"❌ Failed to collect release {release_id}: {str(e)}")
            raise

    def transform_to_delta_schema(self, observations: List[Dict]) -> DataFrame:
        """
        Transform FRED API observations to Delta Lake DataFrame

        Args:
            observations: List of observation dictionaries from API

        Returns:
            DataFrame matching series_observations schema
        """
        logger.debug(f"Transforming {len(observations):,} observations...")

        # Define explicit schema to avoid inference errors
        observations_schema = StructType([
            StructField("series_id", StringType(), nullable=False),
            StructField("date", StringType(), nullable=False),
            StructField("value", DoubleType(), nullable=True),
            StructField("value_str", StringType(), nullable=True),
            StructField("realtime_start", StringType(), nullable=True),
            StructField("realtime_end", StringType(), nullable=True),
        ])

        # Transform observations
        transformed = []
        for obs in observations:
            transformed.append({
                "series_id": obs["series_id"],
                "date": obs["date"],
                "value": float(obs["value"]) if obs["value"] != "." else None,
                "value_str": obs["value"],
                "realtime_start": obs.get("realtime_start"),
                "realtime_end": obs.get("realtime_end"),
            })

        # Create DataFrame with explicit schema
        df = self.spark.createDataFrame(transformed, schema=observations_schema)

        # Convert string dates to DateType to match Delta Lake schema
        df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
              .withColumn("realtime_start", to_date(col("realtime_start"), "yyyy-MM-dd")) \
              .withColumn("realtime_end", to_date(col("realtime_end"), "yyyy-MM-dd"))

        logger.debug(f"✅ Created DataFrame with {df.count():,} rows")
        return df

    async def collect_and_store_release(
        self,
        release_id: int,
        vintage_date: Optional[str] = None,
        api_key: Optional[str] = None
    ) -> Dict:
        """
        Complete workflow: Collect release and store in Delta Lake

        Args:
            release_id: FRED release ID
            vintage_date: Vintage date (defaults to today)
            api_key: Specific API key to use (optional)

        Returns:
            Statistics dictionary
        """
        # Generate ETL run ID
        etl_run_id = f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        start_time = datetime.now()

        if vintage_date is None:
            vintage_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Starting collection for release {release_id} (run: {etl_run_id})")

        # Log collection start
        self.delta_merger.log_collection_run(
            etl_run_id=etl_run_id,
            release_id=release_id,
            started_at=start_time,
            status="running",
            context={
                "triggered_by": "collector_service",
                "pod_name": "fred-collector",  # Will be set by K8s
            }
        )

        try:
            # Step 1: Collect from FRED
            fred_data = await self.collect_release(
                release_id=release_id,
                api_key=api_key
            )

            # Step 2: Transform and merge (only if we have observations)
            if fred_data["observations_count"] > 0:
                # Transform to DataFrame
                df = self.transform_to_delta_schema(fred_data["observations"])

                # Step 3: Merge into Delta Lake
                merge_stats = self.delta_merger.merge_series_observations(
                    new_data=df,
                    vintage_date=vintage_date,
                    release_id=release_id,
                    etl_run_id=etl_run_id,
                    api_version="v1"
                )
            else:
                # No observations - skip merge, set empty stats
                logger.info(f"Release {release_id}: No observations to merge (empty release)")
                merge_stats = {"inserted": 0, "updated": 0, "deleted": 0, "total_processed": 0}

            # Step 4: Update release metadata
            # Schema must match RELEASE_METADATA_SCHEMA in schemas.py
            release_metadata_schema = StructType([
                StructField("release_id", IntegerType(), nullable=False),
                StructField("release_name", StringType(), nullable=False),
                StructField("press_release", BooleanType(), nullable=True),
                StructField("link", StringType(), nullable=True),
                StructField("series_count", IntegerType(), nullable=False),
                StructField("observations_count", IntegerType(), nullable=False),
                StructField("last_collected", TimestampType(), nullable=False),
                StructField("last_updated_by_fred", DateType(), nullable=False),
                StructField("collection_status", StringType(), nullable=False),
                StructField("collection_error", StringType(), nullable=True),
            ])

            release_metadata = self.spark.createDataFrame([{
                "release_id": release_id,
                "release_name": f"Release {release_id}",  # TODO: Get from API
                "press_release": None,  # Optional: Get from releases API
                "link": None,  # Optional: Get from releases API
                "series_count": fred_data["series_count"],
                "observations_count": fred_data["observations_count"],
                "last_collected": datetime.now(),
                "last_updated_by_fred": datetime.now().date(),
                "collection_status": "success",
                "collection_error": None,
            }], schema=release_metadata_schema)

            self.delta_merger.merge_release_metadata(
                new_release_metadata=release_metadata,
                etl_run_id=etl_run_id
            )

            # Step 5: Log completion
            self.delta_merger.log_collection_run(
                etl_run_id=etl_run_id,
                release_id=release_id,
                started_at=start_time,
                completed_at=datetime.now(),
                status="success",
                metrics={
                    "series_count": fred_data["series_count"],
                    "inserted": merge_stats["inserted"],
                    "updated": merge_stats["updated"],
                    "deleted": merge_stats["deleted"],
                    "api_calls": fred_data["api_calls"],
                    "api_key_used": fred_data["api_key_used"],
                },
                context={
                    "triggered_by": "collector_service",
                    "api_version": "v2",
                }
            )

            duration = (datetime.now() - start_time).total_seconds()

            logger.info(
                f"✅ Release {release_id} complete: "
                f"{merge_stats['inserted']:,} inserted, "
                f"{merge_stats['updated']:,} updated, "
                f"{merge_stats['deleted']:,} deleted "
                f"({duration:.1f}s)"
            )

            return {
                "release_id": release_id,
                "etl_run_id": etl_run_id,
                "status": "success",
                "duration_seconds": duration,
                **merge_stats,
                **fred_data,
            }

        except Exception as e:
            # Log failure
            self.delta_merger.log_collection_run(
                etl_run_id=etl_run_id,
                release_id=release_id,
                started_at=start_time,
                completed_at=datetime.now(),
                status="failed",
                error_info={
                    "message": str(e),
                    "stack_trace": None,  # TODO: Add traceback
                }
            )

            logger.error(f"❌ Release {release_id} failed: {str(e)}")
            raise

    async def collect_multiple_releases(
        self,
        release_ids: List[int],
        parallel: bool = False,
        max_concurrent: int = 3
    ) -> List[Dict]:
        """
        Collect multiple releases sequentially or in parallel

        Args:
            release_ids: List of FRED release IDs
            parallel: If True, collect in parallel (default: False)
            max_concurrent: Maximum concurrent collections if parallel

        Returns:
            List of statistics dictionaries
        """
        logger.info(
            f"Collecting {len(release_ids)} releases "
            f"({'parallel' if parallel else 'sequential'})"
        )

        if parallel:
            # Parallel collection with semaphore for rate limiting
            semaphore = asyncio.Semaphore(max_concurrent)

            async def collect_with_semaphore(release_id):
                async with semaphore:
                    return await self.collect_and_store_release(release_id)

            tasks = [collect_with_semaphore(rid) for rid in release_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        else:
            # Sequential collection with delay between releases
            results = []
            for i, release_id in enumerate(release_ids, 1):
                logger.info(f"Progress: {i}/{len(release_ids)}")

                try:
                    result = await self.collect_and_store_release(release_id)
                    results.append(result)
                except Exception as e:
                    results.append({
                        "release_id": release_id,
                        "status": "failed",
                        "error": str(e),
                    })

                # Delay between releases to avoid rate limiting
                if i < len(release_ids):
                    await asyncio.sleep(2)

        # Summarize results
        successful = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
        failed = len(results) - successful

        logger.info(
            f"Collection complete: {successful}/{len(release_ids)} successful, "
            f"{failed} failed"
        )

        return results


# Convenience function for standalone use
async def collect_releases_to_delta(
    release_ids: List[int],
    config_path: str = "../../infrastructure/delta-lake/config.yaml",
    parallel: bool = False
) -> List[Dict]:
    """
    Standalone function to collect releases and store in Delta Lake

    Args:
        release_ids: List of FRED release IDs to collect
        config_path: Path to config.yaml
        parallel: Whether to collect in parallel

    Returns:
        List of collection statistics
    """
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Initialize Spark (simplified - in production use proper config)
    from init_delta_tables import get_spark_session
    spark = get_spark_session(config["storage"]["account_name"])

    # Initialize Delta merger
    merger = FREDDeltaMerger(
        spark=spark,
        storage_account=config["storage"]["account_name"],
        container=config["storage"]["container"]
    )

    # Get API keys (in production, from Key Vault)
    api_keys = [
        "{{ API_KEY_1 }}",  # Replace with actual keys from Key Vault
        "{{ API_KEY_2 }}",
        # ... up to 9 keys
    ]

    # Initialize collector
    collector = FREDCollectorV2(
        api_keys=api_keys,
        spark=spark,
        delta_merger=merger
    )

    # Collect releases
    results = await collector.collect_multiple_releases(
        release_ids=release_ids,
        parallel=parallel,
        max_concurrent=3
    )

    spark.stop()

    return results
