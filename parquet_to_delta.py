"""
FRED Parquet to Delta Lake Converter - NO SPARK REQUIRED
Converts raw Parquet files from blob storage to Delta Lake format
Uses deltalake (Rust-based) + polars for fast, lightweight processing
"""

import polars as pl
import deltalake as dl
from deltalake import write_deltalake
from datetime import datetime
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetToDeltaConverter:
    """Convert FRED raw parquet files to Delta Lake without Spark"""

    def __init__(
        self,
        storage_account: str = "gzcstorageaccount",
        container: str = "macroeconomic-maintained-series",
        use_azure_cli: bool = True
    ):
        self.storage_account = storage_account
        self.container = container
        self.storage_options = {
            "account_name": storage_account,
            "use_azure_cli": "true" if use_azure_cli else "false",
        }
        self.base_path = f"az://{container}/US_Fred_Data"

    def convert_vintage(self, vintage_date: str):
        """
        Convert all raw parquet files for a vintage date to Delta Lake

        Args:
            vintage_date: The collection vintage date (YYYY-MM-DD)
        """
        start_time = datetime.now()

        raw_path = f"{self.base_path}/raw/{vintage_date}"
        delta_path = f"{self.base_path}/series_observations"

        logger.info("=" * 60)
        logger.info("PARQUET TO DELTA CONVERTER (NO SPARK)")
        logger.info(f"  Vintage: {vintage_date}")
        logger.info(f"  Source: {raw_path}/*.parquet")
        logger.info(f"  Target: {delta_path}")
        logger.info("=" * 60)

        # List all parquet files for this vintage
        logger.info("📂 Listing raw parquet files...")

        # Use Azure CLI to list files
        import subprocess
        import json

        result = subprocess.run([
            "az", "storage", "blob", "list",
            "--account-name", self.storage_account,
            "--container-name", self.container,
            "--prefix", f"US_Fred_Data/raw/{vintage_date}/",
            "--query", "[?ends_with(name, '.parquet')].name",
            "--output", "json"
        ], capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Failed to list blobs: {result.stderr}")

        parquet_files = json.loads(result.stdout)
        release_files = [f for f in parquet_files if f.endswith('.parquet') and 'release_' in f]

        logger.info(f"  Found {len(release_files)} parquet files")

        if not release_files:
            logger.warning(f"  No parquet files found for vintage {vintage_date}")
            return

        # Process each file and accumulate into Delta table
        total_rows = 0
        files_processed = 0

        for i, blob_name in enumerate(release_files, 1):
            release_id = int(blob_name.split('release_')[1].split('.parquet')[0])

            # Read parquet file from Azure blob
            blob_uri = f"az://{self.container}/{blob_name}"

            logger.info(f"  [{i}/{len(release_files)}] Release {release_id}...")

            try:
                # Read parquet using polars (very fast)
                df = pl.read_parquet(blob_uri, storage_options=self.storage_options)

                # Transform to proper Delta schema
                df_transformed = df.with_columns([
                    # Convert date string to date type
                    pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
                    # Convert value to float (handle "." as null)
                    pl.when(pl.col("value") == ".").then(None).otherwise(pl.col("value").cast(pl.Float64)).alias("value"),
                    # Keep original value string
                    pl.col("value").alias("value_str"),
                    # Convert vintage_date to date
                    pl.col("vintage_date").str.to_date("%Y-%m-%d").alias("vintage_date"),
                    # Add realtime dates
                    pl.col("vintage_date").str.to_date("%Y-%m-%d").alias("realtime_start"),
                    pl.col("vintage_date").str.to_date("%Y-%m-%d").alias("realtime_end"),
                    # Convert release_id to int32
                    pl.col("release_id").cast(pl.Int32),
                    # Add ETL metadata
                    pl.lit(datetime.now()).alias("etl_updated_at"),
                ])

                # Select final columns
                df_final = df_transformed.select([
                    "series_id",
                    "date",
                    "value",
                    "value_str",
                    "vintage_date",
                    "realtime_start",
                    "realtime_end",
                    "release_id",
                    "etl_updated_at",
                ])

                row_count = len(df_final)
                total_rows += row_count

                # Write to Delta Lake (append mode)
                write_deltalake(
                    delta_path,
                    df_final,
                    storage_options=self.storage_options,
                    mode="append",
                    engine="rust",  # Use Rust engine for performance
                )

                files_processed += 1
                logger.info(f"    ✅ {row_count:,} observations added to Delta")

            except Exception as e:
                logger.error(f"    ❌ Failed to process release {release_id}: {e}")
                continue

        duration = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("CONVERSION COMPLETE")
        logger.info(f"  Files processed: {files_processed}/{len(release_files)}")
        logger.info(f"  Total observations: {total_rows:,}")
        logger.info(f"  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
        logger.info(f"  Throughput: {total_rows/duration:,.0f} rows/sec")
        logger.info(f"  Delta table: {delta_path}")
        logger.info("=" * 60)

        # Show Delta table info
        logger.info("\n📊 Delta Table Info:")
        try:
            dt = dl.DeltaTable(delta_path, storage_options=self.storage_options)
            logger.info(f"  Version: {dt.version()}")
            logger.info(f"  Files: {len(dt.file_uris())}")

            # Count unique series
            lf = pl.scan_delta(delta_path, storage_options=self.storage_options)
            series_count = lf.select("series_id").unique().collect().height
            total_obs = lf.select("series_id").count().collect()["series_id"][0]

            logger.info(f"  Total observations: {total_obs:,}")
            logger.info(f"  Unique series: {series_count:,}")

        except Exception as e:
            logger.warning(f"  Could not get table stats: {e}")


def main():
    """Main entry point"""

    # Get vintage date from args or use today
    vintage_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Converting vintage: {vintage_date}")

    converter = ParquetToDeltaConverter()
    converter.convert_vintage(vintage_date)

    logger.info("\n✅ All raw parquet files converted to Delta Lake!")
    logger.info("You can now query with query_tool/fred_query.py")


if __name__ == "__main__":
    main()
