"""
FRED Collector Service - Main Entry Point
Listens to Event Hub for new release notifications and collects data
"""

import asyncio
import os
import sys
import yaml
import logging
import signal
from datetime import datetime
from typing import List
from azure.eventhub.aio import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql import SparkSession

# Add infrastructure to path
sys.path.append("./delta_lake")
from merge_logic import FREDDeltaMerger
from collector import FREDCollectorV2


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FREDCollectorService:
    """
    Main service class for FRED data collection
    Subscribes to Event Hub and processes release collection requests
    """

    def __init__(self, config_path: str = "./delta_lake/config.yaml"):
        """Initialize service"""
        logger.info("Initializing FRED Collector Service...")

        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Initialize Azure clients
        self.credential = DefaultAzureCredential()
        self.kv_client = None
        self.event_hub_client = None
        self.storage_key = None

        # Initialize Spark and Delta Lake
        self.spark = None
        self.delta_merger = None
        self.collector = None

        # Service state
        self.running = False
        self.shutdown_event = asyncio.Event()

        logger.info("Service configuration loaded")

    async def initialize(self):
        """Initialize all components"""
        logger.info("Initializing components...")

        # 1. Initialize Key Vault client
        kv_name = self.config["fred_api"]["key_vault"]["name"]
        self.kv_client = SecretClient(
            vault_url=f"https://{kv_name}.vault.azure.net/",
            credential=self.credential
        )
        logger.info(f"✅ Key Vault client initialized: {kv_name}")

        # 2. Get FRED API keys from Key Vault
        api_keys = await self._get_fred_api_keys()
        logger.info(f"✅ Retrieved {len(api_keys)} FRED API keys")

        # 2b. Get storage account key from Key Vault
        storage_key_secret = self.kv_client.get_secret("storage-account-key")
        self.storage_key = storage_key_secret.value
        logger.info("✅ Retrieved storage account key")

        # 3. Initialize Spark
        self.spark = self._create_spark_session()
        logger.info("✅ Spark session created")

        # 4. Initialize Delta Lake merger
        self.delta_merger = FREDDeltaMerger(
            spark=self.spark,
            storage_account=self.config["storage"]["account_name"],
            container=self.config["storage"]["container"]
        )
        logger.info("✅ Delta Lake merger initialized")

        # 5. Initialize collector
        self.collector = FREDCollectorV2(
            api_keys=api_keys,
            spark=self.spark,
            delta_merger=self.delta_merger
        )
        logger.info("✅ FRED collector initialized")

        # 6. Initialize Event Hub consumer (if enabled)
        if self.config["event_hub"]["enabled"]:
            await self._initialize_event_hub()
            logger.info("✅ Event Hub consumer initialized")

        logger.info("🚀 All components initialized successfully")

    async def _get_fred_api_keys(self) -> List[str]:
        """Retrieve FRED API keys from Azure Key Vault"""
        key_prefix = self.config["fred_api"]["key_vault"]["secret_prefix"]
        total_keys = self.config["fred_api"]["key_vault"]["total_keys"]

        api_keys = []
        for i in range(1, total_keys + 1):
            secret_name = f"{key_prefix}{i}"
            try:
                secret = self.kv_client.get_secret(secret_name)
                api_keys.append(secret.value)
                logger.debug(f"Retrieved key: {secret_name}")
            except Exception as e:
                logger.warning(f"Failed to retrieve {secret_name}: {str(e)}")

        if not api_keys:
            raise Exception("No FRED API keys found in Key Vault")

        return api_keys

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake and Azure configuration"""
        storage_account = self.config["storage"]["account_name"]
        spark_config = self.config["spark"]

        # Build packages string with Delta Lake and Hadoop Azure
        packages = [
            f"io.delta:delta-core_{spark_config['scala_version']}:{spark_config['delta_version']}",
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "com.microsoft.azure:azure-storage:8.6.6"
        ]

        spark = (
            SparkSession.builder
            .appName(spark_config["app_name"])
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", ",".join(packages))
            # Executor configuration
            .config("spark.executor.instances", spark_config["executor"]["instances"])
            .config("spark.executor.cores", spark_config["executor"]["cores"])
            .config("spark.executor.memory", spark_config["executor"]["memory"])
            # Driver configuration
            .config("spark.driver.cores", spark_config["driver"]["cores"])
            .config("spark.driver.memory", spark_config["driver"]["memory"])
            # Azure authentication with storage account key
            .config(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", self.storage_key)
            .getOrCreate()
        )

        return spark

    async def _initialize_event_hub(self):
        """Initialize Event Hub consumer client"""
        eh_namespace = self.config["event_hub"]["namespace"]
        topic = self.config["event_hub"]["topics"]["data_updated"]

        # Get connection string from Key Vault
        conn_string_secret = self.kv_client.get_secret("event-hub-connection-string")

        self.event_hub_client = EventHubConsumerClient.from_connection_string(
            conn_string_secret.value,  # First positional argument
            consumer_group="$Default",
            eventhub_name=topic
        )

    async def process_event_hub_message(self, partition_context, event):
        """
        Process Event Hub message for new release notification

        Expected message format:
        {
            "release_id": 441,
            "release_name": "Coinbase Cryptocurrencies",
            "update_date": "2025-11-18",
            "priority": "normal"
        }
        """
        try:
            import json
            message = json.loads(event.body_as_str())

            release_id = message["release_id"]
            logger.info(f"Received Event Hub message for release {release_id}")

            # Collect and store release
            result = await self.collector.collect_and_store_release(release_id)

            logger.info(f"Completed release {release_id}: {result['status']}")

            # Update checkpoint
            await partition_context.update_checkpoint(event)

        except Exception as e:
            logger.error(f"Error processing Event Hub message: {str(e)}")

    async def run_event_hub_listener(self):
        """Run Event Hub listener (background task)"""
        logger.info("Starting Event Hub listener...")

        try:
            async with self.event_hub_client:
                await self.event_hub_client.receive(
                    on_event=self.process_event_hub_message,
                    starting_position="-1"  # From beginning
                )
        except asyncio.CancelledError:
            logger.info("Event Hub listener cancelled")
        except Exception as e:
            logger.error(f"Event Hub listener error: {str(e)}")

    async def collect_all_releases_once(self):
        """
        One-time collection of all FRED releases
        Used for initial load or manual trigger
        """
        logger.info("Starting one-time collection of all releases...")

        # Load release IDs (in production, from release catalog or config)
        release_ids_file = os.getenv("RELEASE_IDS_FILE", "./release_ids.txt")

        if os.path.exists(release_ids_file):
            with open(release_ids_file, 'r') as f:
                release_ids = [int(line.strip()) for line in f if line.strip()]
        else:
            # Default: collect all 321 releases
            logger.warning(f"Release IDs file not found: {release_ids_file}")
            logger.info("Using default release list (9-1105)")
            release_ids = list(range(9, 1106))  # Approximate range

        logger.info(f"Collecting {len(release_ids)} releases...")

        results = await self.collector.collect_multiple_releases(
            release_ids=release_ids,
            parallel=False,  # Sequential for reliability
            max_concurrent=3
        )

        # Summary
        successful = sum(1 for r in results if r.get("status") == "success")
        failed = len(results) - successful

        logger.info(
            f"Collection complete: {successful}/{len(release_ids)} successful, "
            f"{failed} failed"
        )

        return results

    async def run(self):
        """Main service run loop"""
        self.running = True

        # Check run mode from environment
        run_mode = os.getenv("RUN_MODE", "event_hub")  # 'event_hub' or 'one_time'

        if run_mode == "one_time":
            # One-time collection
            logger.info("Running in ONE-TIME collection mode")
            await self.collect_all_releases_once()
            self.running = False

        elif run_mode == "event_hub":
            # Event Hub listener mode
            logger.info("Running in EVENT HUB listener mode")

            # Start Event Hub listener
            listener_task = asyncio.create_task(self.run_event_hub_listener())

            # Wait for shutdown signal
            await self.shutdown_event.wait()

            # Cancel listener
            listener_task.cancel()
            await listener_task

        else:
            raise ValueError(f"Unknown RUN_MODE: {run_mode}")

        logger.info("Service stopped")

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down service...")

        self.running = False
        self.shutdown_event.set()

        # Close Event Hub client
        if self.event_hub_client:
            await self.event_hub_client.close()

        # Stop Spark
        if self.spark:
            self.spark.stop()

        logger.info("Service shutdown complete")


# Global service instance
service = None


def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {sig}")
    if service:
        asyncio.create_task(service.shutdown())


async def main():
    """Main entry point"""
    global service

    logger.info("="*80)
    logger.info("FRED COLLECTOR SERVICE - STARTING")
    logger.info("="*80)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Create and initialize service
        service = FREDCollectorService()
        await service.initialize()

        # Run service
        await service.run()

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        if service:
            await service.shutdown()

    logger.info("Service exited")


if __name__ == "__main__":
    asyncio.run(main())
