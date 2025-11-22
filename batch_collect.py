"""
FRED Batch Collection Script
Sends all releases to Event Hub for gentle batch collection
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import httpx
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_all_releases(api_key: str) -> list:
    """Get all FRED releases"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            "https://api.stlouisfed.org/fred/releases",
            params={
                "api_key": api_key,
                "file_type": "json",
                "limit": 1000
            }
        )
        data = response.json()
        return data.get("releases", [])


async def send_batch_to_eventhub(
    producer: EventHubProducerClient,
    releases: list,
    batch_size: int = 5,
    delay_seconds: int = 30
):
    """
    Send releases to Event Hub in batches with delay

    Args:
        producer: Event Hub producer
        releases: List of release dicts
        batch_size: Releases per batch
        delay_seconds: Seconds to wait between batches
    """
    total = len(releases)
    sent = 0

    for i in range(0, total, batch_size):
        batch = releases[i:i + batch_size]

        # Create event batch
        event_batch = await producer.create_batch()

        for release in batch:
            event_data = {
                "release_id": release["id"],
                "release_name": release["name"],
                "update_date": datetime.now().strftime("%Y-%m-%d"),
                "priority": "batch",
                "batch_mode": True
            }
            event_batch.add(EventData(json.dumps(event_data)))

        # Send batch
        await producer.send_batch(event_batch)
        sent += len(batch)

        logger.info(
            f"📤 Sent batch {i//batch_size + 1}: "
            f"releases {i+1}-{min(i+batch_size, total)} "
            f"({sent}/{total} total)"
        )

        # Delay between batches (except last)
        if i + batch_size < total:
            logger.info(f"⏳ Waiting {delay_seconds}s before next batch...")
            await asyncio.sleep(delay_seconds)

    return sent


async def main():
    """Main entry point"""
    # Configuration
    KEY_VAULT_NAME = os.getenv("KEY_VAULT_NAME", "gzc-finma-keyvault")
    EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "fred-new-releases")

    # Batch settings (gentle mode)
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))  # 5 releases per batch
    DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "60"))  # 60s between batches
    START_FROM = int(os.getenv("START_FROM", "0"))  # Resume from release index

    logger.info("="*60)
    logger.info("FRED BATCH COLLECTION - GENTLE MODE")
    logger.info("="*60)
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Delay between batches: {DELAY_SECONDS}s")
    logger.info(f"Starting from index: {START_FROM}")

    # Initialize Azure credentials
    credential = DefaultAzureCredential()

    # Get secrets from Key Vault
    kv_client = SecretClient(
        vault_url=f"https://{KEY_VAULT_NAME}.vault.azure.net/",
        credential=credential
    )

    api_key = kv_client.get_secret("fred-api-key-1").value
    conn_string = kv_client.get_secret("event-hub-connection-string").value

    logger.info("✅ Retrieved secrets from Key Vault")

    # Get all releases
    logger.info("📥 Fetching all FRED releases...")
    releases = await get_all_releases(api_key)
    logger.info(f"✅ Found {len(releases)} releases")

    # Filter from start index
    releases = releases[START_FROM:]
    logger.info(f"📋 Will process {len(releases)} releases (starting from {START_FROM})")

    # Estimate time
    num_batches = (len(releases) + BATCH_SIZE - 1) // BATCH_SIZE
    est_minutes = (num_batches - 1) * DELAY_SECONDS / 60
    logger.info(f"⏱️  Estimated time: {est_minutes:.0f} minutes ({num_batches} batches)")

    # Confirm
    if os.getenv("AUTO_CONFIRM", "false").lower() != "true":
        confirm = input("\nProceed? (y/n): ")
        if confirm.lower() != "y":
            logger.info("Cancelled")
            return

    # Send to Event Hub
    async with EventHubProducerClient.from_connection_string(
        conn_str=conn_string,
        eventhub_name=EVENT_HUB_NAME
    ) as producer:

        sent = await send_batch_to_eventhub(
            producer=producer,
            releases=releases,
            batch_size=BATCH_SIZE,
            delay_seconds=DELAY_SECONDS
        )

    logger.info("="*60)
    logger.info(f"✅ BATCH COMPLETE: {sent} releases queued")
    logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
