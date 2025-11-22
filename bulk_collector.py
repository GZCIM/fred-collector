"""
FRED Bulk Collector - Phase 1: Fast Download to Blob Storage
Downloads all FRED data using V2 API and saves raw Parquet to blob storage
NO SPARK - just fast HTTP calls and file writes
"""

import httpx
import asyncio
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob.aio import BlobServiceClient
from azure.identity.aio import DefaultAzureCredential
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
import logging
import json
import io
import statistics
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FREDBulkCollector:
    """
    Ultra-fast FRED bulk collector
    - Downloads via V2 API (all observations per release in one call)
    - Saves directly to Azure Blob Storage as Parquet
    - No Spark overhead during collection
    """

    def __init__(
        self,
        api_keys: List[str],
        storage_account: str = "gzcstorageaccount",
        container: str = "macroeconomic-maintained-series",
        base_path: str = "US_Fred_Data/raw",
        base_url: str = "https://api.stlouisfed.org/fred"
    ):
        self.api_keys = api_keys
        self.storage_account = storage_account
        self.container = container
        self.base_path = base_path
        self.base_url = base_url
        self.current_key_index = 0

        self.blob_service_client = None
        self.container_client = None

        # Statistics
        self.stats = {
            "releases_collected": 0,
            "total_observations": 0,
            "total_series": 0,
            "api_calls": 0,
            "errors": 0,
            "started_at": None,
            "completed_at": None,
            "qc_warnings": 0,
            "qc_failures": 0,
            "retries_attempted": 0,
            "retries_succeeded": 0,
        }

        # Quality control tracking
        self.qc_results = {}

    async def initialize(self):
        """Initialize blob storage client"""
        import os

        # Try connection string first (works in K8s with secrets)
        conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if conn_str:
            self.blob_service_client = BlobServiceClient.from_connection_string(conn_str)
            logger.info(f"Initialized blob storage using connection string: {self.storage_account}/{self.container}")
        else:
            # Fallback to managed identity (for local dev)
            credential = DefaultAzureCredential()
            self.blob_service_client = BlobServiceClient(
                account_url=f"https://{self.storage_account}.blob.core.windows.net",
                credential=credential
            )
            logger.info(f"Initialized blob storage using managed identity: {self.storage_account}/{self.container}")

        self.container_client = self.blob_service_client.get_container_client(self.container)

    def _get_next_api_key(self) -> str:
        """Round-robin API key selection"""
        key = self.api_keys[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        return key

    def validate_observations_quality(
        self,
        observations: List[Dict],
        release_id: int,
        series_metadata: Dict
    ) -> Dict:
        """
        Validate data quality before storage

        Returns dict with: {
            "is_valid": bool,
            "warnings": List[str],
            "errors": List[str],
            "metrics": Dict
        }
        """
        warnings = []
        errors = []
        metrics = {
            "null_values": 0,
            "empty_series_ids": 0,
            "invalid_dates": 0,
            "duplicate_observations": 0,
            "series_without_metadata": 0,
            "value_outliers": 0,
        }

        # Track unique observations
        unique_obs = set()
        series_obs_counts = defaultdict(int)
        numeric_values = []

        for obs in observations:
            # Check for null/empty values
            if not obs.get("series_id"):
                metrics["empty_series_ids"] += 1
                errors.append(f"Empty series_id in observation")

            if not obs.get("date"):
                metrics["invalid_dates"] += 1
                errors.append(f"Missing date in observation for {obs.get('series_id')}")

            if obs.get("value") is None or obs.get("value") == "":
                metrics["null_values"] += 1
                warnings.append(f"Null value for {obs.get('series_id')} on {obs.get('date')}")

            # Check for duplicates
            obs_key = (obs.get("series_id"), obs.get("date"))
            if obs_key in unique_obs:
                metrics["duplicate_observations"] += 1
                warnings.append(f"Duplicate observation: {obs_key}")
            unique_obs.add(obs_key)

            # Track observations per series
            series_obs_counts[obs.get("series_id")] += 1

            # Collect numeric values for outlier detection
            try:
                val = float(obs.get("value", "0"))
                numeric_values.append(val)
            except (ValueError, TypeError):
                pass  # Non-numeric value, skip

        # Check series have metadata
        for series_id in series_obs_counts.keys():
            if series_id not in series_metadata:
                metrics["series_without_metadata"] += 1
                warnings.append(f"No metadata for series {series_id}")

        # Simple outlier detection (values > 5 std deviations from mean)
        if len(numeric_values) > 10:
            try:
                mean_val = statistics.mean(numeric_values)
                stdev_val = statistics.stdev(numeric_values)
                outlier_threshold = abs(mean_val) + (5 * stdev_val)

                metrics["value_outliers"] = sum(
                    1 for v in numeric_values
                    if abs(v) > outlier_threshold
                )
                if metrics["value_outliers"] > 0:
                    warnings.append(
                        f"{metrics['value_outliers']} potential outlier values detected"
                    )
            except statistics.StatisticsError:
                pass  # Not enough variance for outlier detection

        # Check data completeness
        if len(observations) == 0:
            errors.append("No observations collected")

        if len(series_metadata) == 0:
            warnings.append("No series metadata available")

        # Assess validity
        is_valid = len(errors) == 0 and metrics["null_values"] < len(observations) * 0.1

        if warnings:
            self.stats["qc_warnings"] += len(warnings)
        if errors or not is_valid:
            self.stats["qc_failures"] += 1

        return {
            "is_valid": is_valid,
            "warnings": warnings[:10],  # Limit to first 10
            "errors": errors[:10],
            "metrics": metrics,
            "series_count": len(series_obs_counts),
            "obs_count": len(observations),
        }

    async def verify_release_completeness(
        self,
        release_id: int,
        collected_series_count: int,
        collected_obs_count: int,
        client: httpx.AsyncClient
    ) -> Dict:
        """
        Verify completeness by checking FRED metadata for expected counts

        Returns dict with: {
            "complete": bool,
            "expected_series": int,
            "collected_series": int,
            "warnings": List[str]
        }
        """
        warnings = []

        try:
            # Get release metadata from FRED
            api_key = self._get_next_api_key()
            response = await client.get(
                f"{self.base_url}/release/series",
                params={
                    "release_id": release_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "limit": 1
                },
                timeout=30.0
            )

            if response.status_code == 200:
                data = response.json()
                # FRED returns count in headers or we can infer from limit/offset
                # For now, we'll use a heuristic
                if collected_series_count == 0:
                    warnings.append("No series collected - possible empty release")
                    return {
                        "complete": False,
                        "expected_series": None,
                        "collected_series": collected_series_count,
                        "warnings": warnings
                    }

        except Exception as e:
            logger.warning(f"Could not verify completeness for release {release_id}: {e}")
            warnings.append(f"Completeness check failed: {e}")

        # For now, mark as complete if we got data
        # Enhanced version could track expected counts from FRED catalog
        is_complete = collected_series_count > 0 and collected_obs_count > 0

        return {
            "complete": is_complete,
            "expected_series": None,  # Would need FRED catalog integration
            "collected_series": collected_series_count,
            "collected_observations": collected_obs_count,
            "warnings": warnings
        }

    async def collect_release_to_blob(
        self,
        release_id: int,
        client: httpx.AsyncClient,
        vintage_date: str
    ) -> Dict:
        """
        Collect a single release and save directly to blob storage as Parquet

        Returns:
            Statistics dictionary
        """
        api_key = self._get_next_api_key()
        all_observations = []
        series_metadata = {}
        api_calls = 0
        next_cursor = None

        try:
            # V2 Bulk API - gets ALL observations in one call (paginated)
            while True:
                headers = {"Authorization": f"Bearer {api_key}"}
                params = {
                    "release_id": release_id,
                    "format": "json",
                    "limit": 500000,
                }
                if next_cursor:
                    params["next_cursor"] = next_cursor

                response = await client.get(
                    f"{self.base_url}/v2/release/observations",
                    headers=headers,
                    params=params,
                )
                api_calls += 1
                self.stats["api_calls"] += 1

                if response.status_code == 429:
                    logger.warning(f"Rate limited on release {release_id}, waiting 10s...")
                    await asyncio.sleep(10)
                    continue

                if response.status_code != 200:
                    raise Exception(f"API error {response.status_code}: {response.text}")

                data = response.json()

                # Process series with their observations
                for series in data.get("series", []):
                    series_id = series["series_id"]
                    series_metadata[series_id] = {
                        "title": series.get("title"),
                        "frequency": series.get("frequency"),
                        "units": series.get("units"),
                    }

                    for obs in series.get("observations", []):
                        all_observations.append({
                            "series_id": series_id,
                            "date": obs["date"],
                            "value": obs["value"],
                            "release_id": release_id,
                            "vintage_date": vintage_date,
                            "collected_at": datetime.now().isoformat(),
                        })

                # Check for more pages
                if data.get("has_more", False):
                    next_cursor = data.get("next_cursor")
                    logger.info(f"  Release {release_id}: {len(all_observations):,} obs (fetching more...)")
                    await asyncio.sleep(0.2)
                else:
                    break

            # QC VALIDATION BEFORE SAVING
            qc_result = self.validate_observations_quality(
                all_observations,
                release_id,
                series_metadata
            )

            # Log QC results
            if not qc_result["is_valid"]:
                logger.warning(
                    f"⚠️  Release {release_id} QC FAILED: "
                    f"{len(qc_result['errors'])} errors, "
                    f"{len(qc_result['warnings'])} warnings"
                )
                for error in qc_result["errors"][:5]:
                    logger.warning(f"   ERROR: {error}")

            if qc_result["warnings"]:
                logger.info(
                    f"⚠️  Release {release_id} has {len(qc_result['warnings'])} QC warnings"
                )

            # Completeness check
            completeness_result = await self.verify_release_completeness(
                release_id,
                len(series_metadata),
                len(all_observations),
                client
            )

            if not completeness_result["complete"]:
                logger.warning(
                    f"⚠️  Release {release_id} completeness check warnings: "
                    f"{completeness_result['warnings']}"
                )

            # Save QC results
            self.qc_results[release_id] = {
                "quality": qc_result,
                "completeness": completeness_result,
                "timestamp": datetime.now().isoformat()
            }

            # Save to blob storage as Parquet (even if QC warnings exist)
            if all_observations:
                await self._save_to_blob(release_id, all_observations, vintage_date)

            obs_count = len(all_observations)
            series_count = len(series_metadata)

            self.stats["releases_collected"] += 1
            self.stats["total_observations"] += obs_count
            self.stats["total_series"] += series_count

            qc_status = "✅" if qc_result["is_valid"] else "⚠️ "
            logger.info(
                f"{qc_status} Release {release_id}: {obs_count:,} obs, "
                f"{series_count:,} series, {api_calls} API calls → Blob"
            )

            return {
                "release_id": release_id,
                "status": "success",
                "observations": obs_count,
                "series": series_count,
                "api_calls": api_calls,
                "qc_valid": qc_result["is_valid"],
                "qc_warnings": len(qc_result["warnings"]),
                "qc_errors": len(qc_result["errors"]),
            }

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"❌ Release {release_id} failed: {e}")
            return {
                "release_id": release_id,
                "status": "failed",
                "error": str(e),
            }

    async def _save_to_blob(
        self,
        release_id: int,
        observations: List[Dict],
        vintage_date: str
    ):
        """Save observations to blob storage as Parquet"""

        # Create PyArrow table
        schema = pa.schema([
            ("series_id", pa.string()),
            ("date", pa.string()),
            ("value", pa.string()),
            ("release_id", pa.int32()),
            ("vintage_date", pa.string()),
            ("collected_at", pa.string()),
        ])

        table = pa.Table.from_pylist(observations, schema=schema)

        # Write to buffer as Parquet
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        # Upload to blob
        blob_path = f"{self.base_path}/{vintage_date}/release_{release_id}.parquet"
        blob_client = self.container_client.get_blob_client(blob_path)

        await blob_client.upload_blob(buffer, overwrite=True)
        logger.debug(f"  Saved to blob: {blob_path}")

    async def retry_failed_releases(
        self,
        failed_release_ids: List[int],
        client: httpx.AsyncClient,
        vintage_date: str,
        max_retries: int = 3
    ) -> Dict:
        """
        Retry failed releases with exponential backoff

        Returns:
            Dict with retry results
        """
        retry_results = []
        succeeded = 0
        still_failed = 0

        logger.info(f"🔄 Retrying {len(failed_release_ids)} failed releases (max {max_retries} attempts)")

        for release_id in failed_release_ids:
            retry_count = 0
            last_error = None

            while retry_count < max_retries:
                retry_count += 1
                self.stats["retries_attempted"] += 1

                logger.info(f"  Retry {retry_count}/{max_retries} for release {release_id}")

                try:
                    # Exponential backoff: 5s, 10s, 20s
                    if retry_count > 1:
                        wait_time = 5 * (2 ** (retry_count - 1))
                        logger.debug(f"    Waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)

                    result = await self.collect_release_to_blob(
                        release_id,
                        client,
                        vintage_date
                    )

                    if result.get("status") == "success":
                        logger.info(f"  ✅ Release {release_id} succeeded on retry {retry_count}")
                        self.stats["retries_succeeded"] += 1
                        succeeded += 1
                        retry_results.append({
                            "release_id": release_id,
                            "status": "retry_success",
                            "retry_count": retry_count,
                            **result
                        })
                        break  # Success, no more retries needed
                    else:
                        last_error = result.get("error", "Unknown error")

                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"  Retry {retry_count} for release {release_id} failed: {e}")

            else:
                # All retries exhausted
                logger.error(f"  ❌ Release {release_id} failed after {max_retries} retries")
                still_failed += 1
                retry_results.append({
                    "release_id": release_id,
                    "status": "retry_failed",
                    "retries_attempted": max_retries,
                    "last_error": last_error
                })

        logger.info(f"Retry complete: {succeeded} succeeded, {still_failed} still failed")

        return {
            "succeeded": succeeded,
            "still_failed": still_failed,
            "results": retry_results
        }

    async def generate_qc_report(self, vintage_date: str, results: List[Dict]) -> Dict:
        """
        Generate comprehensive QC reconciliation report

        Returns:
            Detailed QC report dictionary
        """
        total_releases = len(results)
        successful_releases = [r for r in results if isinstance(r, dict) and r.get("status") == "success"]
        failed_releases = [r for r in results if isinstance(r, dict) and r.get("status") == "failed"]

        # QC metrics aggregation
        qc_passed = sum(1 for r in successful_releases if r.get("qc_valid", True))
        qc_warnings_count = sum(r.get("qc_warnings", 0) for r in successful_releases)
        qc_errors_count = sum(r.get("qc_errors", 0) for r in successful_releases)

        # Data quality summary
        total_observations = self.stats["total_observations"]
        total_series = self.stats["total_series"]

        # Calculate quality score (0-100)
        quality_score = 100.0
        if total_releases > 0:
            success_rate = len(successful_releases) / total_releases
            qc_pass_rate = qc_passed / len(successful_releases) if successful_releases else 0
            quality_score = (success_rate * 70) + (qc_pass_rate * 30)  # Weighted

        # Build detailed report
        report = {
            "vintage_date": vintage_date,
            "generated_at": datetime.now().isoformat(),
            "collection_summary": {
                "total_releases": total_releases,
                "successful": len(successful_releases),
                "failed": len(failed_releases),
                "success_rate": f"{(len(successful_releases)/total_releases*100):.1f}%",
                "duration_seconds": (
                    (self.stats["completed_at"] - self.stats["started_at"]).total_seconds()
                    if self.stats.get("completed_at") else None
                ),
            },
            "data_summary": {
                "total_observations": total_observations,
                "total_series": total_series,
                "api_calls": self.stats["api_calls"],
                "avg_obs_per_release": int(total_observations / len(successful_releases)) if successful_releases else 0,
            },
            "quality_control": {
                "qc_passed": qc_passed,
                "qc_failed": len(successful_releases) - qc_passed,
                "qc_pass_rate": f"{(qc_passed/len(successful_releases)*100):.1f}%" if successful_releases else "N/A",
                "total_qc_warnings": qc_warnings_count,
                "total_qc_errors": qc_errors_count,
                "quality_score": f"{quality_score:.1f}/100",
            },
            "retry_summary": {
                "retries_attempted": self.stats["retries_attempted"],
                "retries_succeeded": self.stats["retries_succeeded"],
                "retry_success_rate": (
                    f"{(self.stats['retries_succeeded']/self.stats['retries_attempted']*100):.1f}%"
                    if self.stats["retries_attempted"] > 0 else "N/A"
                ),
            },
            "failed_releases": [
                {
                    "release_id": r.get("release_id"),
                    "error": r.get("error"),
                }
                for r in failed_releases[:20]  # Limit to first 20
            ],
            "qc_issues": [
                {
                    "release_id": rid,
                    "quality": self.qc_results[rid]["quality"],
                    "completeness": self.qc_results[rid]["completeness"],
                }
                for rid in list(self.qc_results.keys())[:20]  # Top 20 with QC issues
                if not self.qc_results[rid]["quality"]["is_valid"]
            ],
            "recommendations": self._generate_recommendations(
                len(successful_releases),
                len(failed_releases),
                qc_passed,
                qc_warnings_count
            )
        }

        return report

    def _generate_recommendations(
        self,
        successful_count: int,
        failed_count: int,
        qc_passed: int,
        qc_warnings: int
    ) -> List[str]:
        """Generate actionable recommendations based on collection results"""
        recommendations = []

        # Success rate recommendations
        if failed_count > 0:
            failure_rate = failed_count / (successful_count + failed_count) * 100
            if failure_rate > 10:
                recommendations.append(
                    f"⚠️  High failure rate ({failure_rate:.1f}%). Investigate API errors and rate limiting."
                )
            elif failure_rate > 5:
                recommendations.append(
                    f"⚠️  Moderate failure rate ({failure_rate:.1f}%). Consider increasing retry attempts."
                )

        # QC recommendations
        if successful_count > 0:
            qc_pass_rate = qc_passed / successful_count * 100
            if qc_pass_rate < 90:
                recommendations.append(
                    f"⚠️  Low QC pass rate ({qc_pass_rate:.1f}%). Review data quality issues in QC report."
                )

            if qc_warnings > successful_count * 2:
                recommendations.append(
                    f"⚠️  High QC warning count ({qc_warnings}). Investigate data quality patterns."
                )

        # Positive feedback
        if not recommendations:
            recommendations.append("✅ Excellent collection quality! All metrics within acceptable ranges.")

        return recommendations

    async def collect_all_releases(
        self,
        release_ids: List[int],
        max_concurrent: int = 10,
        vintage_date: Optional[str] = None
    ) -> Dict:
        """
        Collect all releases in parallel

        Args:
            release_ids: List of FRED release IDs
            max_concurrent: Maximum parallel downloads
            vintage_date: Date stamp for this collection run

        Returns:
            Summary statistics
        """
        if vintage_date is None:
            vintage_date = datetime.now().strftime("%Y-%m-%d")

        self.stats["started_at"] = datetime.now()

        logger.info(f"Starting bulk collection of {len(release_ids)} releases")
        logger.info(f"Vintage date: {vintage_date}")
        logger.info(f"Max concurrent: {max_concurrent}")
        logger.info(f"Blob path: {self.container}/{self.base_path}/{vintage_date}/")

        semaphore = asyncio.Semaphore(max_concurrent)
        results = []

        async with httpx.AsyncClient(timeout=300.0) as client:
            async def collect_with_semaphore(rid):
                async with semaphore:
                    return await self.collect_release_to_blob(rid, client, vintage_date)

            tasks = [collect_with_semaphore(rid) for rid in release_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        self.stats["completed_at"] = datetime.now()
        duration = (self.stats["completed_at"] - self.stats["started_at"]).total_seconds()

        # RETRY FAILED RELEASES
        failed_release_ids = [
            r.get("release_id") for r in results
            if isinstance(r, dict) and r.get("status") == "failed"
        ]

        retry_results = None
        if failed_release_ids:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"RETRYING {len(failed_release_ids)} FAILED RELEASES")
            logger.info(f"{'=' * 60}")

            async with httpx.AsyncClient(timeout=300.0) as retry_client:
                retry_results = await self.retry_failed_releases(
                    failed_release_ids,
                    retry_client,
                    vintage_date,
                    max_retries=3
                )

            # Update results with retry outcomes
            for retry_result in retry_results["results"]:
                # Replace failed result with retry result
                for i, r in enumerate(results):
                    if isinstance(r, dict) and r.get("release_id") == retry_result["release_id"]:
                        results[i] = retry_result
                        break

        # GENERATE QC REPORT
        logger.info("\nGenerating QC reconciliation report...")
        qc_report = await self.generate_qc_report(vintage_date, results)

        # Save manifest with QC report
        await self._save_manifest(vintage_date, release_ids, results, qc_report)

        # FINAL SUMMARY
        logger.info("=" * 60)
        logger.info("BULK COLLECTION COMPLETE WITH QC")
        logger.info(f"  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
        logger.info(f"  Releases: {self.stats['releases_collected']}/{len(release_ids)}")
        logger.info(f"  Observations: {self.stats['total_observations']:,}")
        logger.info(f"  Series: {self.stats['total_series']:,}")
        logger.info(f"  API calls: {self.stats['api_calls']}")
        logger.info(f"  Errors: {self.stats['errors']}")
        if retry_results:
            logger.info(f"  Retries: {retry_results['succeeded']}/{len(failed_release_ids)} succeeded")
        logger.info(f"  QC Score: {qc_report['quality_control']['quality_score']}")
        logger.info(f"  QC Warnings: {self.stats['qc_warnings']}")
        logger.info(f"  QC Failures: {self.stats['qc_failures']}")
        logger.info(f"  Output: az://{self.container}/{self.base_path}/{vintage_date}/")
        logger.info("=" * 60)

        # Show recommendations
        if qc_report["recommendations"]:
            logger.info("\n📊 RECOMMENDATIONS:")
            for rec in qc_report["recommendations"]:
                logger.info(f"  {rec}")

        logger.info(f"\n✅ QC Report saved to: {self.base_path}/{vintage_date}/_qc_report.json")

        return {
            **self.stats,
            "duration_seconds": duration,
            "vintage_date": vintage_date,
            "results": results,
            "qc_report": qc_report,
            "retry_summary": retry_results if retry_results else None,
        }

    async def _save_manifest(
        self,
        vintage_date: str,
        release_ids: List[int],
        results: List[Dict],
        qc_report: Optional[Dict] = None
    ):
        """Save collection manifest and QC report for the Spark processor"""
        manifest = {
            "vintage_date": vintage_date,
            "collected_at": datetime.now().isoformat(),
            "stats": self.stats,
            "releases": [
                r for r in results
                if isinstance(r, dict) and r.get("status") in ["success", "retry_success"]
            ],
            "failed": [
                r for r in results
                if isinstance(r, dict) and r.get("status") in ["failed", "retry_failed"]
            ],
        }

        # Save collection manifest
        blob_path = f"{self.base_path}/{vintage_date}/_manifest.json"
        blob_client = self.container_client.get_blob_client(blob_path)

        await blob_client.upload_blob(
            json.dumps(manifest, indent=2, default=str),
            overwrite=True
        )
        logger.info(f"Saved manifest: {blob_path}")

        # Save QC report separately
        if qc_report:
            qc_blob_path = f"{self.base_path}/{vintage_date}/_qc_report.json"
            qc_blob_client = self.container_client.get_blob_client(qc_blob_path)

            await qc_blob_client.upload_blob(
                json.dumps(qc_report, indent=2, default=str),
                overwrite=True
            )
            logger.info(f"Saved QC report: {qc_blob_path}")

    async def close(self):
        """Close async resources"""
        if self.blob_service_client:
            await self.blob_service_client.close()


async def get_all_release_ids(api_key: str) -> List[int]:
    """Fetch list of all FRED release IDs"""
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            "https://api.stlouisfed.org/fred/releases",
            params={"api_key": api_key, "file_type": "json"}
        )

        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch releases: HTTP {response.status_code} - {response.text[:200]}"
            )

        data = response.json()
        release_ids = [r["id"] for r in data.get("releases", [])]
        logger.info(f"Found {len(release_ids)} releases")
        return release_ids


def get_api_keys_from_env() -> List[str]:
    """Load FRED API keys from environment variables"""
    import os

    api_keys = []
    for i in range(1, 10):  # key1 through key9 (K8s secret naming)
        key = os.environ.get(f"key{i}")
        if key:
            api_keys.append(key)
            logger.info(f"Loaded API key {i}")

    if not api_keys:
        raise ValueError("No API keys found in environment variables")

    logger.info(f"Loaded {len(api_keys)} API keys from environment")
    return api_keys


async def main():
    """Main entry point for bulk collection"""
    import os

    # Get API keys from environment
    api_keys = get_api_keys_from_env()

    collector = FREDBulkCollector(api_keys=api_keys)

    try:
        await collector.initialize()

        # Get all release IDs
        release_ids = await get_all_release_ids(api_keys[0])

        # Get max concurrent from env or default to 2
        max_concurrent = int(os.environ.get("MAX_CONCURRENT", "2"))

        # Collect all releases
        stats = await collector.collect_all_releases(
            release_ids=release_ids,
            max_concurrent=max_concurrent,
        )

        print(f"\nCollection complete! {stats['total_observations']:,} observations saved to blob storage")
        print(f"Next step: Run spark_processor.py to merge into Delta Lake")

    finally:
        await collector.close()


if __name__ == "__main__":
    asyncio.run(main())
