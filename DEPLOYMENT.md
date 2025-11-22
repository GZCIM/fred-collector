# FRED Collector - Deployment Guide & Critical Fixes

**Status:** Operational (Build ca43)
**Last Updated:** 2025-11-20
**Kubernetes Cluster:** gzc-k8s-engine
**Namespace:** fred-pipeline

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Critical Fixes (4 Total)](#critical-fixes)
3. [Container Build](#container-build)
4. [Kubernetes Deployment](#kubernetes-deployment)
5. [Configuration](#configuration)
6. [Verification](#verification)
7. [Troubleshooting](#troubleshooting)
8. [Rollback Procedures](#rollback-procedures)

---

## Quick Start

```bash
# 1. Build container
cd /Users/mikaeleage/Research\ \&\ Analytics\ Services/Projects/azure-data-pipelines/services/fred-collector
docker build -t gzcacr.azurecr.io/fred-collector:ca43 .

# 2. Push to ACR
az acr login --name gzcacr
docker push gzcacr.azurecr.io/fred-collector:ca43

# 3. Deploy to Kubernetes
kubectl apply -f ../../kubernetes/deployments/fred-collector.yaml

# 4. Verify
kubectl get pods -n fred-pipeline -l app=fred-collector
kubectl logs -f deployment/fred-collector -n fred-pipeline
```

---

## Critical Fixes

This deployment includes **4 critical fixes** that were discovered and resolved during development. These fixes are **REQUIRED** for the service to function correctly in production.

### Fix 1: PySpark 3.4.1 Typing.io Bug (SPARK-44169) ✅

**Build:** ca41
**Status:** Implemented
**Severity:** CRITICAL - Workers crash without this fix

#### Problem

PySpark 3.4.1 has a known bug where `pyspark/broadcast.py` imports `BinaryIO` from the non-existent `typing.io` module instead of the correct `typing` module:

```python
# Wrong (PySpark 3.4.1 default)
from typing.io import BinaryIO  # ❌ Module 'typing' has no attribute 'io'

# Correct
from typing import BinaryIO     # ✅ Works correctly
```

This bug is documented as **SPARK-44169** and was fixed in later PySpark versions, but 3.4.1 still has it.

#### Impact

- Spark workers crash when trying to deserialize broadcast variables
- Error message: `AttributeError: module 'typing' has no attribute 'io'`
- Collection completely fails after Spark session initialization

#### Solution

The fix requires patching **inside the pyspark.zip archive** that workers load from, not just the loose source files.

**Dockerfile (lines 37-51):**

```dockerfile
# Fix PySpark 3.4.1 typing.io bug (SPARK-44169)
# CRITICAL: Must patch INSIDE pyspark.zip which workers load from
RUN cd /tmp && \
    # Extract the zip
    unzip /usr/local/lib/python3.10/site-packages/pyspark/python/lib/pyspark.zip -d pyspark_extracted && \
    # Patch broadcast.py inside the extracted files
    sed -i 's/from typing\.io import BinaryIO/from typing import BinaryIO/' pyspark_extracted/pyspark/broadcast.py && \
    # Delete the old zip
    rm /usr/local/lib/python3.10/site-packages/pyspark/python/lib/pyspark.zip && \
    # Create new zip with the fix
    cd pyspark_extracted && zip -r /usr/local/lib/python3.10/site-packages/pyspark/python/lib/pyspark.zip . && \
    # Cleanup
    cd /tmp && rm -rf pyspark_extracted && \
    # Also patch the loose source file for good measure
    sed -i 's/from typing\.io import BinaryIO/from typing import BinaryIO/' /usr/local/lib/python3.10/site-packages/pyspark/broadcast.py
```

#### Verification

After container starts:

```bash
# Check if patch was applied
kubectl exec -it <pod-name> -n fred-pipeline -- \
  unzip -p /usr/local/lib/python3.10/site-packages/pyspark/python/lib/pyspark.zip pyspark/broadcast.py | grep "from typing import BinaryIO"

# Should output: from typing import BinaryIO
```

---

### Fix 2: Python 3.10 Explicit Paths ✅

**Build:** ca41
**Status:** Implemented
**Severity:** HIGH - Workers default to Python 2.7 without this

#### Problem

Spark workers couldn't find Python 3.10 executable and were defaulting to system Python (often 2.7), causing:
- Import errors for Python 3 syntax
- Worker initialization failures
- Serialization/deserialization errors

#### Solution

Explicitly set Python paths in Dockerfile:

**Dockerfile (lines 62-64):**

```dockerfile
# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=/usr/local/bin/python3.10 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10
```

#### Verification

```bash
kubectl exec -it <pod-name> -n fred-pipeline -- env | grep PYSPARK_PYTHON
# Should output:
# PYSPARK_PYTHON=/usr/local/bin/python3.10
# PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10
```

---

### Fix 3: Delta Lake Table Auto-Initialization ✅

**Build:** ca42
**Status:** Implemented
**Severity:** MEDIUM - First run fails without this

#### Problem

Delta Lake merge operations require the target table to exist. On first run, tables don't exist yet, causing:
- `AnalysisException: Table or view not found`
- Collection fails even though API calls succeed
- No way to bootstrap the initial tables

#### Solution

Added `_ensure_table_exists()` method that creates Delta tables on-demand before merge operations.

**delta_lake/merge_logic.py (lines 75-107):**

```python
def _ensure_table_exists(self, table_name: str):
    """Ensure Delta table exists, create if it doesn't"""
    table_path = self._get_table_path(table_name)

    if self._table_exists(table_path):
        return  # Table exists, nothing to do

    logger.info(f"Creating Delta table: {table_name} at {table_path}")

    # Import schemas
    from schemas import get_schema, DELTA_TABLE_PROPERTIES, SERIES_OBSERVATIONS_PARTITION_COLUMNS

    # Get schema for table
    schema = get_schema(table_name)

    # Create empty DataFrame with schema
    empty_df = self.spark.createDataFrame([], schema=schema)

    # Write as Delta table with partitioning and properties
    writer = empty_df.write.format("delta")

    # Add partitioning for series_observations
    if table_name == "series_observations":
        writer = writer.partitionBy(*SERIES_OBSERVATIONS_PARTITION_COLUMNS)

    # Add Delta properties
    for key, value in DELTA_TABLE_PROPERTIES.items():
        writer = writer.option(key, value)

    # Save table
    writer.save(table_path)

    logger.info(f"✅ Created Delta table: {table_name}")
```

This method is called at the start of every merge operation:

```python
def merge_series_observations(self, new_data, vintage_date, release_id, etl_run_id, api_version="v2"):
    # Ensure table exists
    self._ensure_table_exists("series_observations")  # Auto-creates if needed

    # Continue with merge...
```

#### Benefits

- Zero-downtime first deployment
- No manual table creation required
- Safe idempotent operations (checks existence first)
- Consistent table structure across all environments

---

### Fix 4: Remove Databricks-Specific Delta Lake Properties ✅

**Build:** ca43
**Status:** Implemented
**Severity:** LOW - Warnings in logs, doesn't break functionality

#### Problem

Original code used Databricks-specific Delta Lake properties that aren't available in open-source Delta Lake:

```python
# ❌ WRONG - Databricks-only properties
DELTA_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
}
```

This caused:
- Warning messages in logs
- Properties silently ignored
- Confusion about why auto-optimization wasn't working

#### Solution

Removed Databricks-specific properties, keeping only open-source Delta Lake properties.

**delta_lake/schemas.py (lines 159-163):**

```python
# Delta Lake properties for all tables
# Note: Using only open-source Delta Lake properties (not Databricks-specific)
DELTA_TABLE_PROPERTIES = {
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
    # Removed Databricks-only properties (autoOptimize.optimizeWrite, autoOptimize.autoCompact)
}
```

#### Manual Optimization

For optimization, use explicit commands instead:

```python
# Run OPTIMIZE manually or on schedule
delta_merger.optimize_tables()

# Run VACUUM to clean up old files
delta_merger.vacuum_tables(retention_hours=168)
```

---

## Container Build

### Prerequisites

- Docker installed and running
- Azure CLI authenticated (`az login`)
- Access to Azure Container Registry: `gzcacr.azurecr.io`

### Build Process

```bash
# Navigate to service directory
cd /Users/mikaeleage/Research\ \&\ Analytics\ Services/Projects/azure-data-pipelines/services/fred-collector

# Build with build tag
docker build -t gzcacr.azurecr.io/fred-collector:ca43 .

# Also tag as latest
docker tag gzcacr.azurecr.io/fred-collector:ca43 gzcacr.azurecr.io/fred-collector:latest

# Login to ACR
az acr login --name gzcacr

# Push both tags
docker push gzcacr.azurecr.io/fred-collector:ca43
docker push gzcacr.azurecr.io/fred-collector:latest
```

### Build Verification

```bash
# Test locally (requires Azure credentials)
docker run --rm -e RUN_MODE=one_time \
  -e AZURE_CLIENT_ID=ae954be9-9a46-4085-849a-191d8884119c \
  gzcacr.azurecr.io/fred-collector:ca43

# Check layers
docker history gzcacr.azurecr.io/fred-collector:ca43
```

---

## Kubernetes Deployment

### Prerequisites

- `kubectl` configured for `gzc-k8s-engine` cluster
- Namespace `fred-pipeline` exists
- ConfigMaps and Secrets configured

### Deployment Steps

**1. Create Namespace (if needed)**

```bash
kubectl create namespace fred-pipeline
```

**2. Create ConfigMap**

```bash
kubectl create configmap fred-collector-config \
  --from-file=config.yaml=./delta_lake/config.yaml \
  -n fred-pipeline
```

**3. Create Secrets**

```bash
# Storage account key
kubectl create secret generic storage-secrets \
  --from-literal=storage-account-key="<storage-key>" \
  -n fred-pipeline

# Event Hub connection string
kubectl create secret generic event-hub-secrets \
  --from-literal=connection-string="<event-hub-connection>" \
  -n fred-pipeline
```

**4. Deploy Service**

```bash
kubectl apply -f ../../kubernetes/deployments/fred-collector.yaml
```

**5. Verify Deployment**

```bash
# Check pods
kubectl get pods -n fred-pipeline -l app=fred-collector

# Check logs
kubectl logs -f deployment/fred-collector -n fred-pipeline

# Check events
kubectl get events -n fred-pipeline --sort-by='.lastTimestamp'
```

### Deployment Manifest

**Key sections from `kubernetes/deployments/fred-collector.yaml`:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fred-collector
  namespace: fred-pipeline
spec:
  replicas: 2
  selector:
    matchLabels:
      app: fred-collector
  template:
    spec:
      containers:
      - name: fred-collector
        image: gzcacr.azurecr.io/fred-collector:ca43
        env:
        - name: RUN_MODE
          value: "event_hub"  # or "one_time"
        - name: AZURE_CLIENT_ID
          value: "ae954be9-9a46-4085-849a-191d8884119c"
        resources:
          requests:
            cpu: "2"
            memory: "8Gi"
          limits:
            cpu: "4"
            memory: "16Gi"
        volumeMounts:
        - name: config
          mountPath: /app/delta_lake/config.yaml
          subPath: config.yaml
      volumes:
      - name: config
        configMap:
          name: fred-collector-config
```

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUN_MODE` | Yes | `event_hub` | Run mode: `event_hub` or `one_time` |
| `AZURE_CLIENT_ID` | Yes | - | Managed identity client ID |
| `AZURE_TENANT_ID` | No | Auto-detected | Azure tenant ID |
| `RELEASE_IDS_FILE` | No | `./release_ids.txt` | File with release IDs for one-time mode |
| `PYTHONUNBUFFERED` | No | `1` | Enable real-time log output |
| `PYSPARK_PYTHON` | No | `/usr/local/bin/python3.10` | Python path for Spark workers |
| `PYSPARK_DRIVER_PYTHON` | No | `/usr/local/bin/python3.10` | Python path for Spark driver |

### Azure Key Vault Secrets

The service expects these secrets in Key Vault (`gzc-finma-keyvault`):

| Secret Name | Purpose |
|------------|---------|
| `fred-api-key-1` through `fred-api-key-9` | FRED API keys for rotation |
| `storage-account-key` | Azure Storage account access key |
| `event-hub-connection-string` | Event Hub connection string |

### Config File Structure

**delta_lake/config.yaml:**

```yaml
storage:
  account_name: "gzcstorageaccount"
  container: "macroeconomic-maintained-series"

authentication:
  use_managed_identity: true
  client_id: "ae954be9-9a46-4085-849a-191d8884119c"
  tenant_id: "8274c97d-de9d-4328-98cf-2d4ee94bf104"

fred_api:
  key_vault:
    name: "gzc-finma-keyvault"
    secret_prefix: "fred-api-key-"
    total_keys: 9

event_hub:
  enabled: true
  topics:
    data_updated: "fred-data-updated"
```

---

## Verification

### Post-Deployment Checks

**1. Pod Status**

```bash
kubectl get pods -n fred-pipeline -l app=fred-collector
# Expected: STATUS = Running, READY = 1/1
```

**2. Logs Check**

```bash
kubectl logs deployment/fred-collector -n fred-pipeline --tail=50
# Expected: "🚀 All components initialized successfully"
```

**3. Spark Session**

```bash
kubectl logs deployment/fred-collector -n fred-pipeline | grep "Spark session created"
# Expected: "✅ Spark session created"
```

**4. Delta Lake Tables**

```bash
# Check if tables were created
kubectl exec -it <pod-name> -n fred-pipeline -- python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
tables = spark.sql('SHOW TABLES').collect()
print(tables)
"
```

**5. Test Collection**

```bash
# Test with a single release
kubectl exec -it <pod-name> -n fred-pipeline -- python -c "
import asyncio
from collector import FREDCollectorV2
# ... test code
"
```

### Health Checks

**Liveness Probe:**
```bash
# Container health check (30s interval)
python -c "import sys; sys.exit(0)"
```

**Readiness Probe:**
```bash
# Application readiness
curl http://localhost:8080/health  # If health endpoint exists
```

---

## Troubleshooting

### Issue 1: PySpark Worker Crashes

**Symptoms:**
- Error: `AttributeError: module 'typing' has no attribute 'io'`
- Workers fail to start
- Broadcast variable deserialization fails

**Diagnosis:**
```bash
# Check if Fix 1 was applied
kubectl exec -it <pod-name> -n fred-pipeline -- \
  unzip -p /usr/local/lib/python3.10/site-packages/pyspark/python/lib/pyspark.zip pyspark/broadcast.py | grep "typing import"
```

**Solution:**
Rebuild container with Fix 1 (Dockerfile lines 37-51)

---

### Issue 2: Python Version Mismatch

**Symptoms:**
- Workers using Python 2.7
- Import errors for Python 3 syntax
- Serialization errors

**Diagnosis:**
```bash
# Check environment variables
kubectl exec -it <pod-name> -n fred-pipeline -- env | grep PYSPARK_PYTHON
```

**Solution:**
Ensure Fix 2 is applied (PYSPARK_PYTHON env vars)

---

### Issue 3: Table Not Found

**Symptoms:**
- `AnalysisException: Table or view not found`
- First collection run fails

**Diagnosis:**
```bash
# Check if _ensure_table_exists is being called
kubectl logs deployment/fred-collector -n fred-pipeline | grep "Creating Delta table"
```

**Solution:**
Verify Fix 3 is implemented (auto-initialization in merge_logic.py)

---

### Issue 4: Databricks Property Warnings

**Symptoms:**
- Warnings about unknown properties
- Properties ignored silently

**Diagnosis:**
```bash
# Check logs for property warnings
kubectl logs deployment/fred-collector -n fred-pipeline | grep "autoOptimize"
```

**Solution:**
Apply Fix 4 (remove Databricks properties from schemas.py)

---

### Issue 5: API Rate Limiting

**Symptoms:**
- 429 HTTP errors
- Slow collection

**Diagnosis:**
```bash
kubectl logs deployment/fred-collector -n fred-pipeline | grep "429"
```

**Solution:**
```bash
# Reduce parallelism in config.yaml
performance:
  parallel_collections: 1  # Down from default 3
```

---

### Issue 6: Memory Issues

**Symptoms:**
- OOMKilled pods
- Slow performance

**Diagnosis:**
```bash
kubectl top pods -n fred-pipeline
```

**Solution:**
```bash
# Increase memory limits in deployment manifest
resources:
  limits:
    memory: "24Gi"  # Up from 16Gi
```

---

## Rollback Procedures

### Quick Rollback

```bash
# Rollback to previous deployment
kubectl rollout undo deployment/fred-collector -n fred-pipeline

# Check rollout status
kubectl rollout status deployment/fred-collector -n fred-pipeline
```

### Rollback to Specific Build

```bash
# Update image tag
kubectl set image deployment/fred-collector \
  fred-collector=gzcacr.azurecr.io/fred-collector:ca42 \
  -n fred-pipeline

# Or edit deployment directly
kubectl edit deployment fred-collector -n fred-pipeline
```

### Emergency Stop

```bash
# Scale down to 0 replicas
kubectl scale deployment fred-collector --replicas=0 -n fred-pipeline

# Scale back up
kubectl scale deployment fred-collector --replicas=2 -n fred-pipeline
```

---

## Build History

| Build | Date | Status | Fixes Included | Notes |
|-------|------|--------|----------------|-------|
| ca41 | 2025-11-19 | Superseded | Fix 1, Fix 2 | Initial fixes for PySpark and Python paths |
| ca42 | 2025-11-19 | Superseded | Fix 1, Fix 2, Fix 3 | Added auto-initialization |
| ca43 | 2025-11-20 | Current | Fix 1, Fix 2, Fix 3, Fix 4 | Removed Databricks properties |

---

## Next Steps

After successful deployment:

1. **Monitor for 24 hours** - Check logs, memory usage, CPU usage
2. **Run test collection** - Verify with release 441 (4 series, 13,715 observations)
3. **Deploy release-monitor** (Phase 3) - Auto-triggers collection on FRED updates
4. **Set up Application Insights** - Dashboards and alerts
5. **Tune auto-scaling** - Adjust HPA based on actual load

---

## Support

For issues or questions:
- Check logs: `kubectl logs -f deployment/fred-collector -n fred-pipeline`
- Review implementation plan: `/docs/IMPLEMENTATION_PLAN.md`
- Consult Delta Lake guide: `/infrastructure/delta-lake/README.md`
- Contact: Data Engineering Team

---

**Generated by:** README Agent
**Project:** FRED Collector Service
**Generated:** 2025-11-20
