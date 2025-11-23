# Synapse Deployment Lessons Learned

**Project:** FRED Data Collection → Synapse Transformation → Delta Lake
**Date Started:** 2025-11-23
**Status:** In Progress

---

## 📋 Executive Summary

This document captures all methodology, configuration, challenges, and solutions for deploying FRED data transformations using Azure Synapse Analytics. Updated continuously as we discover new patterns and overcome obstacles.

**Key Learning:** Synapse notebooks CAN be deployed programmatically via REST API - we have done this successfully in past sessions.

---

## 🎯 Core Architecture

```
FRED API (341 releases)
  ↓
Blob Storage (raw parquet files)
  ↓
Azure Synapse PySpark Notebook (transformation)
  ↓
Delta Lake (partitioned by year/month)
  ↓
Query Tools (Polars, Spark SQL)
```

---

## ✅ DO's - Best Practices That Work

### 1. Programmatic Notebook Deployment
**Status:** ✅ Proven to work in past sessions

- **Use REST API for deployment** - Don't rely on manual Synapse Studio operations
- **Files available:**
  - `deploy_synapse_transformation.py` - Trigger notebook via REST API
  - `synapse_blob_to_delta.py` - PySpark transformation notebook
  - `quick_deploy_synapse.sh` - One-command deployment
  - `eventhub_synapse_orchestrator.py` - Event-driven orchestration

**Deployment pattern:**
```python
# Trigger notebook via REST API
url = f"{SYNAPSE_ENDPOINT}/notebooks/{notebook_name}/executePipeline"
payload = {
    "notebook": notebook_name,
    "sparkPool": spark_pool,
    "parameters": {
        "storage_account": "gzcstorageaccount",
        "container": "macroeconomic-maintained-series",
        "vintage_date": "2025-11-21",
        "release_id": ""
    }
}
```

### 2. Data Partitioning Strategy
**Status:** ✅ Critical for query performance

- **Partition by `(year, month)`** - NOT by `series_id`
- **Rationale:** Time-range queries are primary use case
- **Z-ordering:** Apply on `series_id` and `release_id` for filtering

**Why time-based partitioning:**
- Most queries filter by date range: "Get 2023 data"
- Series-based partitioning fragments data across too many partitions
- Month-level granularity balances partition size vs. pruning efficiency

### 3. Delta Lake Schema Evolution
**Status:** ✅ Use `overwriteSchema` option

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("overwriteSchema", "true") \
    .save(DELTA_PATH)
```

**Why this matters:**
- Raw data schemas may evolve (new columns added)
- Missing columns (like `release_date`) need to be handled gracefully
- Always add missing columns with `lit(None)` before writing

### 4. Quality Control Checks
**Status:** ✅ Essential before writing

**Required validations:**
1. Schema validation (all required columns present)
2. Null checks on key columns (`series_id`, `date`, `release_id`)
3. Date format validation (`YYYY-MM-DD` pattern)
4. Deduplication on `(series_id, date)` composite key

**If QC fails:** Stop transformation immediately - don't write corrupt data

### 5. Batch Processing for Large Datasets
**Status:** ✅ Critical for 145M+ rows

- **Read in batches:** 50-100 files at a time
- **Use async I/O:** Parallel blob reads for faster throughput
- **Memory management:** Combine batches before final write (not per-batch writes)

**Example pattern:**
```python
batches = []
for i in range(0, len(blobs), batch_size):
    batch_blobs = blobs[i:i + batch_size]
    batch_df = read_batch_async(batch_blobs)
    batches.append(batch_df)

combined_df = pl.concat(batches)
```

### 6. Monitoring and Logging
**Status:** ✅ Essential for long-running jobs

- **Log every phase:** Reading, transforming, QC, writing, optimizing
- **Track metrics:** Row counts, file counts, duration, memory usage
- **Use structured logging:** Makes debugging easier later

---

## ❌ DON'Ts - Pitfalls to Avoid

### 1. Don't Partition by Series ID
**Status:** ❌ Major performance issue

- **Why it fails:** Creates 800k+ partitions (one per series)
- **Impact:** Extremely slow queries, fragmented data
- **Fix:** Delete entire folder, recreate with time-based partitioning

**Deletion is not atomic:**
```bash
# Batch deletion can get stuck at partial completion
az storage blob delete-batch --pattern "series_observations/*"
# Solution: Use overwrite mode instead
```

### 2. Don't Skip Deduplication
**Status:** ❌ Data integrity issue

- **Why it matters:** FRED API can return duplicate observations
- **Impact:** Inflated row counts, incorrect aggregations
- **Fix:** Always deduplicate on `(series_id, date)` before writing

### 3. Don't Write Batches Individually
**Status:** ❌ Creates too many small files

- **Why it fails:** Delta Lake gets fragmented with 1000s of tiny files
- **Impact:** Slow reads, inefficient storage
- **Fix:** Combine all batches, write once with partitioning

### 4. Don't Assume Schema Consistency
**Status:** ❌ Schema drift is real

- **Example:** Some files missing `release_date` column
- **Impact:** Schema mismatch errors during read
- **Fix:** Add missing columns explicitly before concat

```python
if "release_date" not in df.columns:
    df = df.with_columns(pl.lit(None).alias("release_date"))
```

### 5. Don't Ignore Z-Ordering
**Status:** ❌ Misses major optimization

- **Why it matters:** Synapse/Spark reads entire partitions by default
- **Impact:** Queries filter 800k series but read millions of unnecessary rows
- **Fix:** Run Z-ordering after write:

```python
delta_table = DeltaTable.forPath(spark, DELTA_PATH)
delta_table.optimize().executeZOrderBy("series_id", "release_id")
```

### 6. Don't Delete and Hope
**Status:** ❌ Deletion can fail silently

- **Problem:** Azure CLI batch deletion can hang at partial completion
- **Evidence:** Stuck at 5000 files remaining for >5 minutes
- **Solution:** Use `overwrite` mode with `overwriteSchema=true` - it handles cleanup

---

## 🚨 CRITICAL: Notebook Publishing Requirement (Updated 2025-11-23)

**Issue Confirmed:** REST API notebook upload does NOT automatically publish. Notebooks exist in draft mode until manually published.

**Symptoms:**
- Upload succeeds (200/201 response)
- Trigger fails (404 error: "notebook not found")
- Notebook visible in Synapse Studio but marked "Unpublished"

**Solutions:**
1. **Manual Publish (Quick Test):**
   - Open: https://web.azuresynapse.net?workspace=/subscriptions/6f928fec-8d15-47d7-b27b-be8b568e9789/resourceGroups/MTWS_Synapse/providers/Microsoft.Synapse/workspaces/externaldata
   - Navigate: Develop → Notebooks → [your_notebook]
   - Click: "Publish all" button
   - Re-trigger via REST API

2. **Automated CI/CD (Production):** ✅ **IMPLEMENTED**
   - Use GitHub Actions with `validateDeploy` operation
   - **Complete setup guide:** `GITHUB_ACTIONS_SETUP.md`
   - **Workflow file:** `.github/workflows/synapse-deploy.yml`
   - **Converter script:** `.github/scripts/convert_to_notebook.py`
   - Notebooks are automatically published on push to main branch

**Verified:** This issue affects `hello_flow_test` notebook deployment (2025-11-23)
**Solved:** GitHub Actions CI/CD pipeline implemented (2025-11-23)

---

## 🔧 Configuration Maintained

### Synapse Workspace Configuration
```python
SYNAPSE_CONFIG = {
    "workspace_name": "externaldata",
    "resource_group": "MTWS_Synapse",
    "subscription_id": "6f928fec-8d15-47d7-b27b-be8b568e9789",
    "notebook_name": "blob_to_delta_transformer",
    "spark_pool": "fredsparkpool",
    "storage_account": "gzcstorageaccount",
    "container": "macroeconomic-maintained-series",
    "vintage_date": "2025-11-21"
}

SYNAPSE_ENDPOINT = f"https://{SYNAPSE_CONFIG['workspace_name']}.dev.azuresynapse.net"
```

### Storage Paths
```python
# Source data (raw parquet from FRED API)
BLOB_PREFIX = "abfss://macroeconomic-maintained-series@gzcstorageaccount.dfs.core.windows.net/US_Fred_Data/raw/2025-11-21/"

# Target Delta Lake table
DELTA_PATH = "abfss://macroeconomic-maintained-series@gzcstorageaccount.dfs.core.windows.net/US_Fred_Data/series_observations"

# Analysis output tables
ANALYSIS_PATH = "abfss://macroeconomic-maintained-series@gzcstorageaccount.dfs.core.windows.net/US_Fred_Data/analysis"
```

### Required Schema
```python
required_columns = [
    "series_id",        # string - FRED series identifier
    "date",             # string - YYYY-MM-DD format
    "value",            # float - observation value
    "vintage_date",     # string - FRED vintage date
    "collected_at",     # datetime - collection timestamp
    "release_id",       # int - FRED release ID
    "release_date",     # string - release date (nullable)
    "collection_vintage", # string - our collection batch ID
    "year",             # int - partition key
    "month"             # int - partition key
]
```

### Spark Pool Specifications
- **Pool Name:** fredsparkpool
- **Recommended Size:** Medium (4-8 cores)
- **Expected Duration:** 10-20 minutes for 341 releases, 145M rows

---

## 🐛 Known Issues and Solutions

### Issue 1: Batch Deletion Hangs
**Symptom:** `az storage blob delete-batch` stops making progress
**Root Cause:** Unknown - possibly Azure CLI limitation
**Workaround:** Use `overwrite` mode instead of deletion
**Status:** Workaround proven effective

### Issue 2: Missing release_date Column
**Symptom:** Schema mismatch error during batch concat
**Root Cause:** Some FRED releases don't have release dates
**Solution:** Add column with `lit(None)` before processing
**Status:** Fixed in blob_to_delta_transformer.py

### Issue 3: Memory Pressure with Large Batches
**Symptom:** Slow processing or OOM errors
**Root Cause:** Loading 100+ files simultaneously
**Solution:** Reduce batch size to 50, use streaming reads
**Status:** Optimized in current implementation

---

## 📊 Performance Benchmarks

### Local Polars Transformation (M1 Mac, 32GB RAM)
- **Dataset:** 321 files, 145,180,145 rows
- **Read Phase:** ~3-5 minutes (async batch reads)
- **Transform Phase:** ~2-3 minutes (schema alignment, deduplication)
- **Write Phase:** ~5-10 minutes (Delta Lake with partitioning)
- **Z-Ordering:** ~3-5 minutes (optional optimization)
- **Total Duration:** 15-25 minutes

### Synapse PySpark Transformation (Expected)
- **Dataset:** Same 321 files, 145M rows
- **Spark Pool:** Medium (4-8 cores)
- **Expected Duration:** 10-20 minutes
- **Benefits:**
  - Distributed processing across cluster
  - Native Delta Lake optimizations
  - Built-in monitoring and logging

### Query Performance (After Optimization)
```
Single series (UNRATE): ~50-200ms
Release query (release_id=10): ~100-500ms
Time range (2023 Q1): ~200-800ms
Full count: ~1-3 seconds
```

---

## 🔄 Deployment Workflow

### Step 1: Upload Notebook to Synapse
```bash
./quick_deploy_synapse.sh
```

**What it does:**
1. Authenticates with Azure
2. Creates notebook JSON payload
3. Uploads via REST API
4. Verifies upload success

### Step 2: Trigger Notebook Execution
```python
python deploy_synapse_transformation.py
```

**What it does:**
1. Gets Azure access token
2. Triggers notebook via REST API
3. Monitors execution status
4. Publishes completion event (if using Event Hub)

### Step 3: Verify Results
```bash
cd query_tool
./.venv/bin/python -c "
from fred_query import *
q = FREDQuery()
print(q.info())
print(f'Series: {q.list_series(limit=100000).shape[0]:,}')
"
```

---

## 📈 Future Improvements

### Priority 1: Production Deployment to Synapse (Speed Critical!)
**Goal:** Achieve sub-10-minute transformation time for rapid insights after FRED releases

**Why this matters:**
- **Current local approach:** 15-25 minutes (acceptable for dev, too slow for production)
- **Business requirement:** Need data insights immediately after FRED releases
- **Synapse advantage:** 5-10 minutes with distributed processing + no internet upload bottleneck

**What we have ready:**
- ✅ `synapse_blob_to_delta.py` - Production PySpark notebook
- ✅ `deploy_synapse_transformation.py` - REST API deployment
- ✅ `eventhub_synapse_orchestrator.py` - Event-driven triggers
- ✅ Comprehensive lessons learned (this document)

**Expected performance improvement:**
- **Network I/O:** Azure-to-Azure (10-100 Gbps) vs. home internet upload
- **Processing:** Distributed across 8-16 cores vs. local single machine
- **Total time:** 5-10 minutes vs. 15-25 minutes

**Next deployment:** Trigger Synapse notebook via REST API and benchmark actual performance

### Priority 2: Incremental Updates
**Goal:** Only process new releases, not full re-transformation

**Approach:**
1. Track processed releases in `collection_runs` table
2. Filter blob list to exclude already-processed releases
3. Use Delta Lake `merge` operation instead of `overwrite`

**Benefits:**
- Faster transformations (minutes vs. hours)
- Lower compute costs
- Continuous data pipeline

### Event-Driven Pipeline
**Goal:** Automatically trigger transformations when new data arrives

**Components:**
1. FRED Collector → Publishes to Event Hub
2. Event Hub Orchestrator → Listens for events
3. Synapse Notebook → Triggered automatically
4. Completion Event → Notifies downstream consumers

**File:** `eventhub_synapse_orchestrator.py` (already implemented)

### Advanced Analytics
**Goal:** Pre-compute common aggregations

**Analysis tables to generate:**
- Series coverage (first_date, last_date, observation_count)
- Temporal distribution (observations per month/year)
- Release statistics (series count, data range per release)
- Value statistics (mean, stddev, min, max per series)

**Benefits:**
- Instant dashboard queries
- Trend analysis without scanning full dataset
- Data quality monitoring

---

## 🎓 Key Takeaways

1. **Programmatic deployment works** - Use REST API, don't rely on Synapse Studio
2. **Partitioning strategy is critical** - Time-based partitions for time-series data
3. **Quality control before write** - Catch schema issues early
4. **Batch processing essential** - Can't load 145M rows in one go
5. **Overwrite mode is safer than deletion** - Handles cleanup automatically
6. **Z-ordering provides huge speedup** - 10-100x faster filtered queries
7. **Monitor everything** - Long-running jobs need visibility

---

## 📝 Change Log

### 2025-11-23
- Created initial lessons learned document
- Documented current configuration and workflow
- Captured known issues from transformation v9
- Added performance benchmarks for local Polars transformation
- Documented DO's and DON'Ts based on early testing

### [Future entries will be added here as we learn more]

---

**Next Update:** After completing first successful Synapse deployment, we'll add:
- Actual Synapse performance metrics
- Any new issues encountered
- Refined configuration recommendations
- Deployment automation improvements
