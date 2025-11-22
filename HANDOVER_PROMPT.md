# FRED Collector Service - Session Handover Prompt

**Project:** FRED Collector Service
**Location:** `/Users/mikaeleage/Research & Analytics Services/Projects/azure-data-pipelines/services/fred-collector`
**Status:** 🚨 BLOCKED - Akamai WAF blocking AKS cluster IPs
**Last Updated:** 2025-11-22

---

## 🎯 Quick Context

### What This Is
Kubernetes-deployed service for collecting Federal Reserve Economic Data (FRED) using V2 bulk API. Stores data in Azure Delta Lake with vintage tracking and incremental updates.

### Architecture Pattern
- **Data Source:** FRED API (api.stlouisfed.org)
- **Collection Method:** Event-driven via Azure Event Hub (NOT bulk collection of all 341 releases)
- **Storage:** Azure Blob Storage (gzcstorageaccount) → Delta Lake format
- **Orchestration:** Kubernetes (gzc-k8s-engine cluster, fred-pipeline namespace)
- **Authentication:** Azure Managed Identity + Key Vault (9 API keys with round-robin rotation)

### Current Production Status
- ✅ **Code:** Ready (build ca43 with all critical fixes)
- ✅ **Infrastructure:** Deployed to AKS
- ✅ **Testing:** Validated with release 441 (13,715 observations in ~3 seconds)
- 🚨 **BLOCKED:** Akamai WAF blocking API calls from AKS cluster IP

---

## 🚨 CRITICAL: Akamai Blocking Issue

**Problem:** FRED API returns HTTP 403 when called from Azure Kubernetes Service pods.

**Root Cause:** Akamai CDN/WAF treats requests from Azure datacenter IPs as suspicious.

**Blocked IP:** 172.171.167.93 (AKS cluster outbound IP)

**Immediate Action Required:**
1. Contact FRED Support: https://fred.stlouisfed.org/contactus/
2. Request IP whitelisting for institutional research use
3. Provide: IP 172.171.167.93, API key, use case description

**Alternative Solutions:**
- Use existing NAT Gateway in fxspotstream-rg (static IP for whitelisting)
- Run bulk collection from Azure VM instead of K8s
- See `AKAMAI_BLOCKING_ISSUE.md` for complete analysis

**DO NOT** attempt to run the bulk collector until IP whitelisting is resolved!

---

## 📊 Project Resources

### Azure Resources
- **Storage Account:** gzcstorageaccount
- **Container:** macroeconomic-maintained-series
- **Key Vault:** gzc-finma-keyvault (secrets: fred-api-key-1 through fred-api-key-9)
- **Managed Identity:** ae954be9-9a46-4085-849a-191d8884119c
- **K8s Cluster:** gzc-k8s-engine
- **Namespace:** fred-pipeline
- **Container Registry:** gzcacr.azurecr.io

### Docker Images
- **Current Build:** gzcacr.azurecr.io/fred-collector:ca43
- **Bulk Collector:** gzcacr.azurecr.io/fred-bulk-collector:v10-k8s-fix

### K8s Deployments
- **Event-driven collector:** `k8s/deployment.yaml`
- **One-time bulk job:** `k8s/bulk-collector-job.yaml`
- **Scheduled cronjob:** `k8s/bulk-collector-cronjob.yaml`

---

## 🔑 Key Commands

### Check Current Deployment Status
```bash
kubectl get pods -n fred-pipeline -l app=fred-collector
kubectl logs -f deployment/fred-collector -n fred-pipeline
```

### View Collected Data (Local Query)
```bash
cd query_tool
poetry install
poetry run python -c "
from fred_query import FREDQuery
q = FREDQuery()
print(q.info())
series = q.list_series(limit=10)
print(series)
"
```

### Test FRED API Access (Verify Akamai Block)
```bash
# From local machine (should work)
curl "https://api.stlouisfed.org/fred/releases?api_key=YOUR_API_KEY&file_type=json"

# From K8s pod (currently blocked)
kubectl run -it --rm debug --image=curlimages/curl --restart=Never -n fred-pipeline -- \
  curl "https://api.stlouisfed.org/fred/releases?api_key=YOUR_API_KEY&file_type=json"
```

### Build and Push New Image
```bash
# Build collector service
docker build -f Dockerfile -t gzcacr.azurecr.io/fred-collector:ca44 .
docker push gzcacr.azurecr.io/fred-collector:ca44

# Build bulk collector
docker build -f Dockerfile.bulk -t gzcacr.azurecr.io/fred-bulk-collector:v11 .
docker push gzcacr.azurecr.io/fred-bulk-collector:v11
```

### Deploy to Kubernetes
```bash
# Update deployment
kubectl apply -f k8s/deployment.yaml

# Run one-time bulk job (ONLY after Akamai issue resolved!)
kubectl apply -f k8s/bulk-collector-job.yaml

# Check job status
kubectl get jobs -n gzc-analytics
kubectl logs job/fred-bulk-collector -n gzc-analytics
```

---

## 🏗️ Architecture Details

### Collection Modes
1. **Event Hub Mode (Production):** Listens for FRED release updates, collects only new/changed releases
2. **One-Time Mode (Initial Load):** Collects all releases once (use with caution!)

### Data Tables (Delta Lake)
1. **series_observations** - Time series data points (partitioned by series_id)
2. **series_metadata** - Series information
3. **release_metadata** - Release information
4. **collection_history** - Audit log of collection runs

### API Key Rotation
9 FRED API keys in Azure Key Vault, rotated round-robin:
- Theoretical capacity: 1,080 req/min (9 keys × 120 req/min)
- Safe sustained rate: ~500 req/min with retry handling

### Performance Metrics
- **V2 API Speed:** ~3 seconds for 13,715 observations
- **V1 API Comparison:** 100-200x slower (5-10 minutes)
- **Resource Usage:** 2 CPU cores (bursts to 4), 8GB memory per pod
- **Auto-scaling:** 2-10 pods based on load

---

## 🐛 Critical Fixes Implemented

### Build ca43: Databricks Properties Removal
**Issue:** autoOptimize properties are Databricks-only, not open-source Delta Lake
**Fix:** Removed from `delta_lake/schemas.py` lines 159-163
**Status:** ✅ Deployed

### Build ca42: Delta Table Auto-Initialization
**Issue:** Tables must exist before merge operations
**Fix:** Added `_ensure_table_exists()` method in `delta_lake/merge_logic.py` lines 75-107
**Status:** ✅ Deployed

### Build ca41: PySpark Worker Crashes
**Issue 1:** PySpark 3.4.1 typing.io import bug (SPARK-44169)
**Fix:** Dockerfile patch lines 37-51
**Issue 2:** Workers couldn't find Python 3.10 executable
**Fix:** Explicit PYSPARK_PYTHON env vars (Dockerfile lines 62-64)
**Status:** ✅ Deployed

### V2 API Authentication Fix
**Issue:** 401 Unauthorized on FRED V2 API
**Fix:** Changed from query param to Bearer token header (`collector.py` lines 107-114)
**Status:** ✅ Deployed

---

## 📝 Documentation Files

### Project Documentation
- **PROJECT_MANIFEST.json** - Complete project specification (standardized 2025-11-22)
- **README.md** - Overview and deployment guide
- **DEV_JOURNAL.md** - Chronological development history
- **DEPLOYMENT.md** - Detailed K8s deployment instructions
- **QC_FEATURES.md** - Quality control features documentation
- **AKAMAI_BLOCKING_ISSUE.md** - Complete analysis of current blocking issue

### Code Documentation
- **collector.py** - V2 API collection logic with 9-key rotation
- **main.py** - Service entry point with Event Hub integration
- **delta_lake/merge_logic.py** - Delta Lake merge/upsert operations
- **delta_lake/schemas.py** - Table schemas and partitioning definitions
- **bulk_collector.py** - Standalone bulk collection script (v9-qc with QC features)

---

## ⚠️ Important Constraints

### DO NOT:
1. **Deploy bulk collection jobs** until Akamai blocking is resolved
2. **Collect all 341 releases** daily (only new releases via Event Hub)
3. **Manually edit PROJECT_MANIFEST.json** (use README Agent for updates)
4. **Use V1 API** (100-200x slower than V2)
5. **Hard-code credentials** (always use Azure Key Vault)

### DO:
1. **Check Akamai blocking status** before running any collection
2. **Use Event Hub pattern** for incremental collection
3. **Monitor collection_history table** for success/failure rates
4. **Verify Delta Lake data** after collection with query_tool
5. **Update DEV_JOURNAL.md** for significant changes

---

## 🎯 Next Steps (After Akamai Resolution)

### Immediate (Week 1)
1. ✅ Resolve Akamai blocking (contact FRED or implement NAT Gateway)
2. ⏸️ Test bulk collection from K8s (currently blocked)
3. ⏸️ Deploy Event Hub listener (Phase 3)
4. ⏸️ Set up Application Insights dashboards

### Short-term (Month 1)
1. Configure alert rules for collection failures
2. Optimize Spark executor configuration based on metrics
3. Adjust auto-scaling rules
4. Implement comprehensive unit tests

### Long-term (Quarter 1)
1. Deploy release-monitor service (Phase 3)
2. Implement data quality dashboards
3. Cost optimization analysis
4. Multi-region deployment consideration

---

## 🔗 Related Projects

### Parent Project
**azure-data-pipelines** - `/Users/mikaeleage/Research & Analytics Services/Projects/azure-data-pipelines`

### Related Services
- **release-monitor** (Phase 3, planned) - Monitors FRED releases API for new data
- **fred-api-gateway** (future) - Unified API for querying collected FRED data

---

## 📞 Support and Resources

### Contact
- **Team:** Data Engineering
- **Owner:** Mikael Eage
- **Support:** Check logs with kubectl, review docs/IMPLEMENTATION_PLAN.md

### External Resources
- **FRED API Docs:** https://fred.stlouisfed.org/docs/api/fred/
- **FRED Support:** https://fred.stlouisfed.org/contactus/
- **Delta Lake Docs:** https://delta.io/

### Internal Resources
- **System Management Workspace:** `/Users/mikaeleage/System Management Workspace`
- **CLAUDE.md:** `/Users/mikaeleage/CLAUDE.md` (global instructions)
- **README Agent:** `/Users/mikaeleage/Research & Analytics Services/agents_fleet/claude_agent_sdk/readme_agent/`

---

## 🔄 Session Continuation Checklist

When resuming work on this project, verify:

- [ ] Read this HANDOVER_PROMPT.md file
- [ ] Check DEV_JOURNAL.md for latest changes
- [ ] Review AKAMAI_BLOCKING_ISSUE.md status
- [ ] Verify K8s deployment status (`kubectl get pods -n fred-pipeline`)
- [ ] Check Delta Lake data freshness (query_tool)
- [ ] Confirm API keys are accessible in Key Vault
- [ ] Review PROJECT_MANIFEST.json for current spec

**If blocked by Akamai:**
- [ ] Check if FRED has responded to whitelist request
- [ ] Consider NAT Gateway alternative
- [ ] DO NOT run bulk collection from K8s

**If Akamai resolved:**
- [ ] Test single release collection first
- [ ] Monitor resource usage
- [ ] Verify Delta Lake writes
- [ ] Enable Event Hub listener

---

**Generated by:** README Agent
**Project:** FRED Collector Service
**Generated:** 2025-11-22
