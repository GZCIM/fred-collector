# FRED API Akamai Blocking Issue

**Date:** 2025-11-22
**Status:** 🚨 BLOCKED - Akamai WAF blocking AKS cluster IPs
**Impact:** Cannot run bulk collection from Kubernetes

---

## 🔍 Root Cause Analysis

### Problem
FRED API (api.stlouisfed.org) returns **HTTP 403 Forbidden** when called from Azure Kubernetes Service (AKS) pods.

### Evidence
```html
<H1>Access Denied</H1>
You don't have permission to access "http://api.stlouisfed.org/fred/releases?" on this server.
Reference #18.f643017.1763771383.160acb
```

**Server Header:** `AkamaiGHost` - FRED uses Akamai CDN/WAF for protection

**Blocked IP:** `172.171.167.93` (AKS cluster outbound IP)

### Why This Happens
- Akamai's security rules treat requests from Azure datacenter IP ranges as suspicious traffic
- Cloud providers' IP ranges are commonly blocked by WAF/CDN providers to prevent abuse
- Same API key works fine from consumer ISPs but gets blocked from cloud infrastructure

### Verification
✅ **API Key is Valid:** `92d2068e05feb04c9d4b4fbc53d2acd0` works from local machine
✅ **K8s Secret is Correct:** All 4 API keys match expected values
❌ **Cloud IP Blocked:** Same request from AKS pod returns 403

---

## ✅ Recommended Solutions (Ranked)

### Solution 1: Contact FRED for IP Whitelisting (BEST)

**Contact:** FRED Support at https://fred.stlouisfed.org/contactus/
**Request:** Whitelist AKS cluster outbound IP for API access

**Email Template:**
```
Subject: Request to Whitelist Azure IP for FRED API Access

Hello FRED Team,

We are an institutional research organization using FRED API for bulk economic data collection.
Our automated collection system runs on Azure Kubernetes Service (AKS) and is being blocked
by your Akamai WAF.

Could you please whitelist the following IP address for API access?

IP Address: 172.171.167.93
API Key: 92d2068e05feb04c9d4b4fbc53d2acd0
Use Case: Automated bulk collection of economic releases (341 releases, ~40M observations)
Frequency: Weekly full collection runs

The same API key works fine from consumer networks but gets HTTP 403 from Azure infrastructure.

Akamai Reference ID from blocked request: 18.f643017.1763771383.160acb

Thank you!
```

**Pros:**
- ✅ Most sustainable long-term solution
- ✅ No additional Azure infrastructure costs
- ✅ Official support from FRED team
- ✅ Maintains current K8s architecture

**Cons:**
- ⏱️ May take 1-2 weeks for approval

---

### Solution 2: Use Azure NAT Gateway (IMMEDIATE WORKAROUND)

We already have a NAT Gateway in `fxspotstream-rg` with static public IPs. Route bulk collector through it.

**Implementation:**
1. Get NAT Gateway public IPs:
   ```bash
   az network public-ip show -g fxspotstream-rg -n fxspotstream-nat-pip --query ipAddress -o tsv
   ```

2. Associate NAT Gateway with AKS subnet (if not already):
   ```bash
   az network vnet subnet update \
     --resource-group gzc-kubernetes-rg \
     --vnet-name gzc-k8s-engine-vnet \
     --name default \
     --nat-gateway fxspotstream-nat-gateway
   ```

3. Request FRED whitelist NAT Gateway IP instead of load balancer IP

**Pros:**
- ✅ Static IP that won't change with cluster updates
- ✅ Already exists (no new resources needed)
- ✅ Same pattern we use for FX spot stream whitelisting

**Cons:**
- 💰 Small cost for NAT Gateway data processing (~$0.045/GB + $0.045/hour)
- 🔧 Requires subnet configuration change

**Cost Estimate:** ~$50-100/month for bulk collection traffic

---

### Solution 3: Run Bulk Collection from Azure VM

Deploy bulk collector to a dedicated Azure VM instead of AKS.

**Implementation:**
```bash
# Create VM
az vm create \
  --resource-group fred-pipeline-rg \
  --name fred-bulk-collector-vm \
  --image Ubuntu2204 \
  --size Standard_E16s_v3 \
  --assign-identity

# Install Python, Docker, dependencies
# Run bulk_collector.py directly or via Docker
```

**Pros:**
- ✅ Different IP range (may not be blocked)
- ✅ Full control over environment
- ✅ Can request VM's IP be whitelisted

**Cons:**
- 💰 Higher cost than K8s (~$200-300/month for Standard_E16s_v3)
- 🔧 More infrastructure to manage
- ⏱️ Still need FRED to whitelist VM's IP

---

### Solution 4: Add User-Agent and Retry Logic (UNLIKELY TO WORK)

Try adding legitimate User-Agent header to identify as research client.

**Code Change:**
```python
async with httpx.AsyncClient(timeout=60.0) as client:
    response = await client.get(
        "https://api.stlouisfed.org/fred/releases",
        params={"api_key": api_key, "file_type": "json"},
        headers={"User-Agent": "GZC-Research-Analytics/1.0 (contact@example.com)"}
    )
```

**Pros:**
- ✅ Quick to test
- ✅ No infrastructure changes

**Cons:**
- ❌ Unlikely to work (Akamai blocks based on IP, not User-Agent)
- ❌ Only useful if combined with IP whitelisting

---

### Solution 5: Route Through Azure Function/API Management (COMPLEX)

Create Azure Function or API Management as proxy with different IP.

**Pros:**
- ✅ Different outbound IP
- ✅ Can add rate limiting, caching

**Cons:**
- 💰 Additional costs (~$50-200/month)
- 🔧 Complex architecture
- ❌ Azure Functions IPs may also be blocked
- ⏱️ Still likely need whitelisting

---

## 📊 Current Status

### Working Environment
- ✅ Local machine: `curl` to FRED API works perfectly
- ✅ K8s secrets: All API keys correctly configured
- ✅ K8s infrastructure: High-memory node pool ready (128GB RAM)
- ✅ Docker image: v9-qc with comprehensive QC features built and pushed

### Blocked Environment
- ❌ K8s pods: HTTP 403 from Akamai WAF
- ❌ Bulk collection: Cannot run from Kubernetes

### QC Features Ready (v9-qc)
All quality control features implemented and tested locally:
- Data validation (nulls, duplicates, outliers, schema)
- Completeness verification
- Automated retry logic (3 attempts, exponential backoff)
- Quality score calculation (0-100)
- QC reconciliation report with recommendations

**Full QC documentation:** `QC_FEATURES.md`

---

## 🎯 Recommended Action Plan

### Immediate (Today)
1. ✅ **Document issue** (this file)
2. 🔜 **Contact FRED support** with whitelist request
3. 🔜 **Check NAT Gateway** option availability

### Short-term (1-2 weeks)
1. Wait for FRED response on whitelisting
2. If approved: Update with whitelisted IP
3. If rejected: Implement NAT Gateway solution

### Fallback (If all else fails)
Run bulk collection from local machine or dedicated VM with whitelisted IP

---

## 📝 Technical Details

### AKS Cluster Info
- **Cluster:** gzc-k8s-engine
- **Resource Group:** gzc-kubernetes-rg
- **Region:** East US
- **Outbound IP:** 172.171.167.93
- **Outbound IP Resource:** 61dc2962-c039-4fd1-af9f-f75638001236

### NAT Gateway Info (Alternative)
- **Resource Group:** fxspotstream-rg
- **Purpose:** Static IP for whitelisting (used for FX spot stream)
- **Status:** Operational

### Test Results
```bash
# From K8s pod (BLOCKED)
curl https://api.stlouisfed.org/fred/releases?api_key=92d2068e05feb04c9d4b4fbc53d2acd0&file_type=json
# → HTTP/2 403 (Akamai block)

# From local machine (WORKS)
curl https://api.stlouisfed.org/fred/releases?api_key=92d2068e05feb04c9d4b4fbc53d2acd0&file_type=json
# → HTTP 200 with valid JSON
```

---

## 🔗 Related Documentation

- **QC Features:** `QC_FEATURES.md`
- **K8s Job Manifest:** `k8s/bulk-collector-job.yaml`
- **Bulk Collector Code:** `bulk_collector.py`
- **Docker Build Log:** `/tmp/docker-build-v9-qc.log`

---

**Last Updated:** 2025-11-22
**Next Action:** Contact FRED support for IP whitelisting
