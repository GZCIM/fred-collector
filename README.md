# FRED Collector Service

Containerized service for collecting FRED economic data using V2 bulk API and storing in Azure Delta Lake.

## Overview

The FRED Collector Service:
- Collects FRED releases using the V2 bulk API (100-200x faster than V1)
- Writes to Delta Lake with complete vintage tracking
- Supports both event-driven and one-time collection modes
- Runs in Kubernetes with auto-scaling
- Integrates with Azure Key Vault for API keys
- Publishes collection events to Azure Event Hub

## Components

### `collector.py`

Core collection logic:
- **FREDCollectorV2**: Main collector class
- V2 API integration with pagination
- Round-robin API key rotation (9 keys)
- DataFrame transformation
- Delta Lake merge integration
- Parallel and sequential collection modes

### `main.py`

Service entry point:
- **FREDCollectorService**: Main service class
- Event Hub consumer integration
- Azure Key Vault integration for secrets
- Spark session management
- Graceful shutdown handling
- Two run modes: `event_hub` (listener) or `one_time` (batch)

### `Dockerfile`

Container definition:
- Based on `bitnami/spark:3.4.1`
- Includes Python 3 and PySpark
- Delta Lake 2.4.0 with Scala 2.12
- Azure SDK dependencies
- Non-root user for security

### `requirements.txt`

Python dependencies:
- httpx (async HTTP)
- pyspark, delta-spark
- Azure SDKs (identity, keyvault, storage, eventhub)
- Monitoring and logging libraries

## Configuration

All configuration is in `/infrastructure/delta-lake/config.yaml`:

```yaml
storage:
  account_name: "{{ STORAGE_ACCOUNT_NAME }}"
  container: "macroeconomic-maintained-series"

authentication:
  client_id: "{{ MANAGED_IDENTITY_CLIENT_ID }}"
  tenant_id: "{{ AZURE_TENANT_ID }}"

fred_api:
  key_vault:
    name: "gzc-finma-keyvault"
    secret_prefix: "fred-api-key-"
    total_keys: 9

event_hub:
  enabled: true
  namespace: "{{ EVENT_HUB_NAMESPACE }}"
  topics:
    data_updated: "fred-data-updated"
```

## Run Modes

### Mode 1: Event Hub Listener (Production)

Listens to Event Hub for new release notifications:

```bash
# Set environment
export RUN_MODE=event_hub

# Run service
python main.py
```

**Event format:**
```json
{
  "release_id": 441,
  "release_name": "Coinbase Cryptocurrencies",
  "update_date": "2025-11-18",
  "priority": "normal"
}
```

### Mode 2: One-Time Collection (Initial Load)

Collects all releases once:

```bash
# Set environment
export RUN_MODE=one_time
export RELEASE_IDS_FILE=./release_ids.txt

# Run service
python main.py
```

**release_ids.txt format:**
```
9
10
11
...
1105
```

## Docker Build

```bash
# Build image
docker build -t fred-collector:latest .

# Tag for Azure Container Registry
docker tag fred-collector:latest gzcacr.azurecr.io/fred-collector:latest

# Push to ACR
az acr login --name gzcacr
docker push gzcacr.azurecr.io/fred-collector:latest
```

## Kubernetes Deployment

See `/kubernetes/deployments/fred-collector.yaml` for full deployment manifest.

**Quick deploy:**
```bash
kubectl apply -f ../../kubernetes/deployments/fred-collector.yaml
```

**Key features:**
- Horizontal Pod Autoscaler (2-10 pods)
- Managed identity for Azure authentication
- ConfigMap for configuration
- Resource limits (CPU: 2-4 cores, Memory: 8-16GB)
- Liveness and readiness probes

## API Key Management

API keys are stored in Azure Key Vault:

```bash
# List keys
az keyvault secret list --vault-name gzc-finma-keyvault --query "[?starts_with(name, 'fred-api-key')]"

# Add new key
az keyvault secret set \
  --vault-name gzc-finma-keyvault \
  --name fred-api-key-1 \
  --value "{{ YOUR_FRED_API_KEY }}"

# Rotate key
az keyvault secret set \
  --vault-name gzc-finma-keyvault \
  --name fred-api-key-1 \
  --value "{{ NEW_KEY }}"
```

## Monitoring

### Logs

```bash
# View logs
kubectl logs -f deployment/fred-collector -n fred-pipeline

# Filter for specific release
kubectl logs deployment/fred-collector -n fred-pipeline | grep "release 10"
```

### Metrics

Collected metrics (via Application Insights):
- Collection duration per release
- Observations inserted/updated/deleted
- API calls made
- Success/failure rates
- Memory and CPU usage

### Health Check

```bash
# Check pod health
kubectl get pods -n fred-pipeline -l app=fred-collector

# Describe pod
kubectl describe pod <pod-name> -n fred-pipeline
```

## Performance

### Collection Speed

Based on testing with release 441 (4 series, 13,715 observations):
- **V2 API**: ~3 seconds total
- **V1 API**: ~5-10 minutes (100-200x slower)

### Capacity

With 9 API keys:
- **Theoretical max**: 120 req/min × 9 = 1,080 req/min
- **Safe sustained**: ~500 req/min with retry handling
- **Daily capacity**: Easily handles all 321 releases

### Resource Usage

Per pod (typical):
- **CPU**: 2 cores (bursts to 4)
- **Memory**: 8 GB (Delta Lake operations)
- **Storage**: Minimal (writes to Delta Lake)

## Troubleshooting

### Issue: 401 Unauthorized on V2 API (CRITICAL)

**Root Cause:** FRED V2 API requires Bearer token authentication, NOT query parameter.

```bash
# WRONG (V1 style) - Returns 401 on V2 endpoints:
curl "https://api.stlouisfed.org/fred/v2/release/observations?api_key=xxx"

# CORRECT (V2 style) - Works:
curl -H "Authorization: Bearer xxx" "https://api.stlouisfed.org/fred/v2/release/observations"
```

**In Python (httpx):**
```python
# WRONG - query param auth
params = {"api_key": api_key, ...}
response = await client.get(url, params=params)

# CORRECT - Bearer header
headers = {"Authorization": f"Bearer {api_key}"}
response = await client.get(url, params=params, headers=headers)
```

**Documentation:** https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html

**Fixed:** 2025-11-21 in collector.py lines 107-114

### Issue: API Rate Limiting

```bash
# Check logs for 429 errors
kubectl logs deployment/fred-collector | grep "429"

# Reduce parallelism
# Edit config.yaml: performance.parallel_collections = 1
```

### Issue: Delta Lake Write Failures

```bash
# Check Spark logs
kubectl logs deployment/fred-collector | grep "Delta"

# Verify storage account access
kubectl exec -it <pod-name> -- env | grep AZURE
```

### Issue: Event Hub Connection

```bash
# Verify connection string
kubectl get secret event-hub-secrets -n fred-pipeline -o yaml

# Check Event Hub status
az eventhub namespace show \
  --name <namespace> \
  --resource-group <rg> \
  --query "status"
```

## Development

### Local Testing

```python
import asyncio
from collector import collect_releases_to_delta

# Collect specific releases
results = asyncio.run(
    collect_releases_to_delta(
        release_ids=[441, 10, 228],
        parallel=False
    )
)

print(results)
```

### Unit Tests

```bash
# Run tests (TODO: implement)
pytest tests/
```

## Security

### Authentication

- **Managed Identity**: Primary authentication method
- **Key Vault**: Stores all secrets
- **No hardcoded credentials**: All secrets from Key Vault

### Network

- **Private endpoints**: Delta Lake access
- **Firewall rules**: Restrict Key Vault access
- **Network policies**: K8s pod-to-pod communication

### Container Security

- Non-root user (UID 1000)
- Read-only root filesystem (where possible)
- Security context applied
- Image scanning with Azure Container Registry

## Next Steps

After collector deployment:

1. **Test with single release:**
   ```bash
   kubectl exec -it <pod> -- python -c "
   import asyncio
   from collector import collect_releases_to_delta
   asyncio.run(collect_releases_to_delta([441]))
   "
   ```

2. **Deploy release monitor** (Phase 3)
   - Monitors FRED releases/dates API
   - Publishes to Event Hub
   - Triggers collector

3. **Set up monitoring** (Phase 7)
   - Application Insights dashboards
   - Alert rules
   - Performance metrics

4. **Optimize performance:**
   - Tune Spark executor configuration
   - Adjust auto-scaling rules
   - Optimize Delta Lake partitioning

## Support

For issues or questions:
- Check logs: `kubectl logs deployment/fred-collector`
- Review `/docs/IMPLEMENTATION_PLAN.md`
- Consult `/infrastructure/delta-lake/README.md`

---

**Created:** 2025-11-19
**Status:** Phase 2 Complete
**Next Phase:** Release Monitor Service
