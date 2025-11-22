# MAINTENANCE.md - FRED Collector Service

**Project:** FRED Collector Service
**Location:** `/Users/mikaeleage/Research & Analytics Services/Projects/azure-data-pipelines/services/fred-collector`
**GitHub Repository:** `GZCIM/fred-collector` (to be created)
**Last Updated:** 2025-11-22

---

## 📋 Documentation Maintenance Requirements

### Required Documentation Files

All documentation files must be kept synchronized and updated with each significant change:

1. **PROJECT_MANIFEST.json** - Project specification and metadata
2. **DEV_JOURNAL.md** - Chronological development history (Layer 3 documentation)
3. **HANDOVER_PROMPT.md** - Session continuation guide
4. **README.md** - Overview and usage guide
5. **DEPLOYMENT.md** - Kubernetes deployment instructions
6. **QC_FEATURES.md** - Quality control features documentation
7. **MAINTENANCE.md** - This file (maintenance procedures)

### Documentation Update Triggers

Update documentation when:

- ✅ **Code Changes**: New features, bug fixes, critical patches
- ✅ **Deployment Changes**: K8s configuration, image tags, resource limits
- ✅ **Architecture Changes**: EventHub integration, data storage, API changes
- ✅ **Configuration Changes**: Environment variables, secrets, Azure resources
- ✅ **Build Changes**: New Docker images, Dockerfile modifications
- ✅ **Performance Changes**: Resource usage, optimization results

---

## 🔄 Documentation Review Requirements

### Before Every GitHub Push

**Mandatory Checklist:**

- [ ] **DEV_JOURNAL.md** - Add entry for changes made (date, author, description)
- [ ] **HANDOVER_PROMPT.md** - Update "Last Updated" date and status
- [ ] **PROJECT_MANIFEST.json** - Update `lastUpdated`, `currentBuild`, `status` if changed
- [ ] **README.md** - Update if user-facing features changed
- [ ] **DEPLOYMENT.md** - Update if K8s configuration changed

### Documentation Standards

**DEV_JOURNAL.md Format:**
```markdown
## YYYY-MM-DD - [Change Description]
**Author:** [Your Name]

### [Section: Changes/Critical Fix/etc.]
[Description of what was changed and why]

**Files Modified:**
- `path/to/file.py` lines X-Y
- `path/to/other.py` lines A-B

**Impact:**
- [Describe the impact of the change]

**Testing:**
- [Describe how it was tested]
```

**HANDOVER_PROMPT.md Updates:**
- Update "Last Updated" date at top
- Update "Current Production Status" section
- Add to "Critical: Akamai Blocking Issue" if relevant (currently contains INCORRECT info - needs revision)
- Update "Next Steps" if priorities changed

**PROJECT_MANIFEST.json Updates:**
```json
{
  "lastUpdated": "YYYY-MM-DD",
  "deployment": {
    "currentBuild": "ca##",
    "containerImage": "gzcacr.azurecr.io/fred-collector:ca##"
  },
  "status": "deployed|testing|blocked"
}
```

---

## 🐙 GitHub Push Maintenance

### Repository Setup (One-Time)

```bash
# Navigate to project
cd "/Users/mikaeleage/Research & Analytics Services/Projects/azure-data-pipelines/services/fred-collector"

# Initialize Git (if not already done)
git init

# Add remote
git remote add origin https://github.com/GZCIM/fred-collector.git

# Create .gitignore
cat > .gitignore <<'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
.venv/
*.egg-info/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Logs
*.log
logs/

# Secrets (CRITICAL - never commit!)
*.env
.env.*
**/secrets/
credentials.json
*.key

# Build artifacts
dist/
build/
*.egg

# Docker
.dockerignore

# Azure
.azure/

# Testing
.pytest_cache/
.coverage
htmlcov/
EOF

# Initial commit
git add .
git commit -m "Initial commit: FRED Collector Service

- Kubernetes-deployed FRED data collection service
- V2 bulk API integration with 9-key rotation
- Delta Lake storage with vintage tracking
- EventHub listener for incremental updates
- Complete documentation and deployment manifests

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>"

# Push to GitHub
git branch -M main
git push -u origin main
```

### Pre-Push Checklist

**BEFORE every `git push`, complete ALL items:**

1. **Documentation Review** (see above)
   - [ ] DEV_JOURNAL.md updated
   - [ ] HANDOVER_PROMPT.md updated
   - [ ] PROJECT_MANIFEST.json updated
   - [ ] README.md updated if needed
   - [ ] DEPLOYMENT.md updated if needed

2. **Code Quality**
   - [ ] No hardcoded secrets or credentials
   - [ ] No debugging print statements
   - [ ] Code follows project conventions
   - [ ] Comments added for complex logic

3. **Testing**
   - [ ] Local testing completed
   - [ ] Docker image builds successfully
   - [ ] K8s manifests validated (if changed)

4. **Git Hygiene**
   - [ ] Meaningful commit message
   - [ ] Changes are atomic (one logical change per commit)
   - [ ] No accidental file additions (check `git status`)

### Standard Push Workflow

```bash
# 1. Check status
git status

# 2. Review changes
git diff

# 3. Stage files (selective)
git add PROJECT_MANIFEST.json DEV_JOURNAL.md HANDOVER_PROMPT.md
git add [other modified files]

# 4. Commit with meaningful message
git commit -m "Brief summary (50 chars or less)

Detailed description of changes:
- What was changed
- Why it was changed
- How it was tested

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>"

# 5. Push to GitHub
git push origin main
```

### Commit Message Format

**Structure:**
```
<type>: <subject line (max 50 chars)>

<body: detailed description (wrap at 72 chars)>
- Bullet points for changes
- Why the change was needed
- How it was tested

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
```

**Types:**
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes only
- `refactor:` Code refactoring (no functional changes)
- `test:` Adding or modifying tests
- `chore:` Maintenance tasks (dependencies, config)
- `deploy:` Deployment or infrastructure changes

**Examples:**
```
feat: Add EventHub listener for incremental updates

- Implemented EventHub consumer in main.py
- Added event_hub run mode to collector
- Configured Azure Event Hub integration
- Tested with release update events

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
```

```
fix: Correct Akamai blocking analysis in HANDOVER_PROMPT.md

- Removed incorrect Akamai WAF blocking theory
- Updated with evidence of successful 2025-11-22 collection
- Documented K8s rolling IP behavior via load balancer
- Marked actual issue as deployment/configuration error

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
```

```
deploy: Update K8s deployment to use ca43 EventHub listener

- Changed image from v2-bulk to ca43-no-databricks-props
- Added RUN_MODE=event_hub environment variable
- Updated deployment.yaml with correct configuration
- Verified deployment with kubectl apply

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
```

---

## 🐳 Docker Image Maintenance

### Building and Pushing Images

**Event-driven collector (main service):**
```bash
cd "/Users/mikaeleage/Research & Analytics Services/Projects/azure-data-pipelines/services/fred-collector"

# Build
docker build -f Dockerfile -t gzcacr.azurecr.io/fred-collector:ca48 .

# Test locally (optional)
docker run --rm gzcacr.azurecr.io/fred-collector:ca48 python -c "import sys; print(sys.version)"

# Push to registry
docker push gzcacr.azurecr.io/fred-collector:ca48
```

**Bulk collector (one-time jobs):**
```bash
# Build
docker build -f Dockerfile.bulk -t gzcacr.azurecr.io/fred-bulk-collector:v11 .

# Push
docker push gzcacr.azurecr.io/fred-bulk-collector:v11
```

### Image Tagging Strategy

**Semantic Versioning:**
- Event-driven collector: `ca##` (e.g., ca43, ca44, ca45)
- Bulk collector: `v#` or `v#-descriptive` (e.g., v10, v10-k8s-fix, v11-qc)

**Tag Format:**
- `ca##` - Critical architecture change number
- `ca##-descriptive` - Descriptive suffix for specific fixes
- `latest` - DO NOT USE in production (unreliable)

**After Pushing New Image:**
1. Update PROJECT_MANIFEST.json `currentBuild` and `containerImage`
2. Update DEV_JOURNAL.md with build notes
3. Update k8s/deployment.yaml if deploying
4. Commit and push to GitHub

---

## ☸️ Kubernetes Deployment Maintenance

### Deployment Updates

**Update image tag:**
```bash
kubectl set image deployment/fred-collector fred-collector=gzcacr.azurecr.io/fred-collector:ca48 -n fred-pipeline
```

**Update environment variables:**
```bash
kubectl set env deployment/fred-collector RUN_MODE=event_hub -n fred-pipeline
```

**Apply full deployment file:**
```bash
kubectl apply -f k8s/deployment.yaml
```

### Monitoring Deployments

**Check deployment status:**
```bash
kubectl get deployment fred-collector -n fred-pipeline -o wide
```

**Check pod status:**
```bash
kubectl get pods -n fred-pipeline -l app=fred-collector
```

**View logs:**
```bash
kubectl logs -f deployment/fred-collector -n fred-pipeline
```

**Check recent events:**
```bash
kubectl describe deployment fred-collector -n fred-pipeline | tail -20
```

### Rollback Procedure

If new deployment fails:

```bash
# Rollback to previous version
kubectl rollout undo deployment/fred-collector -n fred-pipeline

# Check rollout status
kubectl rollout status deployment/fred-collector -n fred-pipeline

# Verify rollback
kubectl get deployment fred-collector -n fred-pipeline -o wide
```

---

## 📊 Data Quality Checks

### Verify Data Collection

**Check Delta Lake data:**
```bash
cd query_tool
poetry run python -c "
from fred_query import FREDQuery
q = FREDQuery()
print(q.info())
series = q.list_series(limit=100000)
print(f'Series collected: {series.shape[0]:,}')
"
```

**Check blob storage:**
```bash
az storage blob list \
  --account-name gzcstorageaccount \
  --container-name macroeconomic-maintained-series \
  --prefix "US_Fred_Data/raw/" \
  --output table | wc -l
```

### Collection Health Monitoring

**Check collection history:**
```python
import polars as pl

uri = 'az://macroeconomic-maintained-series/US_Fred_Data/collection_history'
storage_options = {'account_name': 'gzcstorageaccount', 'use_azure_cli': 'true'}

history = pl.scan_delta(uri, storage_options=storage_options) \
    .select(['collection_timestamp', 'release_id', 'status', 'observations_inserted']) \
    .filter(pl.col('collection_timestamp') > '2025-11-20') \
    .collect()

print(history)
```

---

## 🔐 Secrets Management

### NEVER Commit Secrets!

**Forbidden to commit:**
- API keys (FRED, Azure)
- Connection strings
- Passwords
- Certificates
- `.env` files
- `credentials.json`

**Safe practices:**
- ✅ Use Azure Key Vault for all secrets
- ✅ Reference secrets via Managed Identity
- ✅ Add sensitive files to .gitignore
- ✅ Use environment variables in K8s secrets
- ✅ Rotate keys periodically (documented in Key Vault)

**If secret accidentally committed:**
```bash
# Immediately rotate the compromised secret
# Remove from Git history (contact senior engineer)
# Update .gitignore to prevent recurrence
```

---

## 🚨 Critical Issues Tracking

### Known Issues

**1. Akamai Blocking Theory (RESOLVED - was misdiagnosis)**
- **Status**: FALSE - K8s can access FRED API
- **Evidence**: 2025-11-22 collection at 01:23 UTC successful with 186 files
- **Root Cause**: Likely deployment/configuration error, NOT IP blocking
- **Action**: Update HANDOVER_PROMPT.md and AKAMAI_BLOCKING_ISSUE.md

**2. Wrong Deployment Configuration (ACTIVE)**
- **Status**: NEEDS FIX
- **Issue**: Running v2-bulk image instead of ca43-no-databricks-props
- **Impact**: Production EventHub listener not deployed
- **Fix**: Update deployment.yaml and apply to K8s

**3. Raw Parquet Files Not in Delta Lake (ACTIVE)**
- **Status**: DATA GAP
- **Issue**: 321 releases in raw files, only 49 in Delta Lake
- **Impact**: Incomplete Delta Lake data
- **Fix**: Load raw data into Delta Lake tables

---

## 📅 Regular Maintenance Schedule

### Daily
- [ ] Check K8s pod health (`kubectl get pods -n fred-pipeline`)
- [ ] Review logs for errors (`kubectl logs -f deployment/fred-collector -n fred-pipeline`)

### Weekly
- [ ] Verify Delta Lake data freshness (query_tool)
- [ ] Check Azure Key Vault key rotation dates
- [ ] Review Application Insights metrics (when enabled)

### Monthly
- [ ] Update dependencies (requirements.txt)
- [ ] Review and update documentation
- [ ] Cost optimization analysis (Azure resources)

### Quarterly
- [ ] Comprehensive system review
- [ ] Performance tuning based on metrics
- [ ] Security audit (secrets, access control)

---

## 📞 Support and Escalation

### Contacts
- **Team**: Data Engineering
- **Owner**: Mikael Eage
- **Support**: Check logs with kubectl, review docs/IMPLEMENTATION_PLAN.md

### Troubleshooting Resources
- **Local Docs**: DEV_JOURNAL.md, DEPLOYMENT.md, QC_FEATURES.md
- **FRED API Docs**: https://fred.stlouisfed.org/docs/api/fred/
- **Delta Lake Docs**: https://delta.io/
- **Azure Docs**: https://docs.microsoft.com/azure/

### Common Issues

**Pod not starting:**
```bash
kubectl describe pod <pod-name> -n fred-pipeline
kubectl logs <pod-name> -n fred-pipeline
```

**API key exhaustion:**
- Check Key Vault for key rotation
- Verify round-robin logic in collector.py
- Monitor rate limits (120 req/min per key)

**Delta Lake write failures:**
- Check Azure storage account access
- Verify Managed Identity permissions
- Review merge_logic.py for schema compatibility

---

**Last Updated:** 2025-11-22
**Maintainer:** Data Engineering Team
**GitHub Repository:** GZCIM/fred-collector

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
