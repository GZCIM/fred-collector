# GitHub Actions Setup for Synapse Deployment

**Purpose:** Automate Synapse notebook deployment via GitHub Actions CI/CD pipeline
**Status:** Ready for implementation
**Last Updated:** 2025-11-23

---

## 📋 Overview

This document explains how to set up GitHub Actions for automated Synapse notebook deployment, solving the publishing requirement identified in SYNAPSE_DEPLOYMENT_LESSONS_LEARNED.md.

**The Problem:** REST API uploads create notebooks in draft mode - they cannot be executed until published.

**The Solution:** GitHub Actions with the `validateDeploy` operation automatically publishes notebooks as part of deployment.

---

## 🎯 Prerequisites

1. **GitHub Repository:** https://github.com/GZCIM/synapse-externaldata (or create new)
2. **Azure Service Principal** with Synapse Admin permissions
3. **GitHub Secrets** configured (see below)
4. **Synapse Workspace:**
   - Name: `externaldata`
   - Resource Group: `MTWS_Synapse`
   - Subscription: `6f928fec-8d15-47d7-b27b-be8b568e9789`

---

## 🔐 Step 1: Create Azure Service Principal

```bash
# Create service principal
az ad sp create-for-rbac \
  --name "github-actions-synapse-deploy" \
  --role contributor \
  --scopes /subscriptions/6f928fec-8d15-47d7-b27b-be8b568e9789/resourceGroups/MTWS_Synapse \
  --sdk-auth

# Output will be JSON - save it for GitHub secrets
```

**Assign Synapse Admin Role:**

```bash
# Get the service principal object ID
SP_OBJECT_ID=$(az ad sp list --display-name "github-actions-synapse-deploy" --query "[0].id" -o tsv)

# Assign Synapse Administrator role
az synapse role assignment create \
  --workspace-name externaldata \
  --role "Synapse Administrator" \
  --assignee $SP_OBJECT_ID
```

---

## 🔑 Step 2: Configure GitHub Secrets

Navigate to: **Repository Settings → Secrets and variables → Actions → New repository secret**

Create these secrets:

| Secret Name | Value | How to Get |
|-------------|-------|------------|
| `AZURE_CLIENT_ID` | Application (client) ID | From service principal JSON output |
| `AZURE_CLIENT_SECRET` | Client secret value | From service principal JSON output |
| `AZURE_TENANT_ID` | Directory (tenant) ID | From service principal JSON output |
| `AZURE_SUBSCRIPTION_ID` | `6f928fec-8d15-47d7-b27b-be8b568e9789` | Azure subscription ID |

---

## 📁 Step 3: Repository Structure

Your repository should have this structure:

```
synapse-externaldata/
├── .github/
│   ├── workflows/
│   │   └── synapse-deploy.yml          # CI/CD workflow
│   └── scripts/
│       └── convert_to_notebook.py      # Python → JSON converter
├── synapse_hello_flow.py               # Infrastructure test notebook
├── synapse_blob_to_delta.py           # Production transformation notebook
├── GITHUB_ACTIONS_SETUP.md            # This file
└── README.md                           # Project documentation
```

**Files Already Created:**
- ✅ `.github/workflows/synapse-deploy.yml`
- ✅ `.github/scripts/convert_to_notebook.py`
- ✅ `synapse_hello_flow.py`
- ✅ `synapse_blob_to_delta.py`

---

## 🚀 Step 4: Initialize GitHub Repository

```bash
# Navigate to project directory
cd "/Users/mikaeleage/Research & Analytics Services/Projects/azure-data-pipelines/services/fred-collector"

# Initialize git (if not already initialized)
git init

# Add remote (use existing repo or create new)
git remote add origin https://github.com/GZCIM/synapse-externaldata.git

# Add files
git add .github/ synapse_*.py GITHUB_ACTIONS_SETUP.md SYNAPSE_DEPLOYMENT_LESSONS_LEARNED.md

# Commit
git commit -m "Add GitHub Actions CI/CD for Synapse deployment

- Automated notebook publishing via validateDeploy operation
- Converts Python files to Synapse JSON format
- Deploys to externaldata workspace
- Eliminates manual publishing requirement"

# Push to main branch
git push -u origin main
```

---

## ⚙️ Step 5: How the Workflow Works

### Trigger Conditions

The workflow runs on:
1. **Push to main** when `synapse_*.py` files change
2. **Manual dispatch** via GitHub Actions UI

### Workflow Steps

```yaml
1. Checkout repository
2. Azure Login (using service principal)
3. Set up Python 3.11
4. Convert Python files to notebook JSON format
5. Deploy to Synapse using validateDeploy operation
6. Display deployment summary
```

### What Gets Deployed

- `synapse_hello_flow.py` → `hello_flow_test` notebook
- `synapse_blob_to_delta.py` → `blob_to_delta_transformer` notebook

### Key Configuration

```yaml
operation: 'validateDeploy'  # ← This validates AND publishes automatically
DeleteArtifactsNotInTemplate: 'false'  # Keep existing notebooks
ArtifactsFolder: './synapse-artifacts'  # Generated notebook JSONs
```

---

## ✅ Step 6: Verify Deployment

### After Pushing to GitHub

1. **Check GitHub Actions:**
   - Go to: https://github.com/GZCIM/synapse-externaldata/actions
   - Verify workflow runs successfully
   - Check deployment logs

2. **Check Synapse Studio:**
   - Open: https://web.azuresynapse.net?workspace=/subscriptions/6f928fec-8d15-47d7-b27b-be8b568e9789/resourceGroups/MTWS_Synapse/providers/Microsoft.Synapse/workspaces/externaldata
   - Navigate: Develop → Notebooks
   - Confirm notebooks are **Published** (not draft)

3. **Execute Hello Flow Test:**
   ```python
   # Via Synapse Studio
   # 1. Open hello_flow_test notebook
   # 2. Attach to fredsparkpool
   # 3. Click "Run All"
   # 4. Expected duration: 2-5 minutes
   # 5. Should complete all 6 tests successfully
   ```

---

## 🔄 Development Workflow

### For Notebook Changes

1. **Edit Python file locally:**
   ```bash
   vim synapse_hello_flow.py
   # Make your changes
   ```

2. **Commit and push:**
   ```bash
   git add synapse_hello_flow.py
   git commit -m "Update hello flow: add new test case"
   git push
   ```

3. **Automatic deployment:**
   - GitHub Actions triggers automatically
   - Notebook is converted to JSON
   - Deployed and published to Synapse
   - Ready to execute immediately

### For New Notebooks

1. **Create Python file:**
   ```bash
   touch synapse_new_feature.py
   # Write PySpark code
   ```

2. **Update conversion script:**
   ```python
   # Edit .github/scripts/convert_to_notebook.py
   notebooks = [
       {
           "source": "synapse_new_feature.py",
           "name": "new_feature_notebook",
           "description": "New feature description"
       },
       # ... existing notebooks
   ]
   ```

3. **Commit and push:**
   ```bash
   git add synapse_new_feature.py .github/scripts/convert_to_notebook.py
   git commit -m "Add new feature notebook"
   git push
   ```

---

## 🐛 Troubleshooting

### Workflow Fails with "Notebook not found"

**Problem:** Notebooks deployed but not published
**Solution:** Verify `operation: 'validateDeploy'` in workflow YAML (not just 'deploy')

### Authentication Failures

**Problem:** "Unauthorized" or "Forbidden" errors
**Solution:** Verify service principal has Synapse Administrator role:

```bash
az synapse role assignment list \
  --workspace-name externaldata \
  --query "[?principalId=='<SP_OBJECT_ID>']"
```

### Conversion Script Errors

**Problem:** Python → JSON conversion fails
**Solution:** Check Python file syntax and encoding:

```bash
python .github/scripts/convert_to_notebook.py
# Should show "✅ CONVERSION COMPLETE"
```

### Notebooks Don't Appear in Synapse

**Problem:** Deployment succeeds but notebooks missing
**Solution:** Check ArtifactsFolder path and notebook JSON generation:

```bash
ls -la synapse-artifacts/notebook/
# Should see: hello_flow_test.json, blob_to_delta_transformer.json
```

---

## 📊 Monitoring and Logs

### GitHub Actions Logs

```bash
# View latest workflow run
gh run list --limit 1

# View detailed logs
gh run view --log
```

### Synapse Execution Logs

After notebook execution in Synapse:
- View in Synapse Studio: Monitor → Pipeline runs
- Check Spark application logs
- Review output cells for errors

---

## 🔒 Security Best Practices

1. **Never commit secrets** to repository
2. **Use service principal** (not personal credentials)
3. **Rotate secrets** every 90 days
4. **Limit service principal scope** to required resource group only
5. **Enable branch protection** on main branch
6. **Require PR reviews** before merge to production

---

## 📚 References

- **Official Synapse CI/CD Docs:** https://learn.microsoft.com/en-us/azure/synapse-analytics/cicd/continuous-integration-delivery
- **GitHub Action:** https://github.com/Azure/Synapse-workspace-deployment
- **Lessons Learned:** `SYNAPSE_DEPLOYMENT_LESSONS_LEARNED.md`

---

## 🎯 Next Steps

After successful hello flow deployment:

1. **Validate Infrastructure:** Confirm hello_flow_test executes successfully
2. **Deploy Production Notebook:** Push `synapse_blob_to_delta.py`
3. **Execute Transformation:** Run blob_to_delta_transformer for 341 releases
4. **Benchmark Performance:** Compare Synapse (expected 10-20 min) vs local (15-25 min)
5. **Set Up Monitoring:** Configure alerting for failed executions

---

**Created:** 2025-11-23
**Author:** Claude Code
**Purpose:** Automate Synapse notebook deployment and eliminate manual publishing
