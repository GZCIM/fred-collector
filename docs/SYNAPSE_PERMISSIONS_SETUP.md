# Azure Synapse Permissions Setup Guide

**Complete Guide to Synapse Pipeline Execution via REST API**

This document captures the complete permission setup process for programmatic control of Azure Synapse notebooks via REST API. Follow this guide to avoid repeating the troubleshooting process.

---

## Table of Contents

1. [Problem Overview](#problem-overview)
2. [Solution Architecture](#solution-architecture)
3. [Service Principal Configuration](#service-principal-configuration)
4. [Permission Setup (Step-by-Step)](#permission-setup-step-by-step)
5. [Pipeline Creation](#pipeline-creation)
6. [GitHub Actions Integration](#github-actions-integration)
7. [Testing & Verification](#testing--verification)
8. [Troubleshooting Common Issues](#troubleshooting-common-issues)

---

## Problem Overview

**Goal:** Execute Azure Synapse notebooks programmatically via REST API for automated data transformation

**Challenge:** Azure Synapse notebooks cannot be executed directly via REST API. They must be wrapped in a Synapse Pipeline, which requires specific RBAC permissions.

**Solution:** Create a Synapse Pipeline that calls the notebook, then grant the service principal the necessary permissions to execute pipelines via REST API.

---

## Solution Architecture

```
GitHub Actions Workflow
    ↓
Azure Service Principal (ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc)
    ↓
OAuth Bearer Token (https://dev.azuresynapse.net)
    ↓
Synapse REST API (externaldata.dev.azuresynapse.net)
    ↓
Synapse Pipeline (blob_to_delta_pipeline)
    ↓
Synapse Notebook (blob_to_delta_transformer)
    ↓
Spark Pool (fredsparkpool)
```

---

## Service Principal Configuration

### Service Principal Details

- **Application (Client) ID:** `ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc`
- **Display Name:** `gzc-github-oidc-prod`
- **Authentication Method:** OpenID Connect (OIDC) - Passwordless
- **Subscription:** `6f928fec-8d15-47d7-b27b-be8b568e9789`
- **Resource Group:** `MTWS_Synapse`
- **Synapse Workspace:** `externaldata`

### GitHub Actions Configuration

The service principal is configured for OIDC authentication in GitHub Actions. No client secret is stored in GitHub Secrets.

**GitHub Secrets Required:**
- `AZURE_CLIENT_ID`: ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc
- `AZURE_TENANT_ID`: Your Azure AD tenant ID
- `AZURE_SUBSCRIPTION_ID`: 6f928fec-8d15-47d7-b27b-be8b568e9789

---

## Permission Setup (Step-by-Step)

### Understanding Synapse RBAC

Azure Synapse has a **multi-layered permission system**:

1. **Azure RBAC** - Controls access to the Synapse workspace resource itself
2. **Synapse RBAC** - Controls access to objects within the workspace (pipelines, notebooks, etc.)

**CRITICAL:** Both layers require specific permissions for programmatic execution.

### Error 1: Missing Synapse Contributor Permission

#### Symptoms
```
Error: 401 Unauthorized
Message: "The principal 'ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc' does not have the required Synapse RBAC permission to perform this action."
Required permission: Action: Microsoft.Synapse/workspaces/read
```

#### Root Cause
The service principal lacked basic read permission on the Synapse workspace.

#### Solution
Grant **Synapse Contributor** role:

```bash
az synapse role assignment create \
  --workspace-name externaldata \
  --role "Synapse Contributor" \
  --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc
```

#### Verification
```bash
# List all role assignments for the service principal
az synapse role assignment list \
  --workspace-name externaldata \
  --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc
```

**Result:**
- Role Assignment ID: `4a777980-53ab-41ac-aa52-67bf2c648818`
- Role Definition: Synapse Contributor
- Scope: workspaces/externaldata

---

### Error 2: Missing Credential Access Permission

#### Symptoms
```
Error: 403 Forbidden (AccessControlUnauthorized)
Message: "Insufficient permissions to call this API. ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc does not have Microsoft.Synapse/workspaces/credentials/useSecret/action on scope workspaces/externaldata/credentials/WorkspaceSystemIdentity"
```

#### Root Cause
Even with Synapse Contributor role, the service principal needed explicit permission to use workspace credentials.

#### Solution
Grant **Synapse Credential User** role:

```bash
az synapse role assignment create \
  --workspace-name externaldata \
  --role "Synapse Credential User" \
  --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc
```

#### Verification
```bash
# Verify both roles are assigned
az synapse role assignment list \
  --workspace-name externaldata \
  --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc \
  --output table
```

**Result:**
- Role Assignment ID: `01f9d53d-61e7-45cb-8610-f8fbac429491`
- Role Definition: Synapse Credential User
- Scope: workspaces/externaldata/credentials/WorkspaceSystemIdentity

---

## Pipeline Creation

### Pipeline Definition

The Synapse Pipeline wraps the notebook execution and provides the REST API endpoint.

**File:** `blob_to_delta_pipeline.json`

```json
{
  "name": "blob_to_delta_pipeline",
  "properties": {
    "description": "Executes blob_to_delta_transformer notebook with configurable parameters",
    "activities": [
      {
        "name": "Transform_Blob_to_Delta",
        "type": "SynapseNotebook",
        "typeProperties": {
          "notebook": {
            "referenceName": "blob_to_delta_transformer",
            "type": "NotebookReference"
          },
          "parameters": {
            "storage_account": {
              "value": "@pipeline().parameters.storage_account",
              "type": "string"
            },
            "container": {
              "value": "@pipeline().parameters.container",
              "type": "string"
            },
            "vintage_date": {
              "value": "@pipeline().parameters.vintage_date",
              "type": "string"
            },
            "release_id": {
              "value": "@pipeline().parameters.release_id",
              "type": "string"
            }
          },
          "sparkPool": {
            "referenceName": "fredsparkpool",
            "type": "BigDataPoolReference"
          }
        }
      }
    ],
    "parameters": {
      "storage_account": { "type": "string", "defaultValue": "gzcstorageaccount" },
      "container": { "type": "string", "defaultValue": "macroeconomic-maintained-series" },
      "vintage_date": { "type": "string", "defaultValue": "2025-11-21" },
      "release_id": { "type": "string", "defaultValue": "" }
    }
  }
}
```

### Deployment via Azure CLI

```bash
# Deploy pipeline to Synapse workspace
az synapse pipeline create \
  --workspace-name externaldata \
  --name blob_to_delta_pipeline \
  --file blob_to_delta_pipeline.json
```

---

## GitHub Actions Integration

### Complete Workflow

**File:** `.github/workflows/synapse-execute.yml`

```yaml
name: Execute Synapse Blob-to-Delta Transformation

on:
  workflow_dispatch:
    inputs:
      vintage_date:
        description: 'Vintage date (YYYY-MM-DD)'
        required: true
        default: '2025-11-21'
      release_id:
        description: 'Release ID (leave empty for all releases)'
        required: false
        default: ''

jobs:
  execute-transformation:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Azure Login
        uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Trigger Synapse Pipeline Execution
        run: |
          echo "Getting Azure access token..."
          TOKEN=$(az account get-access-token --resource=https://dev.azuresynapse.net --query accessToken -o tsv)

          # Build parameters JSON
          PARAMS=$(cat <<EOF
          {
            "storage_account": "gzcstorageaccount",
            "container": "macroeconomic-maintained-series",
            "vintage_date": "${{ github.event.inputs.vintage_date }}",
            "release_id": "${{ github.event.inputs.release_id }}"
          }
          EOF
          )

          # Trigger pipeline execution via REST API
          RUN_RESPONSE=$(curl -X POST \
            "https://externaldata.dev.azuresynapse.net/pipelines/blob_to_delta_pipeline/createRun?api-version=2020-12-01" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"parameters\": $PARAMS}")

          echo "Response: $RUN_RESPONSE"

          # Extract run ID
          RUN_ID=$(echo $RUN_RESPONSE | jq -r '.runId // .id // empty')

          if [ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ]; then
            echo "✅ Pipeline execution started successfully"
            echo "Run ID: $RUN_ID"
            echo "Monitor execution at: https://web.azuresynapse.net/monitoring/pipelineruns"
          else
            echo "❌ Failed to start pipeline execution"
            echo "Full response: $RUN_RESPONSE"
            exit 1
          fi
```

### Local Testing

Test the pipeline execution locally without GitHub Actions:

```bash
# Create test script
cat > /tmp/test_pipeline.sh << 'EOF'
#!/bin/bash
TOKEN=$(az account get-access-token --resource=https://dev.azuresynapse.net --query accessToken -o tsv)

curl -X POST \
  "https://externaldata.dev.azuresynapse.net/pipelines/blob_to_delta_pipeline/createRun?api-version=2020-12-01" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"storage_account": "gzcstorageaccount", "container": "macroeconomic-maintained-series", "vintage_date": "2025-11-21", "release_id": ""}}' | jq .
EOF

chmod +x /tmp/test_pipeline.sh
/tmp/test_pipeline.sh
```

---

## Testing & Verification

### Step 1: Verify Service Principal Permissions

```bash
# Check both role assignments exist
az synapse role assignment list \
  --workspace-name externaldata \
  --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc \
  --output table

# Expected output:
# Role                      RoleDefinitionId                              PrincipalId
# ------------------------  --------------------------------------------  --------------------------------------
# Synapse Contributor       4a777980-53ab-41ac-aa52-67bf2c648818          ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc
# Synapse Credential User   01f9d53d-61e7-45cb-8610-f8fbac429491          ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc
```

### Step 2: Test Pipeline Execution

```bash
# Trigger pipeline via REST API
TOKEN=$(az account get-access-token --resource=https://dev.azuresynapse.net --query accessToken -o tsv)

RUN_RESPONSE=$(curl -X POST \
  "https://externaldata.dev.azuresynapse.net/pipelines/blob_to_delta_pipeline/createRun?api-version=2020-12-01" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"storage_account": "gzcstorageaccount", "container": "macroeconomic-maintained-series", "vintage_date": "2025-11-21", "release_id": ""}}')

echo $RUN_RESPONSE | jq .

# Expected successful response:
# {
#   "runId": "6db984e5-f281-4364-a7ea-2faf10884582"
# }
```

### Step 3: Monitor Pipeline Run

```bash
# Check pipeline run status
RUN_ID="6db984e5-f281-4364-a7ea-2faf10884582"  # From above response

az synapse pipeline-run show \
  --workspace-name externaldata \
  --run-id $RUN_ID

# Or via REST API
TOKEN=$(az account get-access-token --resource=https://dev.azuresynapse.net --query accessToken -o tsv)

curl -X GET \
  "https://externaldata.dev.azuresynapse.net/pipelineruns/$RUN_ID?api-version=2020-12-01" \
  -H "Authorization: Bearer $TOKEN" | jq .
```

### Step 4: View in Azure Portal

Monitor execution at: https://web.azuresynapse.net/monitoring/pipelineruns

---

## Troubleshooting Common Issues

### Issue 1: "Principal does not have required Synapse RBAC permission"

**Error Code:** 401 Unauthorized

**Solution:**
```bash
az synapse role assignment create \
  --workspace-name externaldata \
  --role "Synapse Contributor" \
  --assignee <your-service-principal-id>
```

### Issue 2: "Insufficient permissions to call this API"

**Error Code:** 403 Forbidden (AccessControlUnauthorized)

**Solution:**
```bash
az synapse role assignment create \
  --workspace-name externaldata \
  --role "Synapse Credential User" \
  --assignee <your-service-principal-id>
```

### Issue 3: Notebook Not Published

**Symptoms:** Pipeline execution succeeds but notebook fails immediately or shows "Notebook not found"

**Root Cause:** Changes to notebooks require publishing before they take effect.

**Solution:** Use CI/CD with `validateDeploy` operation:

```yaml
- name: Deploy Synapse Artifacts
  uses: azure/synapse-workspace-deployment@v1.0.0
  with:
    operation: 'validateDeploy'  # This compiles AND publishes
    TargetWorkspaceName: 'externaldata'
    ArtifactsFolder: './synapse-artifacts'
    ResourceGroupName: 'MTWS_Synapse'
```

### Issue 4: Invalid Token

**Symptoms:** 401 errors with message about invalid or expired token

**Solution:** Ensure you're requesting a token for the correct resource:

```bash
# CORRECT - for Synapse REST API
az account get-access-token --resource=https://dev.azuresynapse.net

# WRONG - this is for Azure Resource Manager
az account get-access-token --resource=https://management.azure.com
```

### Issue 5: Pipeline Run Fails with SyntaxError

**Symptoms:** Pipeline triggers successfully but fails with Python SyntaxError in notebook

**Root Cause:** Notebook code is improperly formatted (entire code on one line)

**Solution:**
1. Use the conversion script: `.github/scripts/convert_to_notebook.py`
2. Deploy via CI/CD with `validateDeploy` operation
3. Verify notebook in Synapse Studio before running pipeline

---

## Summary Checklist

Use this checklist to set up Synapse programmatic access for a new service principal:

- [ ] **Step 1:** Create service principal (or use existing)
- [ ] **Step 2:** Grant "Synapse Contributor" role
  ```bash
  az synapse role assignment create \
    --workspace-name externaldata \
    --role "Synapse Contributor" \
    --assignee <service-principal-id>
  ```
- [ ] **Step 3:** Grant "Synapse Credential User" role
  ```bash
  az synapse role assignment create \
    --workspace-name externaldata \
    --role "Synapse Credential User" \
    --assignee <service-principal-id>
  ```
- [ ] **Step 4:** Create Synapse Pipeline that wraps notebook
- [ ] **Step 5:** Deploy pipeline to Synapse workspace
- [ ] **Step 6:** Test pipeline execution via REST API
- [ ] **Step 7:** Integrate with GitHub Actions (optional)
- [ ] **Step 8:** Verify notebook is properly published

---

## Reference Commands

### Quick Command Reference

```bash
# Grant Synapse Contributor
az synapse role assignment create --workspace-name externaldata --role "Synapse Contributor" --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc

# Grant Synapse Credential User
az synapse role assignment create --workspace-name externaldata --role "Synapse Credential User" --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc

# List role assignments
az synapse role assignment list --workspace-name externaldata --assignee ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc --output table

# Get access token
az account get-access-token --resource=https://dev.azuresynapse.net --query accessToken -o tsv

# Trigger pipeline
curl -X POST "https://externaldata.dev.azuresynapse.net/pipelines/blob_to_delta_pipeline/createRun?api-version=2020-12-01" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"parameters": {"storage_account": "gzcstorageaccount", "container": "macroeconomic-maintained-series", "vintage_date": "2025-11-21", "release_id": ""}}'

# Check pipeline run status
az synapse pipeline-run show --workspace-name externaldata --run-id <run-id>
```

---

## Additional Resources

- **Azure Synapse REST API Reference:** https://learn.microsoft.com/en-us/rest/api/synapse/
- **Synapse RBAC Roles:** https://learn.microsoft.com/en-us/azure/synapse-analytics/security/synapse-workspace-synapse-rbac-roles
- **GitHub Actions for Synapse:** https://github.com/Azure/synapse-workspace-deployment

---

**Last Updated:** 2025-11-23
**Author:** Automated setup documentation
**Synapse Workspace:** externaldata
**Service Principal:** ae9c4f22-b5ab-47ab-b0eb-9d82f5ef2acc
