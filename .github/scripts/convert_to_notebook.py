#!/usr/bin/env python3
"""
Convert Python files to Synapse notebook format

This script converts .py files (like synapse_hello_flow.py and synapse_blob_to_delta.py)
into the JSON notebook format expected by Azure Synapse Analytics.
"""

import json
import os
from pathlib import Path

def convert_py_to_notebook(py_file_path: str, notebook_name: str, spark_pool: str = "fredsparkpool"):
    """
    Convert a Python file to Synapse notebook JSON format

    Args:
        py_file_path: Path to the Python source file
        notebook_name: Name for the notebook
        spark_pool: Spark pool to attach (default: fredsparkpool)

    Returns:
        dict: Notebook JSON structure
    """
    # Read Python file
    with open(py_file_path, 'r') as f:
        python_code = f.read()

    # Create notebook structure matching Synapse format
    notebook = {
        "name": notebook_name,
        "properties": {
            "nbformat": 4,
            "nbformat_minor": 2,
            "bigDataPool": {
                "referenceName": spark_pool,
                "type": "BigDataPoolReference"
            },
            "sessionProperties": {
                "driverMemory": "28g",
                "driverCores": 4,
                "executorMemory": "28g",
                "executorCores": 4,
                "numExecutors": 2,
                "conf": {
                    "spark.dynamicAllocation.enabled": "false",
                    "spark.dynamicAllocation.minExecutors": "2",
                    "spark.dynamicAllocation.maxExecutors": "2",
                    "spark.autotune.trackingId": notebook_name
                }
            },
            "metadata": {
                "saveOutput": True,
                "enableDebugMode": False,
                "kernelspec": {
                    "name": "synapse_pyspark",
                    "display_name": "Synapse PySpark"
                },
                "language_info": {
                    "name": "python"
                },
                "a365ComputeOptions": {
                    "id": f"/subscriptions/6f928fec-8d15-47d7-b27b-be8b568e9789/resourceGroups/MTWS_Synapse/providers/Microsoft.Synapse/workspaces/externaldata/bigDataPools/{spark_pool}",
                    "name": spark_pool,
                    "type": "Spark",
                    "endpoint": f"https://externaldata.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/{spark_pool}",
                    "auth": {
                        "type": "AAD",
                        "authResource": "https://dev.azuresynapse.net"
                    },
                    "sparkVersion": "3.3",
                    "nodeCount": 4,
                    "cores": 8,
                    "memory": 56,
                    "automaticScaleJobs": False
                },
                "sessionKeepAliveTimeout": 30
            },
            "cells": [
                {
                    "cell_type": "code",
                    "metadata": {
                        "collapsed": False
                    },
                    "source": [line + '\n' for line in python_code.split('\n')],
                    "outputs": [],
                    "execution_count": None
                }
            ]
        }
    }

    return notebook


def main():
    """
    Main conversion function

    Converts all synapse_*.py files to notebook JSON format
    """
    print("=" * 80)
    print("SYNAPSE NOTEBOOK CONVERSION")
    print("=" * 80)
    print()

    # Get project root (2 levels up from .github/scripts)
    project_root = Path(__file__).parent.parent.parent

    # Create output directory
    output_dir = project_root / "synapse-artifacts" / "notebook"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define notebooks to convert
    notebooks = [
        {
            "source": "synapse_hello_flow.py",
            "name": "hello_flow_test",
            "description": "Infrastructure test notebook"
        },
        {
            "source": "synapse_blob_to_delta.py",
            "name": "blob_to_delta_transformer",
            "description": "Production FRED transformation notebook"
        }
    ]

    converted_count = 0

    for notebook_config in notebooks:
        source_file = project_root / notebook_config["source"]

        if not source_file.exists():
            print(f"⚠️  Skipping {notebook_config['source']} (not found)")
            continue

        print(f"Converting: {notebook_config['source']}")
        print(f"  → Notebook: {notebook_config['name']}")
        print(f"  → Description: {notebook_config['description']}")

        # Convert to notebook format
        notebook_json = convert_py_to_notebook(
            str(source_file),
            notebook_config["name"],
            spark_pool="fredsparkpool"
        )

        # Write JSON file
        output_file = output_dir / f"{notebook_config['name']}.json"
        with open(output_file, 'w') as f:
            json.dump(notebook_json, f, indent=2)

        print(f"  ✅ Written: {output_file}")
        print()

        converted_count += 1

    print("=" * 80)
    print(f"✅ CONVERSION COMPLETE: {converted_count} notebooks converted")
    print("=" * 80)
    print()
    print("Output directory:")
    print(f"  {output_dir}")
    print()
    print("Next steps:")
    print("  1. Commit and push to GitHub")
    print("  2. GitHub Actions will automatically deploy to Synapse")
    print("  3. Notebooks will be published and ready to execute")
    print()


if __name__ == "__main__":
    main()
