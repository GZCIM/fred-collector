# FRED Collector Service - Docker Container
# Base image with Python 3.10 (compatible with PySpark 3.4.1)

FROM python:3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    gnupg \
    ca-certificates \
    procps \
    unzip \
    zip \
    && rm -rf /var/lib/apt/lists/*

# Install Java 11 from Temurin (Eclipse Adoptium)
RUN mkdir -p /etc/apt/keyrings && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

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

# Copy application code
COPY collector.py .
COPY main.py .
COPY delta_lake/ ./delta_lake/

# Create directories for logs and data
RUN mkdir -p /app/logs /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=/usr/local/bin/python3.10 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run as non-root user
RUN useradd -m -u 1000 fred && chown -R fred:fred /app
USER fred

# Default command
CMD ["python", "main.py"]
