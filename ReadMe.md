# Guided Capstone – Step 2: Data Ingestion with Spark

## Overview
This project implements the Step Two Data Ingestion pipeline for the Guided Capstone.
The goal is to ingest semi-structured stock exchange data (CSV and JSON), parse it into a
common Spark DataFrame schema, handle corrupted records, and persist the results as
partitioned Parquet files.

The pipeline is executed using local Apache Spark running in WSL2 (Ubuntu) rather than Azure Databricks, while still reading from and writing to Azure Blob Storage.

## Data Sources
- CSV and JSON daily exchange submissions
- Stored in Azure Blob Storage
- Accessed using `wasbs://` from Azure Databricks

## Key Features
- Reads raw text files using Spark
- Parses CSV records using string splitting and record-type logic
- Parses JSON records using tolerant map-based parsing
- Routes invalid or corrupted records into a `B` (bad) partition
- Creates a unified event schema for trades and quotes
- Writes partitioned Parquet output in a single pass

## Technologies
- Apache Spark (local execution via WSL2)
- PySpark
- Azure Blob Storage
- Hadoop Azure connector (hadoop-azure)
- Python virtual environment
- python-dotenv for local secret management

## Output Structure
output_dir/
partition=Q/
partition=T/
partition=B/

## How to Run
1. Upload source data to Azure Blob Storage
2. Store Azure Storage credentials in config/.env
3. Ensure WSL2, Java, and PySpark are installed and configured
4. From project root, run : python -m src.step2

## Notes
- Secrets and credentials are not stored in the repository
- Azure Blob Storage access is configured via storage account access keys
- Output is partitioned using Spark's `partitionBy()` for efficiency


