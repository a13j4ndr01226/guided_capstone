# Guided Capstone – Step 2: Data Ingestion with Spark

## Overview
This project implements the Step Two Data Ingestion pipeline for the Guided Capstone. 
The goal is to ingest semi-structured stock exchange data (CSV and JSON), parse it into a 
common Spark DataFrame schema, handle corrupted records, and persist the results as 
partitioned Parquet files.

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
- Azure Databricks
- PySpark
- Azure Blob Storage
- Azure Key Vault (via Databricks secret scope)

## Output Structure
output_dir/
partition=Q/
partition=T/
partition=B/

## How to Run
1. Upload source data to Azure Blob Storage
2. Configure secrets for storage account name and key
3. Run the Databricks notebook / script
4. Verify output in DBFS or Blob Storage

## Notes
- Secrets and credentials are not stored in the repository
- Output is partitioned using Spark's `partitionBy()` for efficiency


