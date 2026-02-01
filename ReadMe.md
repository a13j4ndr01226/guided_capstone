# Guided Capstone

## Step 2 Overview
This project implements the Step Two data ingestion pipeline for the Guided Capstone.  
The goal is to ingest semi-structured stock exchange data (CSV and JSON), parse it into a
common Spark DataFrame schema, handle corrupted records, and persist the results as
partitioned Parquet files.

The pipeline is executed using local Apache Spark running in WSL2 (Ubuntu) rather than Azure Databricks, while still reading from and writing to Azure Blob Storage.

---

## Step 3 Overview
Step Three implements the **End-of-Day (EOD) data load**.  
It consumes the partitioned Parquet output produced in Step 2, separates trade and quote
events into final datasets, applies record correction logic, and writes query-ready Parquet
outputs back to Azure Blob Storage.

The latest Step 2 ingestion run is automatically detected and used as input.

---

## Data Sources
- CSV and JSON daily exchange submissions
- Stored in Azure Blob Storage
- Accessed using `wasbs://` from local Spark (WSL2)

---

## Key Features
- Reads raw text files using Spark
- Parses CSV records using string splitting and record-type logic
- Parses JSON records using tolerant map-based parsing
- Routes invalid or corrupted records into a `B` (bad) partition
- Creates a unified event schema for trades and quotes
- Writes partitioned Parquet output in a single pass
- Applies EOD data correction logic to retain only the latest version of each record

---

## Technologies
- Apache Spark (local execution via WSL2)
- PySpark
- Azure Blob Storage
- Hadoop Azure connector (`hadoop-azure`)
- Python virtual environment
- `python-dotenv` for local secret management

---

## Output Structure

### Step 2 Output
output/market_exchange_parquet/
├── run_YYYYMMDD_HHMMSS_microseconds/
│   ├── partition=Q/
│   ├── partition=T/
│   ├── partition=B/
│   └── _SUCCESS

### Step 3 Output
output/market_exchange_eod/
├── trade/
│   └── trade_dt=YYYY-MM-DD/
├── quote/
│   └── trade_dt=YYYY-MM-DD/

## How to Run

python -m src.step2
python -m src.step3

## Notes

Secrets and credentials are not stored in the repository

Azure Blob Storage access is configured via storage account access keys

Spark output is partitioned using partitionBy() for efficiency

Step 3 overwrites existing EOD outputs to support safe re-runs during development