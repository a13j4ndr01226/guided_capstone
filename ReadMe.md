# Guided Capstone – Market Data Engineering Pipeline

This project implements a multi-stage data engineering pipeline that ingests
stock exchange market data, processes it using Apache Spark, and produces an
analytical dataset suitable for market analysis.

The pipeline performs the following stages:

1. Data ingestion of semi-structured exchange submissions
2. Transformation and validation of quote and trade records
3. End-of-day (EOD) dataset generation
4. Construction of an analytical quote-trade dataset with derived metrics

The pipeline is executed using Apache Spark running locally on Linux,
while data is read from and written to Azure Blob Storage.

---

## Pipeline Flow

Raw Exchange Data
        ↓
Step 2 – Data Ingestion
        ↓
Partitioned Parquet Dataset
        ↓
Step 3 – End-of-Day Processing
        ↓
Trade and Quote EOD Datasets
        ↓
Step 4 – Analytical Dataset
        ↓
Quote-Trade Analytical Output

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

## Step 4 Overview

Step Four builds the **Quote-Trade Analytical Dataset** used for downstream
analysis and trading signal evaluation.

This stage consumes the Step 3 End-of-Day datasets and produces a unified
analytical view combining quote events, trade events, moving averages,
and closing prices.

Key processing steps include:

- Calculating a **30-minute moving average trade price** for each symbol and exchange
- Combining quote and trade events into a **single ordered event stream**
- Forward-filling the latest trade price and moving average for quote events
- Joining with **previous-day closing prices**
- Computing **bid and ask price movement relative to the closing price**

The final dataset is written as partitioned Parquet files to Azure Blob Storage
for efficient querying and analysis.

---

## Step 5 Overview

Step Five introduces pipeline job tracking using PostgreSQL.  
This stage records the execution status of the analytical pipeline to provide
basic monitoring and observability for pipeline runs.

A `job_tracker` table is created in a local PostgreSQL database to store the
status of each pipeline execution. The Step 4 analytical pipeline integrates a
Python tracking utility that updates the job status after execution.

Key functionality includes:

- Generating a daily job identifier (`jobname_YYYY-MM-DD`)
- Recording pipeline execution status (`success` or `failed`)
- Storing the timestamp of the latest update
- Updating existing records using PostgreSQL UPSERT (`ON CONFLICT`) logic

This allows the pipeline to maintain a simple execution log for monitoring
successful runs and identifying failures.

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

## Environment Setup

The project was developed using:

- Python 3.x
- Apache Spark 3.x
- WSL2 (Ubuntu)
- Azure Blob Storage

Create and activate a virtual environment:

python -m venv venv
source venv/bin/activate

Install dependencies:

pip install -r requirements.txt

Azure credentials are stored locally using environment variables and loaded
via `.env` using `python-dotenv`.

Step 5 introduces the PostgreSQL driver:

psycopg2-binary (Included in teh requirements.txt)

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

### Step 4 Output
output/quote-trade-analytical/
└── date=YYYY-MM-DD/
    ├── part-*.parquet
    └── _SUCCESS

The Step 4 dataset contains the analytical event stream with the following
derived metrics:

- `last_trade_pr` – latest trade price carried forward
- `last_mov_avg_pr` – moving average trade price
- `close_pr` – previous day closing price
- `bid_pr_mv` – bid price movement relative to close
- `ask_pr_mv` – ask price movement relative to close

## How to Run

Step 2 – Ingest raw exchange data

python -m src.step2


Step 3 – Build End-of-Day datasets

python -m src.step3


Step 4 – Generate analytical quote-trade dataset

spark-submit src/step4.py

## Notes

Secrets and credentials are not stored in the repository

Azure Blob Storage access is configured via storage account access keys

Spark output is partitioned using partitionBy() for efficiency

Step 3 overwrites existing EOD outputs to support safe re-runs during development