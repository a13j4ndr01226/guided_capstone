"""
Guided Capstone Step 2 - Local Spark (WSL) + Azure Blob



"""

import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.azure_blob import configure_wasbs_account_key
from utils.logger_config import get_logger
from dotenv import load_dotenv
from pathlib import Path

# Load .env from config/ directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT/ "config" / ".env")

# ------------------------------------------------------------
# Create SparkSession
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("guided-capstone-step2")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "com.microsoft.azure:azure-storage:8.6.6"
        ])
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# 1) Logging + run metadata
# ------------------------------------------------------------
log = get_logger("step2")
run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

# ------------------------------------------------------------
# 2) Azure credentials (from env vars)
# ------------------------------------------------------------
STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT")
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER")
STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY")

if not STORAGE_ACCOUNT or not AZURE_CONTAINER or not STORAGE_KEY:
    raise RuntimeError("Missing Azure env variables.")

configure_wasbs_account_key(spark, STORAGE_ACCOUNT, STORAGE_KEY)

# ------------------------------------------------------------
# 3) Azure paths (same behavior as Databricks)
# ------------------------------------------------------------
csv_root  = f"wasbs://{AZURE_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/data/csv/"
json_root = f"wasbs://{AZURE_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/data/json/"

out_base = (
    f"wasbs://{AZURE_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/"
    f"output/market_exchange_parquet/run_{run_id}"
)

limit_n = 0  # set >0 for testing

# ------------------------------------------------------------
# 4) Read CSV
# ------------------------------------------------------------
log.info(f"Reading CSV recursively from: {csv_root}")

csv_lines = (
    spark.read.format("text")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.txt")
    .load(csv_root)
)

if limit_n:
    csv_lines = csv_lines.limit(limit_n)

csv_lines = csv_lines.filter(~F.trim(F.col("value")).startswith("{"))

parts = F.split(F.col("value"), ",")
csv_len = F.size(parts)

csv_is_q = (parts.getItem(2) == "Q") & (csv_len >= 11)
csv_is_t = (parts.getItem(2) == "T") & (csv_len >= 8)
csv_is_valid = csv_is_q | csv_is_t

csv_df = (
    csv_lines
    .withColumn("trade_dt", F.to_date(parts.getItem(0), "yyyy-MM-dd"))
    .withColumn("arrival_tm", F.to_timestamp(parts.getItem(1), "yyyy-MM-dd HH:mm:ss.SSS"))
    .withColumn("symbol", parts.getItem(3))
    .withColumn("event_tm", F.to_timestamp(parts.getItem(4), "yyyy-MM-dd HH:mm:ss.SSS"))
    .withColumn("event_seq_nb", parts.getItem(5).cast("long"))
    .withColumn("exchange", parts.getItem(6))
    .withColumn("rec_type", parts.getItem(2))
    .withColumn("bid_pr", F.when(csv_is_q, parts.getItem(7).cast("double")))
    .withColumn("bid_size", F.when(csv_is_q, parts.getItem(8).cast("long")))
    .withColumn("ask_pr", F.when(csv_is_q, parts.getItem(9).cast("double")))
    .withColumn("ask_size", F.when(csv_is_q, parts.getItem(10).cast("long")))
    .withColumn("trade_pr", F.when(csv_is_t, parts.getItem(7).cast("double")))
    .withColumn("partition", F.when(csv_is_valid, parts.getItem(2)).otherwise(F.lit("B")))
    .select(
        "trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb",
        "arrival_tm", "trade_pr", "bid_pr", "bid_size", "ask_pr", "ask_size", "partition"
    )
)

# ------------------------------------------------------------
# 5) Read JSON 
# ------------------------------------------------------------
log.info(f"Reading JSON recursively from: {json_root}")

json_lines = (
    spark.read.format("text")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.txt")
    .load(json_root)
)

if limit_n:
    json_lines = json_lines.limit(limit_n)

json_lines = json_lines.filter(F.trim(F.col("value")).startswith("{"))

json_map = json_lines.withColumn(
    "m",
    F.from_json(F.col("value"), "map<string,string>")
)

json_ok = F.col("m").isNotNull()
rtype = F.coalesce(F.col("m")["event_type"], F.col("m")["rec_type"])

json_is_q = rtype == "Q"
json_is_t = rtype == "T"
json_is_valid = json_is_q | json_is_t

json_df = (
    json_map
    .withColumn("trade_dt", F.expr("try_to_date(m['trade_dt'], 'yyyy-MM-dd')"))
    .withColumn("arrival_tm", F.expr("try_to_timestamp(m['file_tm'], 'yyyy-MM-dd HH:mm:ss.SSS')"))
    .withColumn("event_tm", F.expr("try_to_timestamp(m['event_tm'], 'yyyy-MM-dd HH:mm:ss.SSS')"))
    .withColumn("event_seq_nb", F.col("m")["event_seq_nb"].cast("long"))
    .withColumn("rec_type", rtype)
    .withColumn("symbol", F.col("m")["symbol"])
    .withColumn("exchange", F.col("m")["exchange"])
    .withColumn("trade_pr", F.when(json_is_t, F.col("m")["trade_pr"].cast("double")))
    .withColumn("bid_pr", F.when(json_is_q, F.col("m")["bid_pr"].cast("double")))
    .withColumn("bid_size", F.when(json_is_q, F.col("m")["bid_size"].cast("long")))
    .withColumn("ask_pr", F.when(json_is_q, F.col("m")["ask_pr"].cast("double")))
    .withColumn("ask_size", F.when(json_is_q, F.col("m")["ask_size"].cast("long")))
    .withColumn(
        "partition",
        F.when(json_ok & json_is_valid, rtype).otherwise(F.lit("B"))
    )
    .select(
        "trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb",
        "arrival_tm", "trade_pr", "bid_pr", "bid_size", "ask_pr", "ask_size", "partition"
    )
)

# ------------------------------------------------------------
# 6) Combine + write
# ------------------------------------------------------------
combined = csv_df.unionByName(json_df).cache()

log.info("Partition counts:")
for r in combined.groupBy("partition").count().collect():
    log.info(f"{r['partition']}: {r['count']}")

(
    combined
    .write
    .mode("overwrite")
    .partitionBy("partition")
    .parquet(out_base)
)

log.info(f"Step 2 complete. Output written to: {out_base}")
