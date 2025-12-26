# Databricks notebook source
# ============================================
# Guided Capstone Step 2 - Databricks Notebook Main
# ============================================

import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ------------------------------------------------------------
# 0) Make your Workspace folder importable so `utils/` works
# ------------------------------------------------------------
PROJECT_DIR = "/Workspace/Users/alejandro_2539@live.com"
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from utils.azure_blob import configure_wasbs_account_key
from utils.logger_config import get_logger

log = get_logger("main")
run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

# ------------------------------------------------------------
# 1) Pull secrets from Key Vault-backed Databricks scope
# ------------------------------------------------------------
SECRET_SCOPE = "azure-secret"
STORAGE_ACCOUNT = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account-name")
STORAGE_KEY     = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account-key")

if not STORAGE_ACCOUNT or not STORAGE_KEY:
    raise RuntimeError("Storage secrets missing. Check scope/key names.")

# ------------------------------------------------------------
# 2) Paths (Blob input, DBFS output)
# ------------------------------------------------------------
CONTAINER = "guided-cs"
csv_root  = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/csv/"
json_root = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/json/"

out_base = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/output/market_exchange_parquet/run_{run_id}"


# Set for quick testing, then turn off:
limit_n = 0  # 100 for test, 0 disables

spark.sparkContext.setLogLevel("WARN")
configure_wasbs_account_key(spark, STORAGE_ACCOUNT, STORAGE_KEY)

# ------------------------------------------------------------
# 3) Read CSV (text lines)
# ------------------------------------------------------------
log.info(f"Reading CSV recursively from: {csv_root}")

csv_lines = (
    spark.read.format("text")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.txt")
    .load(csv_root)
)
if limit_n and limit_n > 0:
    csv_lines = csv_lines.limit(limit_n)

csv_lines = csv_lines.filter(~F.trim(F.col("value")).startswith("{"))

parts = F.split(F.col("value"), ",")
csv_raw = csv_lines.withColumn("parts", parts).withColumn("rtype", parts.getItem(2))

csv_len = F.size("parts")
csv_is_q = (F.col("rtype") == "Q") & (csv_len >= 11)
csv_is_t = (F.col("rtype") == "T") & (csv_len >= 8)
csv_is_valid = csv_is_q | csv_is_t

csv_df = (
    csv_raw
    .withColumn("trade_dt", F.to_date(parts.getItem(0), "yyyy-MM-dd"))
    .withColumn("arrival_tm", F.to_timestamp(parts.getItem(1), "yyyy-MM-dd HH:mm:ss.SSS"))
    .withColumn("symbol", parts.getItem(3))
    .withColumn("event_tm", F.to_timestamp(parts.getItem(4), "yyyy-MM-dd HH:mm:ss.SSS"))
    .withColumn("event_seq_nb", parts.getItem(5).cast("long"))
    .withColumn("exchange", parts.getItem(6))
    .withColumn("rec_type", F.col("rtype"))
    .withColumn("bid_pr", F.when(csv_is_q, parts.getItem(7).cast("double")))
    .withColumn("bid_size", F.when(csv_is_q, parts.getItem(8).cast("long")))
    .withColumn("ask_pr", F.when(csv_is_q, parts.getItem(9).cast("double")))
    .withColumn("ask_size", F.when(csv_is_q, parts.getItem(10).cast("long")))
    .withColumn("trade_pr", F.when(csv_is_t, parts.getItem(7).cast("double")))
    .withColumn("partition", F.when(csv_is_valid, F.col("rtype")).otherwise(F.lit("B")))
    .select(
        "trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm",
        "trade_pr", "bid_pr", "bid_size", "ask_pr", "ask_size", "partition"
    )
)

# ------------------------------------------------------------
# 4) Read JSON (text lines -> from_json)
# ------------------------------------------------------------
log.info(f"Reading JSON recursively from: {json_root}")

json_lines = (
    spark.read.format("text")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.txt")
    .load(json_root)
)

if limit_n and limit_n > 0:
    json_lines = json_lines.limit(limit_n)

json_lines = json_lines.filter(F.trim(F.col("value")).startswith("{"))

# Parse as a permissive MAP first (handles missing fields & minor schema issues better)
json_map = (
    json_lines
    .withColumn("m", F.from_json(F.col("value"), "map<string,string>"))
)

# Flag records that failed JSON parsing
json_map = json_map.withColumn("json_ok", F.col("m").isNotNull())

# Pull fields safely from the map
trade_dt_s   = F.col("m")["trade_dt"]
file_tm_s    = F.col("m")["file_tm"]
event_type_s = F.col("m")["event_type"]
rec_type_s   = F.col("m")["rec_type"]
symbol_s     = F.col("m")["symbol"]
event_tm_s   = F.col("m")["event_tm"]
seq_s        = F.col("m")["event_seq_nb"]
exch_s       = F.col("m")["exchange"]
trade_pr_s   = F.col("m")["trade_pr"]
bid_pr_s     = F.col("m")["bid_pr"]
bid_size_s   = F.col("m")["bid_size"]
ask_pr_s     = F.col("m")["ask_pr"]
ask_size_s   = F.col("m")["ask_size"]

rtype = F.coalesce(event_type_s, rec_type_s)
json_is_q = (rtype == "Q")
json_is_t = (rtype == "T")
json_is_valid = json_is_q | json_is_t

# Use try_to_date / try_to_timestamp to avoid hard failures
json_df = (
    json_map
    .withColumn("trade_dt", F.expr("try_to_date(m['trade_dt'], 'yyyy-MM-dd')"))
    .withColumn("arrival_tm", F.expr("try_to_timestamp(m['file_tm'], 'yyyy-MM-dd HH:mm:ss.SSS')"))
    .withColumn("event_tm", F.expr("try_to_timestamp(m['event_tm'], 'yyyy-MM-dd HH:mm:ss.SSS')"))
    .withColumn("event_seq_nb", seq_s.cast("long"))
    .withColumn("rec_type", rtype)
    .withColumn("symbol", symbol_s)
    .withColumn("exchange", exch_s)
    .withColumn("trade_pr", F.when(json_is_t, trade_pr_s.cast("double")))
    .withColumn("bid_pr", F.when(json_is_q, bid_pr_s.cast("double")))
    .withColumn("bid_size", F.when(json_is_q, bid_size_s.cast("long")))
    .withColumn("ask_pr", F.when(json_is_q, ask_pr_s.cast("double")))
    .withColumn("ask_size", F.when(json_is_q, ask_size_s.cast("long")))
    .withColumn(
        "partition",
        F.when(F.col("json_ok") & json_is_valid, rtype).otherwise(F.lit("B"))
    )
    .select(
        "trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm",
        "trade_pr", "bid_pr", "bid_size", "ask_pr", "ask_size", "partition"
    )
)

# show a few JSON rows that failed parsing
bad = json_map.filter(~F.col("json_ok")).select("value").limit(5).collect()
for i, r in enumerate(bad, 1):
    log.info(f"Bad JSON sample {i}: {r['value'][:200]}")

# ------------------------------------------------------------
# 5) Combine + write once (optimal for Databricks)
# ------------------------------------------------------------
combined = csv_df.unionByName(json_df).cache()

log.info("Partition counts:")
counts = combined.groupBy("partition").count().collect()
for r in counts:
    log.info(f"{r['partition']}: {r['count']}")


(
    combined
    .write
    .mode("overwrite")
    .partitionBy("partition")   # creates partition=Q/T/B folders
    .parquet(out_base)
)

log.info(f"Done. Output written to: {out_base}")
display(dbutils.fs.ls(out_base))
