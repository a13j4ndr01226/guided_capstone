import os
from dotenv import load_dotenv
from utils.logger_config import get_logger
from utils.latest_ingestion import get_latest_run_path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def apply_latest(df, key_cols, arrival_col="arrival_tm", tiebreaker_cols=None):
    """
    Keep only the latest-arriving record per business key.
    """
    if tiebreaker_cols is None:
        tiebreaker_cols = []

    order_cols = [F.col(arrival_col).desc()] + [
        F.col(c).desc() for c in tiebreaker_cols
    ]

    w = Window.partitionBy(*key_cols).orderBy(*order_cols)

    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def main():
    log = get_logger("step3")
    log.info("Starting")
    load_dotenv("config/.env")

    step2_base = os.environ["STEP2_OUTPUT_BASE"]
    step3_base = os.environ["STEP3_OUTPUT_BASE"]

    storage_acct = os.environ["AZURE_STORAGE_ACCOUNT"]
    storage_key = os.environ["AZURE_STORAGE_KEY"]

    spark = (
        SparkSession.builder
        .appName("guided-capstone-step3")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4,"
            "com.microsoft.azure:azure-storage:8.6.6"
        )
        .getOrCreate()
    )

    spark.conf.set(
        f"fs.azure.account.key.{storage_acct}.blob.core.windows.net",
        storage_key
    )

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set(
        f"fs.azure.account.key.{storage_acct}.blob.core.windows.net",
        storage_key
    )

    print("CONF CHECK:",
      hconf.get(f"fs.azure.account.key.{storage_acct}.blob.core.windows.net") is not None)


    log.info("Spark created")

    # ---------------------------
    # Finding latest run
    # ---------------------------
    log.info(f"Finding latest run under {step2_base}")
    input_run_path = get_latest_run_path(spark, step2_base)

    trade_common = spark.read.parquet(f"{input_run_path}/partition=T")
    log.info(f"trade_common count: {trade_common.count()}")
    print("Trades Schema")
    trade_common.printSchema()
    
    quote_common = spark.read.parquet(f"{input_run_path}/partition=Q")
    log.info(f"quote_common count: {quote_common.count()}")  
    print("Quotes Schema")
    quote_common.printSchema()

    # ---------------------------
    # Trades
    # ---------------------------
    trade_key = [
        "trade_dt",
        "symbol",
        "exchange",
        "event_tm",
        "event_seq_nb"
    ]

    trade = trade_common.select(
        "trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm","trade_pr"
    )

    trade_corrected = apply_latest(trade, trade_key, arrival_col="arrival_tm")

    log.info(f"trade_corrected count: {trade_corrected.count()}")
    log.info(f"Writing trades to {step3_base}/trade")
    
    trade_corrected.write \
        .mode("overwrite") \
        .partitionBy("trade_dt") \
        .parquet(f"{step3_base}/trade")

    # ---------------------------
    # Quotes
    # ---------------------------
    quote_key = trade_key  # same business key

    quote = quote_common.select(
    "trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm",
    "bid_pr","bid_size","ask_pr","ask_size")
    
    quote_corrected = apply_latest(quote, trade_key, arrival_col="arrival_tm")
    
    log.info(f"quote_corrected count: {quote_corrected.count()}")
    log.info(f"Writing quotes to {step3_base}/quote")

    quote_corrected.write \
        .mode("overwrite") \
        .partitionBy("trade_dt") \
        .parquet(f"{step3_base}/quote")

    # ---------------------------
    # Validation
    # ---------------------------
    spark.read.parquet(f"{step3_base}/trade").groupBy("trade_dt").count().show()
    
    spark.read.parquet(f"{step3_base}/quote").groupBy("trade_dt").count().show()

    print("step 3 complete")
    spark.stop()


if __name__ == "__main__":
    main()
