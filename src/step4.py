import sys
import os
import datetime

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.latest_trade_date import get_latest_trade_date
from utils.logger_config import get_logger

import configparser
from utils.job_tracker import Tracker

# ---------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------------

def load_environment():

    load_dotenv("config/.env")

    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")
    container = os.getenv("AZURE_CONTAINER")

    return account, key, container


# ---------------------------------------------------
# CREATE SPARK SESSION
# ---------------------------------------------------

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("step4_analytical_etl")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6"
        )
        .config(
            "spark.hadoop.fs.wasbs.impl",
            "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
        )
        .config(
            "spark.hadoop.fs.wasbs.impl.disable.cache",
            "true"
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark


# ---------------------------------------------------
# CONFIGURE AZURE STORAGE ACCESS
# ---------------------------------------------------

def configure_azure_access(spark, account, key):

    spark.conf.set(
        f"fs.azure.account.key.{account}.blob.core.windows.net",
        key
    )


# ---------------------------------------------------
# READ STEP 3 DATA
# ---------------------------------------------------

def read_eod_data(spark, container, account, trade_date):

    base_path = f"wasbs://{container}@{account}.blob.core.windows.net/output/market_exchange_eod"

    trade_path = f"{base_path}/trade/trade_dt={trade_date}"
    quote_path = f"{base_path}/quote/trade_dt={trade_date}"

    trades_df = spark.read.parquet(trade_path)

    trades_df = trades_df.withColumn(
        "event_tm",
        F.to_timestamp("event_tm")
    ).withColumn(
        "trade_dt",
        F.lit(trade_date)
    )

    quotes_df = spark.read.parquet(quote_path)

    quotes_df = quotes_df.withColumn(
        "event_tm",
        F.to_timestamp("event_tm")
    ).withColumn(
        "trade_dt",
        F.lit(trade_date)
    )

    return trades_df, quotes_df

# ---------------------------------------------------
# BUILD 30-MINUTE MOVING AVERAGE STAGING TABLE
# ---------------------------------------------------

def build_trade_moving_avg(trades_df):

    window_spec = (
        Window
        .partitionBy("symbol", "exchange")
        .orderBy(F.col("event_tm").cast("long"))
        .rangeBetween(-1800, 0)
    )

    mov_avg_df = trades_df.withColumn(
        "mov_avg_pr",
        F.avg("trade_pr").over(window_spec)
    )

    return mov_avg_df


# ---------------------------------------------------
# GET PREVIOUS DAY CLOSE PRICE
# ---------------------------------------------------

def build_previous_day_close(spark, container, account, prev_date):

    base_path = f"wasbs://{container}@{account}.blob.core.windows.net/output/market_exchange_eod"

    prev_trade_path = f"{base_path}/trade/trade_dt={prev_date}"

    prev_trades = spark.read.parquet(prev_trade_path)

    prev_trades = prev_trades.withColumn(
        "event_tm",
        F.to_timestamp("event_tm")
    )

    window_spec = (
        Window
        .partitionBy("symbol", "exchange")
        .orderBy(
            F.col("event_tm").desc(),
            F.col("event_seq_nb").desc()
        )
    )

    last_trade_df = (
        prev_trades
        .withColumn("rn", F.row_number().over(window_spec))
        .filter("rn = 1")
        .select(
            "symbol",
            "exchange",
            F.col("trade_pr").alias("close_pr")
        )
    )

    return last_trade_df


# ---------------------------------------------------
# BUILD UNION EVENT TIMELINE
# ---------------------------------------------------

def build_union_stream(quotes_df, mov_avg_df):

    trade_stream = (
        mov_avg_df
        .select(
            "trade_dt",
            F.lit("T").alias("rec_type"),
            "symbol",
            "event_tm",
            "event_seq_nb",
            "exchange",
            F.lit(None).alias("bid_pr"),
            F.lit(None).alias("bid_size"),
            F.lit(None).alias("ask_pr"),
            F.lit(None).alias("ask_size"),
            "trade_pr",
            "mov_avg_pr"
        )
        .withColumn("rec_sort", F.lit(0)) # trades first
    )

    quote_stream = (
        quotes_df
        .select(
            "trade_dt",
            F.lit("Q").alias("rec_type"),
            "symbol",
            "event_tm",
            "event_seq_nb",
            "exchange",
            "bid_pr",
            "bid_size",
            "ask_pr",
            "ask_size",
            F.lit(None).alias("trade_pr"),
            F.lit(None).alias("mov_avg_pr")
        )
        .withColumn("rec_sort", F.lit(1)) # quotes after
    )

    union_df = trade_stream.unionByName(quote_stream)

    return union_df


# ---------------------------------------------------
# LAST TRADE VALUES
# ---------------------------------------------------

def enrich_quotes(union_df):

    window_spec = (
        Window
        .partitionBy("symbol", "exchange")
        .orderBy("event_tm", "event_seq_nb", "rec_sort")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    enriched = (
        union_df
        .withColumn(
            "last_trade_pr",
            F.last("trade_pr", ignorenulls=True).over(window_spec)
        )
        .withColumn(
            "last_mov_avg_pr",
            F.last("mov_avg_pr", ignorenulls=True).over(window_spec)
        )
    )

    quotes_only = enriched.filter("rec_type = 'Q'")

    return quotes_only


# ---------------------------------------------------
# JOIN PREVIOUS DAY CLOSE
# ---------------------------------------------------

def join_close_prices(quotes_df, close_df):

    final_df = (
        quotes_df
        .join(
            F.broadcast(close_df),
            ["symbol", "exchange"],
            "left"
        )
        .withColumn(
            "bid_pr_mv",
            F.col("bid_pr") - F.col("close_pr")
        )
        .withColumn(
            "ask_pr_mv",
            F.col("ask_pr") - F.col("close_pr")
        )
    )

    return final_df


# ---------------------------------------------------
# WRITE OUTPUT
# ---------------------------------------------------

def write_output(df, container, account, trade_date):

    output_path = f"wasbs://{container}@{account}.blob.core.windows.net/output/quote-trade-analytical/date={trade_date}"

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )


# ---------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------

def main():
    log = get_logger("step4")
    log.info("Starting Step 4")

    account, key, container = load_environment()

    spark = create_spark_session()

    configure_azure_access(spark, account, key)

    trade_date = get_latest_trade_date(spark, container, account)

    log.info(f"Processing trade date: {trade_date}")

    prev_date = trade_date - datetime.timedelta(days=1)
    prev_date = prev_date.strftime("%Y-%m-%d")

    log.info(f"Previous trade date: {prev_date}")
    
    trades_df, quotes_df = read_eod_data(
        spark, container, account, trade_date
    )

    mov_avg_df = build_trade_moving_avg(trades_df)

    close_df = build_previous_day_close(
        spark, container, account, prev_date
    )

    union_df = build_union_stream(quotes_df, mov_avg_df)

    enriched_quotes = enrich_quotes(union_df)

    final_df = join_close_prices(enriched_quotes, close_df)

    final_df.explain(True)

    final_df.explain("formatted")

    # Validate row count match
    log.info("quotes input:", quotes_df.count())
    log.info("quotes output:", final_df.count())

    # Null checks
    final_df.filter("last_trade_pr IS NULL").count()

    # Spot check
    final_df.orderBy("symbol", "event_tm").show(20, False)

    write_output(final_df, container, account, trade_date)

    spark.stop()

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    tracker = Tracker("analytical_etl", config)

    try:
        main()
        tracker.update_job_status("success")

    except Exception as e:
        print(e)
        tracker.update_job_status("failed")