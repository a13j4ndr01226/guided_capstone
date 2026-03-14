from pyspark.sql import functions as F


def get_latest_trade_date(spark, container, account):
    """
    Detect the most recent trade_dt partition in Step 3 output.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    container : str
        Azure Blob Storage container name
    account : str
        Azure storage account name

    Returns
    -------
    str
        Latest trade_dt value (YYYY-MM-DD)
    """

    base_path = (
        f"wasbs://{container}@{account}.blob.core.windows.net/"
        "output/market_exchange_eod/trade"
    )

    df = spark.read.parquet(base_path)

    latest_date = (
        df.select("trade_dt")
        .distinct()
        .orderBy(F.col("trade_dt").desc())
        .limit(1)
        .collect()[0][0]
    )

    return latest_date