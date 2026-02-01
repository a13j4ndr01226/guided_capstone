""" 
# utils/azure_blob.py
"""


def configure_wasbs_account_key(spark, storage_account: str, storage_key: str) -> None:
    if not storage_key:
        raise RuntimeError("AZURE_STORAGE_KEY is not set")

    conf_key = f"fs.azure.account.key.{storage_account}.blob.core.windows.net"

    spark.sparkContext._jsc.hadoopConfiguration().set(conf_key, storage_key)

