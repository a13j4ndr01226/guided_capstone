from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv("config/.env")
acct = os.environ["AZURE_STORAGE_ACCOUNT"]
key = os.environ["AZURE_STORAGE_KEY"]

spark = (
    SparkSession.builder
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6")
    .getOrCreate()
)

spark.conf.set(f"fs.azure.account.key.{acct}.blob.core.windows.net", key)

df = spark.read.parquet("wasbs://.../run_<timestamp>/partition=T")
df.printSchema()
df.show(5, truncate=False)
