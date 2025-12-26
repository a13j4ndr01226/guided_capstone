from typing import List

# def probe_textfile(spark, input_path: str, n: int = 5) -> List[str]:
#     """
#     Smoke test: can Spark read the files at input_path?
#     Returns the first n lines (or fewer).
#     """
#     rdd = spark.sparkContext.textFile(input_path)
#     sample = rdd.take(n)  # triggers the read
#     return sample

# utils/read_probe.py

def probe_textfile(spark, path: str, n: int = 5):
    # Just take lines directly; still uses Python, but minimal.
    # If this still triggers python workers in your setup, use the df approach below.
    return spark.sparkContext.textFile(path).take(n)

# # utils/read_probe.py

# def probe_textfile(spark, path: str, n: int = 5):
#     df = spark.read.text(path)   # JVM reader
#     return [row["value"] for row in df.limit(n).collect()]
