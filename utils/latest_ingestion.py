def get_latest_run_path(spark, runs_base_path: str) -> str:
    """
    Return the full WASBS path to the latest run_* folder under runs_base_path.

    Assumes run naming like: run_YYYYMMDD_HHMMSS_microseconds
    so lexicographic max() gives latest.
    """
    jvm = spark._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()

    base = jvm.org.apache.hadoop.fs.Path(runs_base_path)
    fs = base.getFileSystem(hconf)

    statuses = fs.listStatus(base)

    run_names = []
    for s in statuses:
        if s.isDirectory():
            name = s.getPath().getName()
            if name.startswith("run_"):
                run_names.append(name)

    if not run_names:
        raise ValueError(f"No run_* directories found under: {runs_base_path}")

    latest = max(run_names)
    return f"{runs_base_path.rstrip('/')}/{latest}"
