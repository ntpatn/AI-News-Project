# src/function/utils/debug/debug_memory.py

import os
import gc
import glob
import psutil
import pandas as pd


def debug_memory_and_files_pandas(
    tag: str = "", pattern: str = "/tmp/*", sample_limit: int = 2
):
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024**2
    dfs = [o for o in gc.get_objects() if isinstance(o, pd.DataFrame)]
    tmp_files = glob.glob(pattern)

    print(f"\n🧩 [DEBUG PANDAS: {tag}]")
    print(f"   • Memory used: {mem_mb:.2f} MB")
    print(f"   • DataFrames alive: {len(dfs)}")
    for i, df in enumerate(dfs[:sample_limit]):
        print(f"     #{i + 1} shape={df.shape}, cols={len(df.columns)}")

    print(f"   • Temp files: {len(tmp_files)}")
    for f in tmp_files[:sample_limit]:
        print(f"     - {f} ({os.path.getsize(f) / 1024:.1f} KB)")
    print("---------------------------------------------------\n")


def debug_memory_and_files_spark(tag: str = "", spark=None, pattern: str = "/tmp/*"):
    if spark is None:
        print(f"[WARN] SparkSession not provided for {tag}")
        return

    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024**2
    tmp_files = glob.glob(pattern)

    print(f"\n🔥 [DEBUG SPARK: {tag}]")
    try:
        sc = spark.sparkContext
        app_id = sc.applicationId
        master = sc.master
        executors = len(sc._jsc.sc().getExecutorMemoryStatus().keys())

        print(f"   • Spark app id: {app_id}")
        print(f"   • Master: {master}")
        print(f"   • Executors: {executors}")
        print(f"   • Driver memory: {mem_mb:.2f} MB")

        # (Optional) รายชื่อ temporary views ที่มีอยู่
        views = [r.name for r in spark.catalog.listTables("default")]
        print(
            f"   • Temp views: {len(views)} -> {', '.join(views[:3]) if views else '-'}"
        )

    except Exception as e:
        print(f"   ⚠️ Spark info unavailable: {e}")

    print(f"   • Temp files: {len(tmp_files)}")
    for f in tmp_files[:2]:
        print(f"     - {f} ({os.path.getsize(f) / 1024:.1f} KB)")
    print("---------------------------------------------------\n")


