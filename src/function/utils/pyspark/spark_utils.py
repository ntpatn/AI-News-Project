from contextlib import contextmanager
from pyspark.sql import SparkSession
from src.function.utils.yaml.yaml_utils import read_yaml


def get_spark_session(config_path="dags/config/spark_config.yaml") -> SparkSession:
    cfg = read_yaml(config_path)
    spark_cfg = cfg.get("spark", {})

    required_top = ["appName", "master", "config"]
    for key in required_top:
        if key not in spark_cfg:
            raise KeyError(f"Missing required key: spark.{key} in {config_path}")

    app_name = spark_cfg["appName"]
    master = spark_cfg["master"]
    params = spark_cfg["config"]

    required_params = ["driver_memory", "shuffle_partitions", "timezone", "compression"]
    missing = [p for p in required_params if p not in params]
    if missing:
        raise KeyError(
            f"Missing required spark.config key(s): {', '.join(missing)} in {config_path}"
        )

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", params["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(params["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", params["timezone"])
        .config("spark.sql.parquet.compression.codec", params["compression"])
    )

    # optional keys
    if "adaptive_enabled" in params:
        builder = builder.config(
            "spark.sql.adaptive.enabled", str(params["adaptive_enabled"]).lower()
        )
    if "arrow_enabled" in params:
        builder = builder.config(
            "spark.sql.execution.arrow.pyspark.enabled",
            str(params["arrow_enabled"]).lower(),
        )

    for jar in spark_cfg.get("jars", []):
        builder = (
            builder.config("spark.jars", jar)
            .config("spark.driver.extraClassPath", jar)
            .config("spark.executor.extraClassPath", jar)
        )

    spark = builder.getOrCreate()
    print(
        f"✅ Spark started: app='{app_name}', master='{master}' (driver_memory={params['driver_memory']})"
    )
    return spark


@contextmanager
def spark_session(config_path="dags/config/spark_config.yaml"):
    spark = get_spark_session(config_path)
    try:
        yield spark
    finally:
        print("🧹 Stopping Spark session")
        spark.stop()
