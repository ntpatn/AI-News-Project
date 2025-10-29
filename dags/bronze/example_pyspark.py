from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
import time
import yaml

from src.etl.silver.registry import EXTRACTOR_REGISTRY, load_extractors


@dag(
    dag_id="spark_extract_bronze_newsapi",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)
def spark_extract_bronze_newsapi():
    @task()
    def extract_with_spark():
        start = time.time()
        print(f"🚀 Starting Spark Session...{start}")
        spark = (
            SparkSession.builder.appName("AI-News-ETL")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.jars", "/opt/spark_drivers/postgresql-42.7.3.jar")
            .config(
                "spark.driver.extraClassPath",
                "/opt/spark_drivers/postgresql-42.7.3.jar",
            )
            .config(
                "spark.executor.extraClassPath",
                "/opt/spark_drivers/postgresql-42.7.3.jar",
            )
            .getOrCreate()
        )

        print("✅ Spark initialized successfully")

        cfg = yaml.safe_load(open("dags/config/source_news_articles_currentsapi.yaml"))

        extract_cfg = cfg["extractor"]
        load_extractors()
        extractor_cls = EXTRACTOR_REGISTRY[extract_cfg["name"]]
        extractor = extractor_cls(**extract_cfg["params"])

        df = extractor.extract(spark)
        df.show(5)

    extract_with_spark()


spark_extract_bronze_newsapi = spark_extract_bronze_newsapi()
