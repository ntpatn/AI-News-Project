from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
from src.function.utils.pyspark.spark_utils import get_spark_session
from src.function.utils.yaml.yaml_utils import read_yaml
from src.registry import extractor, load_plugins
from pyspark.sql.utils import AnalysisException
import time
import logging
from datetime import timedelta

# DAG Config
CONFIG_PATH = "dags/silver/config/spark_config.yaml"
CONFIG_EXTRACT = "dags/silver/config/source_news_articles_currentsapi.yaml"
log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
}


@dag(
    dag_id="silver_extract_with_spark_strict",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["silver", "extract", "spark"],
)
def silver_extract_with_spark_strict():
    @task()
    def run_spark_extract(config_path=CONFIG_EXTRACT):
        start_time = time.time()
        log.info("🚀 Starting Spark Extract job for config: %s", config_path)

        try:
            load_plugins("src.etl.silver", "extractors")
            log.info("✅ Known extractors: %s", extractor.keys())
            if not extractor.keys():
                raise AirflowFailException("No extractors found!")

            spark = get_spark_session(CONFIG_PATH)
            cfg = read_yaml(config_path)
            extract_cfg = cfg["extractor"]

            extractor_instance = extractor.create(
                extract_cfg["name"], **extract_cfg["params"]
            )
            df = extractor_instance.extract(spark)
            record_count = df.count()
            log.info("✅ Extracted %d records", record_count)
            df.show(5, truncate=False)
            duration = round(time.time() - start_time, 2)
            log.info("⏱️ Extract duration: %.2fs", duration)
            return {"records": record_count, "duration": duration}

        except KeyError as e:
            log.error("❌ Config error: %s", e)
            raise AirflowFailException(f"Configuration error: {e}")
        except AnalysisException as e:
            log.error("❌ Spark analysis error: %s", e)
            raise AirflowFailException(f"Spark SQL error: {e}")
        except Exception as e:
            log.exception("❌ Unexpected error: %s", e)
            raise AirflowFailException(f"Extraction failed: {e}")
        finally:
            try:
                spark.stop()
                log.info("🧹 SparkSession stopped cleanly.")
            except Exception:
                pass

    # DAG Flow (single-task example)
    run_spark_extract()


silver_extract_with_spark_strict = silver_extract_with_spark_strict()
