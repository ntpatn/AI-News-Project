# dag_bronze_silver_currentsapi_prod.py
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException, AirflowSkipException
from datetime import datetime, timedelta
import os
import gc
import psutil
import logging
import pandas as pd
import json
from src.etl.bronze.transform.data_format_strategy import (
    FromJsonToDataFrameFormatter,
    DataFormatter,
    CsvToTempFormatter,
    JsonToTempFormatter,
)

# ---------- Default / Observability ----------
log = logging.getLogger(__name__)

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,  # requires smtp configured in Airflow
    "email_on_retry": False,
    "email": ["nithispat@gmail.com"],
}


# ---------- Helper ----------
def debug_memory_and_files(tag: str = "") -> None:
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024**2
    dfs = [o for o in gc.get_objects() if isinstance(o, pd.DataFrame)]
    tmp_files = [f for f in os.listdir("/tmp") if f.startswith("currentsapi_")]
    log.info(
        "🧩 [DEBUG: %s] Memory used: %.2f MB | DataFrames: %d | Tmp files: %d",
        tag,
        mem_mb,
        len(dfs),
        len(tmp_files),
    )


with DAG(
    dag_id="pipeline_bronze_currentsapi_prod",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=["bronze", "silver", "currentsapi", "prod"],
    description="Production-ready pipeline: extract CurrentsAPI -> bronze (add sf_id) -> silver cleaning -> load to Postgres. Uses temp files (no large XCom).",
) as dag:

    @task()
    def extract_get_currentsapi_bronze_pipeline() -> str:
        from src.etl.bronze.extractors.data_structure_extract_strategy import (
            DataExtractor,
        )

        data = None
        debug_memory_and_files("extract:start")
        try:
            currentsapi_api_key = os.environ.get("CURRENTS_API_KEY")
            if not currentsapi_api_key:
                raise AirflowException(
                    "CURRENTS_API_KEY environment variable is not set."
                )

            config = {
                "type": "api_url",
                "path": "https://api.currentsapi.services/v1/latest-news",
                "params": {"language": "en", "apiKey": currentsapi_api_key},
            }
            data = DataExtractor.get_extractor(config).extractor()
            json_path = DataFormatter(
                JsonToTempFormatter(prefix="currentsapi_raw_")
            ).formatting(data)
            log.info("Extracted %s and wrote to %s", "currentsapi", json_path)
            return json_path

        except Exception as e:
            log.exception("Failed to extract data from CurrentsAPI: %s", e)
            raise AirflowException(
                "extract_get_currentsapi_bronze_pipeline failed"
            ) from e

        finally:
            # Keep 'data' for GC safety
            del data
            gc.collect()
            debug_memory_and_files("extract:end")

    # -------------------------
    # TRANSFORM -> JSON -> DataFrame -> add metadata -> CSV (bronze)
    # -------------------------
    @task()
    def transform_get_currentsapi_bronze_pipeline(json_path: str) -> str:
        from src.etl.bronze.transform.data_metadata_strategy import MetadataAppender
        from airflow.operators.python import get_current_context

        df = None
        debug_memory_and_files("transform:start")
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            ctx = get_current_context()
            df = DataFormatter(
                FromJsonToDataFrameFormatter(array_keys=["news"])
            ).formatting(data)

            metadata = {
                "createdate": pd.Timestamp.now(tz="UTC").to_pydatetime(),
                "usercreate": "system",
                "updatedate": pd.NaT,
                "userupdate": pd.NaT,
                "activedata": True,
                "batch_id": ctx["run_id"],
                "source_system": "currentsapi",
                "layer": "bronze",
                "language": "en",
            }
            df = MetadataAppender(metadata).meta(df)

            csv_path = DataFormatter(
                CsvToTempFormatter(prefix="currentsapi_bronze_")
            ).formatting(df)
            log.info(
                "Transformed JSON -> DataFrame(shape=%s) -> wrote CSV %s",
                df.shape,
                csv_path,
            )
            return csv_path

        except Exception as e:
            log.exception("Failed to transform CurrentsAPI data: %s", e)
            raise AirflowException(
                "transform_get_currentsapi_bronze_pipeline failed"
            ) from e

        finally:
            del df
            gc.collect()
            debug_memory_and_files("transform:end")

    # -------------------------
    # ADD SF ID (fast bulk generator) -> produce new CSV path
    # -------------------------
    @task()
    def transform_add_sf_running_currentsapi_bronze_pipeline(csv_path: str) -> str:
        df = None
        debug_memory_and_files("add_sf:start")
        try:
            from src.function.index.sf_generator import SnowflakeGenerator
            from airflow.hooks.base import BaseHook

            # Get DB connection via Airflow Connection id (not hardcoded DSN)
            conn = BaseHook.get_connection("postgres_localhost_5433")
            dsn = f"dbname={conn.schema} user={conn.login} password={conn.password} host={conn.host} port={conn.port}"
            sf = SnowflakeGenerator(
                dsn=dsn, source_name="bronze.currentsapi", version_no=1
            )

            df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")
            if df.empty:
                log.info("No rows found in bronze CSV %s", csv_path)
                raise AirflowSkipException("No data to add sf_id for")

            df["sf_id"] = sf.bulk_generate_fast(len(df))

            out_csv_path = DataFormatter(
                CsvToTempFormatter(prefix="currentsapi_bronze_sf_")
            ).formatting(df)
            log.info("Added sf_id to DF -> wrote %s (%d rows)", out_csv_path, len(df))
            return out_csv_path

        except AirflowSkipException:
            # pass through skip so downstream can handle
            raise

        except Exception as e:
            log.exception("Failed to add sf_id: %s", e)
            raise AirflowException(
                "transform_add_sf_running_currentsapi_bronze_pipeline failed"
            ) from e

        finally:
            del df
            gc.collect()
            debug_memory_and_files("add_sf:end")

    # -------------------------
    # SILVER cleaning
    # -------------------------
    @task()
    def transform_currentsapi_silver_pipeline(csv_path_with_sf: str) -> str:
        df = None
        debug_memory_and_files("silver:start")
        try:
            from src.etl.bronze.transform.data_silver_cleaning_strategy import (
                SilverCleaningCurrentsapi,
            )

            df = pd.read_csv(csv_path_with_sf, sep=";", encoding="utf-8-sig")
            if df.empty:
                log.info("No rows for silver cleaning in %s", csv_path_with_sf)
                raise AirflowSkipException("No data for silver cleaning")

            df = SilverCleaningCurrentsapi().silverCleaning(df)
            df["layer"] = "silver"

            out_csv_path = DataFormatter(
                CsvToTempFormatter(prefix="currentsapi_silver_")
            ).formatting(df)
            log.info(
                "Silver cleaning done -> wrote %s (rows=%d)", out_csv_path, len(df)
            )
            return out_csv_path

        except AirflowSkipException:
            raise

        except Exception as e:
            log.exception("Failed silver cleaning: %s", e)
            raise AirflowException(
                "transform_currentsapi_silver_pipeline failed"
            ) from e

        finally:
            del df
            gc.collect()
            debug_memory_and_files("silver:end")

    @task()
    def load_get_currentsapi_bronze_pipeline(
        csv_path: str, schema: str, table: str, conflict_columns: list
    ) -> str:
        loader = None
        debug_memory_and_files("load:start")
        try:
            from src.etl.bronze.loader.data_structure_loader_strategy import (
                PostgresUpsertLoader,
            )

            loader = PostgresUpsertLoader(
                dsn="postgres_localhost_5433",
                schema=schema,
                table=table,
                conflict_columns=conflict_columns,
                delimiter=";",
            )

            with open(csv_path, "r", encoding="utf-8-sig") as f:
                csv_content = f.read()

            loader.loader(csv_content)
            log.info("Data loaded to %s.%s successfully.", schema, table)
            return "ok"

        except AirflowSkipException:
            raise

        except Exception as e:
            log.exception("Failed to load CurrentsAPI data to DB: %s", e)
            raise AirflowException("load_get_currentsapi_bronze_pipeline failed") from e

        finally:
            del loader
            gc.collect()
            debug_memory_and_files("load:end")

    with TaskGroup("pipeline_bronze_currentsapi") as etl_bronze_group:
        raw_json = extract_get_currentsapi_bronze_pipeline()
        bronze_csv = transform_get_currentsapi_bronze_pipeline(raw_json)
        bronze_csv_with_sf = transform_add_sf_running_currentsapi_bronze_pipeline(
            bronze_csv
        )

        bronze_load = load_get_currentsapi_bronze_pipeline(
            bronze_csv_with_sf,
            "bronze",
            "source_news_articles_currentsapi",
            ["id", "url"],
        )

        raw_json >> bronze_csv >> bronze_csv_with_sf >> bronze_load

    with TaskGroup("pipeline_silver_clean_currentsapi") as tl_silver_group:
        silver_csv = transform_currentsapi_silver_pipeline(bronze_csv_with_sf)

        silver_load = load_get_currentsapi_bronze_pipeline(
            silver_csv,
            "silver",
            "clean_news_articles_currentsapi",
            ["id", "url"],
        )

        silver_csv >> silver_load

    etl_bronze_group >> tl_silver_group
