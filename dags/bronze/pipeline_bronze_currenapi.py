from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.exceptions import AirflowException
import pandas as pd
import os

currentsapi_api_key = os.environ.get("CURRENTS_API_KEY")
if not currentsapi_api_key:
    raise AirflowException("CURRENTS_API_KEY environment variable is not set.")
LANGUAGE = "en"


@task()
def extract_get_currentsapi_bronze_pipeline():
    try:
        from src.etl.bronze.extract.data_structure_extract_strategy import DataExtractor

        config = {
            "type": "api_url",
            "path": "https://api.currentsapi.services/v1/latest-news",
            "params": {"language": LANGUAGE, "apiKey": currentsapi_api_key},
        }
        data = DataExtractor.get_extractor(config).extractor()
        return data
    except Exception as e:
        print(f"Failed to extract data from CurrentsAPI: {e}")
        raise AirflowException(
            "Force extract_get_currentsapi_bronze_pipeline task to fail"
        )


@task()
def transform_get_currentsapi_bronze_pipeline(data):
    try:
        from src.etl.bronze.transform.data_format_strategy import (
            FromJsonToDataFrameFormatter,
            FromDataFrameToCsvFormatter,
            DataFormatter,
        )
        from src.etl.bronze.transform.data_metadata_strategy import MetadataAppender
        from airflow.operators.python import get_current_context

        ctx = get_current_context()

        df = DataFormatter(
            FromJsonToDataFrameFormatter(array_keys=["news"])
        ).formatting(data)
        # df = df.explode("category", ignore_index=True)
        metadata = {
            "createdate": pd.Timestamp.now(tz="UTC").to_pydatetime(),
            "usercreate": "system",
            "updatedate": pd.NaT,
            "userupdate": pd.NaT,
            "activedata": True,
            "batch_id": ctx["run_id"],
            "source_system": "currentsapi",
            "layer": "bronze",
            "language": LANGUAGE,
        }
        meta_appender = MetadataAppender(metadata)
        df = meta_appender.meta(df)
        csv = DataFormatter(
            FromDataFrameToCsvFormatter(index=False, encoding="utf-8-sig", sep=";")
        ).formatting(df)
        return csv
    except Exception as e:
        print(f"Failed to transform CurrentsAPI data: {e}")
        raise AirflowException(
            "Force transform_get_currentsapi_bronze_pipeline task to fail"
        )


@task()
def load_get_currentsapi_bronze_pipeline(data):
    try:
        from src.etl.bronze.load.data_structure_loader_strategy import (
            PostgresUpsertLoader,
        )

        loader = PostgresUpsertLoader(
            dsn="postgres_localhost_5433",
            schema="bronze",
            table="source_news_articles_currentsapi",
            conflict_columns=["id", "url"],
            delimiter=";",
        )
        loader.loader(data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Failed to load CurrentsAPI data to database: {e}")
        raise AirflowException(
            "Force load_get_currentsapi_bronze_pipeline task to fail"
        )


with DAG(
    dag_id="pipeline_bronze_currentsapi",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=["bronze", "currentsapi"],
) as dag:
    with TaskGroup(
        "pipeline_bronze_currentsapi", tooltip="Extract, Transform and Load"
    ) as etl_group:
        data_extract = extract_get_currentsapi_bronze_pipeline()
        data_transform = transform_get_currentsapi_bronze_pipeline(data_extract)
        data_load = load_get_currentsapi_bronze_pipeline(data_transform)

        data_extract >> data_transform >> data_load

    etl_group
