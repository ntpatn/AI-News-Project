from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
import pandas as pd
import os

currentapi_api_key = os.environ.get("CURRENTS_API_KEY")


@task()
def extract_get_currentsapi_bronze_pipeline():
    try:
        from src.etl.extract.data_structure_extract_strategy import DataExtractor

        config = {
            "type": "api_url",
            "path": "https://api.currentsapi.services/v1/latest-news",
            "params": {"language": "en", "apiKey": currentapi_api_key},
        }
        data = DataExtractor.get_extractor(config).extractor()
        return data
    except Exception as e:
        print(f"Error: {e}")
        raise AirflowException(
            "Force extract_get_currentsapi_bronze_pipeline task to fail"
        )


@task()
def transform_get_currentsapi_bronze_pipeline(data):
    try:
        from src.etl.transform.data_format_strategy import (
            FromJsonToDataFrameFormatter,
            FromDataFrameToCsvFormatter,
            DataFormatter,
        )
        from src.etl.transform.data_metadata_strategy import MetadataAppender
        from airflow.operators.python import get_current_context

        ctx = get_current_context()

        df = DataFormatter(
            FromJsonToDataFrameFormatter(array_keys=["news"])
        ).formatting(data)
        # df = df.explode("category", ignore_index=True)
        metadata = {
            "createdate": pd.Timestamp.now(tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "usercreate": "system",
            "updatedate": pd.NaT,
            "userupdate": "system",
            "activedata": True,
            "batch_id": ctx["run_id"],
            "source": "currentsapi",
            "layer": "bronze",
        }
        meta_appender = MetadataAppender(metadata)
        df = meta_appender.meta(df)
        csv = DataFormatter(
            FromDataFrameToCsvFormatter(index=False, encoding="utf-8-sig", sep=",")
        ).formatting(df)
        return csv
    except Exception as e:
        print(f"Error: {e}")
        raise AirflowException(
            "Force transform_get_currentsapi_bronze_pipeline task to fail"
        )


@task()
def load_get_currentsapi_bronze_pipeline(data):
    try:
        from src.etl.load.data_structure_loader_strategy import PostgresCsvCopyLoader

        loader = PostgresCsvCopyLoader(
            "postgres_localhost_5433",
            "bronze",
            "source_news_articles_currentsapi",
        )
        loader.create(data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error: {e}")
        raise AirflowException(
            "Force load_get_currentsapi_bronze_pipeline task to fail"
        )


with DAG(
    dag_id="pipline_bronze_currenapi",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=["bronze", "currenapi"],
) as dag:
    with TaskGroup(
        "pipline_bronze_currenapi", tooltip="Extract, Transform and Load"
    ) as etl_group:
        data_extract = extract_get_currentsapi_bronze_pipeline()
        data_transform = transform_get_currentsapi_bronze_pipeline(data_extract)
        data_load = load_get_currentsapi_bronze_pipeline(data_transform)

        data_extract >> data_transform >> data_load

    etl_group
