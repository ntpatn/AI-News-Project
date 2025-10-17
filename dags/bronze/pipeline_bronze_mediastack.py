from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.exceptions import AirflowException
import pandas as pd
import os

mediastack_api_key = os.environ.get("MEDIASTACK_API_KEY")
if not mediastack_api_key:
    raise AirflowException("MEDIASTACK_API_KEY environment variable is not set.")


@task()
def extract_get_mediastack_bronze_pipeline():
    try:
        from src.etl.bronze.extract.data_structure_extract_strategy import DataExtractor

        config = {
            "type": "api_url",
            "path": "https://api.mediastack.com/v1/news",
            "params": {"limit": 5, "access_key": mediastack_api_key},
        }
        data = DataExtractor.get_extractor(config).extractor()
        return data
    except Exception as e:
        print(f"Failed to extract data from MediaStack: {e}")
        raise AirflowException(
            "Force extract_get_mediastack_bronze_pipeline task to fail"
        )


@task()
def transform_get_mediastack_bronze_pipeline(data):
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
            FromJsonToDataFrameFormatter(array_keys=["data"])
        ).formatting(data)
        # df = df.explode("category", ignore_index=True)
        metadata = {
            "createdate": pd.Timestamp.now(tz="UTC").to_pydatetime(),
            "usercreate": "system",
            "updatedate": pd.NaT,
            "userupdate": pd.NaT,
            "activedata": True,
            "batch_id": ctx["run_id"],
            "source_system": "mediastack",
            "layer": "bronze",
        }
        meta_appender = MetadataAppender(metadata)
        df = meta_appender.meta(df)
        csv = DataFormatter(
            FromDataFrameToCsvFormatter(index=False, encoding="utf-8-sig", sep=";")
        ).formatting(df)
        return csv
    except Exception as e:
        print(f"Failed to transform MediaStack data: {e}")
        raise AirflowException(
            "Force transform_get_mediastack_bronze_pipeline task to fail"
        )


@task()
def load_get_mediastack_bronze_pipeline(data):
    try:
        from src.etl.bronze.load.data_structure_loader_strategy import (
            PostgresUpsertLoader,
        )

        loader = PostgresUpsertLoader(
            dsn="postgres_localhost_5433",
            schema="bronze",
            table="source_news_articles_mediastack",
            conflict_columns=["url"],
            delimiter=";",
        )
        loader.loader(data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Failed to load MediaStack data to database: {e}")
        raise AirflowException("Force load_get_mediastack_bronze_pipeline task to fail")


with DAG(
    dag_id="pipeline_bronze_mediastack",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=["bronze", "mediastack"],
) as dag:
    with TaskGroup(
        "pipeline_bronze_mediastack", tooltip="Extract, Transform and Load"
    ) as etl_group:
        data_extract = extract_get_mediastack_bronze_pipeline()
        data_transform = transform_get_mediastack_bronze_pipeline(data_extract)
        data_load = load_get_mediastack_bronze_pipeline(data_transform)

        data_extract >> data_transform >> data_load

    etl_group
