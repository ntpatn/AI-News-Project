from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
from airflow.exceptions import AirflowException

CATEGORIES = [
    "business",
    "entertainment",
    "general",
    "health",
    "science",
    "sports",
    "technology",
]
api_key = os.environ.get("NEWSAPI_API_KEY")
if not api_key:
    raise AirflowException("NEWSAPI_API_KEY environment variable is not set.")


@task()
def extract_get_newsapi_bronze_pipeline(category: str):
    try:
        from src.etl.extract.data_structure_extract_strategy import DataExtractor

        # ดึง key ใน runtime เพื่อไม่ให้ DAG พังตอน parse
        config = {
            "type": "api_url",
            "path": "https://newsapi.org/v2/top-headlines",
            "params": {
                "country": "us",
                "pageSize": 1,
                "apiKey": api_key,
                "category": category,
            },
        }
        data = DataExtractor.get_extractor(config).extractor()
        for item in data.get("articles", []):
            item["category"] = category
        return data
    except Exception as e:
        print(f"Failed to extract data from NEWSAPI: {e}")
        raise AirflowException("Force extract_get_newsapi_bronze_pipeline task to fail")


@task()
def transform_get_newsapi_bronze_pipeline(data):
    try:
        from src.etl.transform.data_format_strategy import (
            FromJsonToDataFrameFormatter,
            FromDataFrameToCsvFormatter,
            DataFormatter,
        )
        from src.etl.transform.data_metadata_strategy import MetadataAppender
        from airflow.operators.python import get_current_context
        from src.etl.transform.data_transform_strategy import RenameColumnsTransform
        import pandas as pd
        from datetime import timezone

        ctx = get_current_context()
        df = DataFormatter(
            FromJsonToDataFrameFormatter(array_keys=["articles"])
        ).formatting(data)
        print(df.columns)
        metadata = {
            "createdate": pd.Timestamp.now(tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "usercreate": "system",
            "updatedate": pd.NaT,
            "userupdate": pd.NaT,
            "activedata": True,
            "batch_id": ctx["run_id"],
            "source_system": "newsapi",
            "layer": "bronze",
        }
        df = MetadataAppender(metadata).meta(df)
        print(df.columns)
        df = RenameColumnsTransform(
            {
                "source.id": "source_id",
                "source.name": "source_name",
                "urlToImage": "url_to_image",
                "publishedAt": "published_at",
            }
        ).transform(df)
        print(df.columns)
        csv = DataFormatter(
            FromDataFrameToCsvFormatter(index=False, encoding="utf-8-sig", sep=";")
        ).formatting(df)
        return csv
    except Exception as e:
        print(f"Failed to transform NEWSAPI data: {e}")
        raise AirflowException(
            "Force transform_get_newsapi_bronze_pipeline task to fail"
        )


@task()
def load_get_newsapi_bronze_pipeline(csv_text: str):
    try:
        from src.etl.load.data_structure_loader_strategy import PostgresUpsertLoader

        loader = PostgresUpsertLoader(
            dsn="postgres_localhost_5433",
            schema="bronze",
            table="source_news_articles_newsapi",
            conflict_columns=["url"],
            delimiter=";",
        )
        loader.loader(csv_text)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Failed to load NEWSAPI data to database: {e}")
        raise AirflowException("Force load_get_newsapi_bronze_pipeline task to fail")


with DAG(
    dag_id="pipeline_bronze_newsapi",
    schedule="0 6 * * *",
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=["bronze", "newsapi"],
) as dag:
    with TaskGroup(
        "pipeline_bronze_newsapi", tooltip="Extract, Transform and Load"
    ) as etl_group:
        extracted = extract_get_newsapi_bronze_pipeline.expand(category=CATEGORIES)
        transformed = transform_get_newsapi_bronze_pipeline.expand(data=extracted)
        loaded = load_get_newsapi_bronze_pipeline.expand(csv_text=transformed)

        extracted >> transformed >> loaded

    etl_group
