from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
from airflow.exceptions import AirflowException, AirflowSkipException
import pandas as pd

LANGUAGE = "en"
COUNTRY = "us"
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
        print(f"[extract] category={category}")
        from src.etl.bronze.extract.data_structure_extract_strategy import DataExtractor

        config = {
            "type": "api_url",
            "path": "https://newsapi.org/v2/top-headlines",
            "params": {
                "country": COUNTRY,
                "pageSize": 1,
                "apiKey": api_key,
                "category": category,
                "language": LANGUAGE,
            },
        }
        data = DataExtractor.get_extractor(config).extractor()
        for item in data.get("articles", []):
            item["category"] = category
        return {"category": category, "data": data}
    except Exception as e:
        print(f"Failed to extract data from NEWSAPI: {e}")
        raise AirflowException("Force extract_get_newsapi_bronze_pipeline task to fail")


@task()
def transform_get_newsapi_bronze_pipeline(batch: dict):
    try:
        from src.etl.bronze.transform.data_format_strategy import (
            FromJsonToDataFrameFormatter,
            FromDataFrameToCsvFormatter,
            DataFormatter,
        )
        from src.etl.bronze.transform.data_metadata_strategy import MetadataAppender
        from airflow.operators.python import get_current_context
        from src.etl.bronze.transform.data_transform_strategy import (
            RenameColumnsTransform,
        )

        category = batch["category"]
        print(f"[transform] category={category}")
        data = batch["data"]
        ctx = get_current_context()
        df = DataFormatter(
            FromJsonToDataFrameFormatter(array_keys=["articles"])
        ).formatting(data)
        print(df.columns)
        metadata = {
            "createdate": pd.Timestamp.now(tz="UTC").to_pydatetime(),
            "usercreate": "system",
            "updatedate": pd.NaT,
            "userupdate": pd.NaT,
            "activedata": True,
            "batch_id": ctx["run_id"],
            "source_system": "newsapi",
            "layer": "bronze",
            "country": COUNTRY,
            "language": LANGUAGE,
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
        return {"category": category, "csv": csv}
    except Exception as e:
        print(f"Failed to transform NEWSAPI data: {e}")
        raise AirflowException(
            "Force transform_get_newsapi_bronze_pipeline task to fail"
        )


@task()
def transform_add_sf_running_newsapi_bronze_pipeline(batch: dict):
    try:
        from src.etl.bronze.transform.data_format_strategy import (
            FromDataFrameToCsvFormatter,
            DataFormatter,
        )
        from src.function.index.sf_generator import SnowflakeGenerator
        from airflow.hooks.base import BaseHook
        from src.etl.bronze.load.utils.csv_buffer import prepare_csv_buffer

        category = batch["category"]
        print(f"[transform] category={category}")
        data = batch["csv"]
        conn = BaseHook.get_connection("postgres_localhost_5433")
        dsn = f"dbname={conn.schema} user={conn.login} password={conn.password} host={conn.host} port={conn.port}"
        sf = SnowflakeGenerator(dsn=dsn, source_name="bronze.newsapi", version_no=1)
        csv_buffer, _ = prepare_csv_buffer(data)
        df = pd.read_csv(csv_buffer, sep=";", encoding="utf-8-sig")
        df["sf_id"] = sf.bulk_generate_fast(len(df))
        csv = DataFormatter(
            FromDataFrameToCsvFormatter(index=False, encoding="utf-8-sig", sep=";")
        ).formatting(df)
        return {"category": category, "csv": csv}
    except Exception as e:
        print(f"Failed to transform add id_sf in newsapi data: {e}")
        raise AirflowException(
            "Force transform_add_sf_running_newsapi_bronze_pipeline task to fail"
        )


@task()
def load_get_newsapi_bronze_pipeline(batch: dict):
    try:
        from src.etl.bronze.load.data_structure_loader_strategy import (
            PostgresUpsertLoader,
        )
        import io
        import csv

        category = batch["category"]
        csv_text = batch["csv"]
        print(f"[load] category={category}")

        if not csv_text or not csv_text.strip():
            raise AirflowSkipException("No CSV content. Skip loading.")

        # 2) เช็ก header ให้มีคอลัมน์ที่ต้องใช้ (เช่น url) → ถ้าไม่มีให้ข้าม
        reader = csv.reader(io.StringIO(csv_text), delimiter=";")
        header = next(reader, None) or []
        if "url" not in header:
            raise AirflowSkipException(
                "CSV missing required column 'url'. Skip loading."
            )

        loader = PostgresUpsertLoader(
            dsn="postgres_localhost_5433",
            schema="bronze",
            table="source_news_articles_newsapi",
            conflict_columns=["url"],
            delimiter=";",
        )
        loader.loader(csv_text)
        print("Data inserted successfully.")
    except ValueError as e:
        if "Conflict columns missing" in str(e):
            raise AirflowSkipException(str(e))
        raise
    except AirflowSkipException:
        raise
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
        transformed = transform_get_newsapi_bronze_pipeline.expand(batch=extracted)
        running = transform_add_sf_running_newsapi_bronze_pipeline.expand(
            batch=transformed
        )
        loaded = load_get_newsapi_bronze_pipeline.expand(batch=running)

        extracted >> transformed >> running >> loaded

    etl_group
