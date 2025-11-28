from airflow import DAG
from datetime import datetime
from airflow.decorators import task

import pandas as pd
import re
import os
import logging
from src.function.utils.debug.debug_memory import debug_memory_and_files_pandas
from src.function.utils.debug.debug_finally import finalize_task
from src.etl.bronze.transform.data_format_strategy import (
    CsvToTempFormatter,
    DataFormatter,
)

log = logging.getLogger(__name__)
con = os.environ.get("Connection_String2")

# ======================================================
# 1) EXTRACT FROM SILVER
# ======================================================


@task
def extract_gold_base(schema: str, table: str):
    """Load Silver Combined into Gold Base."""
    from src.etl.bronze.extractors.data_structure_extract_strategy import DataExtractor

    df = None
    try:
        debug_memory_and_files_pandas("gold_extract:start")

        db_config = {
            "type": "db",
            "conn_str": con,
            "table": table,
            "schema": schema,
        }

        with DataExtractor.get_extractor(cfg=db_config) as Extractor:
            df = Extractor.extractor()

        csv_path = DataFormatter(
            CsvToTempFormatter(prefix=f"{table}_gold_extract_")
        ).formatting(df)

        return csv_path

    finally:
        finalize_task("gold_extract:end", df, debug_memory_and_files_pandas)


# ======================================================
# 2) FEATURE ENGINEERING
# ======================================================


@task
def transform_gold_features(csv_path: str):
    from sentence_transformers import SentenceTransformer

    df = None
    try:
        debug_memory_and_files_pandas("gold_transform:start")

        df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")

        def clean_text(t):
            if pd.isna(t):
                return ""
            t = re.sub(r"http\S+", " ", t)
            t = re.sub(r"<.*?>", " ", t)
            t = re.sub(r"[^0-9a-zA-Zก-๙ ]+", " ", t)
            t = re.sub(r"\s+", " ", t)
            return t.strip().lower()

        df["title_clean"] = df["title"].astype(str).apply(clean_text)
        df["description_clean"] = df["description"].astype(str).apply(clean_text)
        df["text_for_embed"] = df["title_clean"] + " " + df["description_clean"]
        value_counts = df["category"].value_counts()
        rare = value_counts[value_counts < 5].index
        df["category"] = df["category"].replace(rare, "general")

        # 2) normalize
        df["category"] = df["category"].astype(str).str.lower().str.strip()

        # 3) fix typos
        df["category"] = df["category"].replace(
            {"poiltics": "politics", "poltics": "politics", "politic": "politics"}
        )

        # 4) if multi-category, pick first
        df["category"] = df["category"].str.split(",").str[0]

        # 5) whitelist only 8 groups
        VALID = [
            "business",
            "entertainment",
            "general",
            "health",
            "science",
            "sports",
            "technology",
            "politics",
        ]

        df["category"] = df["category"].apply(lambda x: x if x in VALID else "general")

        # 6) encode → 0..7
        encode_map = {cat: idx for idx, cat in enumerate(VALID)}
        df["category_encode"] = df["category"].map(encode_map)
        model = SentenceTransformer(
            "sentence-transformers/all-MiniLM-L6-v2", device="cuda"
        )
        emb = model.encode(
            df["text_for_embed"].tolist(), batch_size=32, show_progress_bar=False
        )
        df["embedding"] = emb.tolist()

        # ---- KEEP ONLY NECESSARY COLUMNS ----
        df = df[
            [
                "sf_id",
                "url",
                "source_system",
                "category",
                "title_clean",
                "description_clean",
                "published_at",
                "embedding",
                "category_encode",
            ]
        ]

        out_csv = DataFormatter(CsvToTempFormatter(prefix="gold_features_")).formatting(
            df
        )

        return out_csv

    finally:
        finalize_task("gold_transform:end", df, debug_memory_and_files_pandas)


# ======================================================
# 3) LOAD GOLD TABLE
# ======================================================


@task
def load_gold_features(csv_path: str, schema: str, table: str):
    from src.etl.bronze.loader.data_structure_loader_strategy import (
        PostgresUpsertLoader,
    )

    loader = None

    try:
        debug_memory_and_files_pandas("gold_load:start")

        loader = PostgresUpsertLoader(
            dsn="postgres_localhost_5433",
            schema=schema,
            table=table,
            conflict_columns=["sf_id"],
            delimiter=";",
        )

        with open(csv_path, "r", encoding="utf-8-sig") as f:
            csv_content = f.read()

        loader.loader(csv_content)
        return "OK"

    finally:
        finalize_task("gold_load:end", loader, debug_memory_and_files_pandas)


# ======================================================
# 4) DAG SETUP
# ======================================================

with DAG(
    dag_id="pipeline_gold_features",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    SCHEMA_SILVER = "silver"
    TABLE_SILVER = "silver_combined"

    SCHEMA_GOLD = "gold"
    TABLE_GOLD = "news_features"

    extract = extract_gold_base(SCHEMA_SILVER, TABLE_SILVER)
    transform = transform_gold_features(extract)
    load = load_gold_features(transform, SCHEMA_GOLD, TABLE_GOLD)

    extract >> transform >> load
