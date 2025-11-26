import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select
import os
from pathlib import Path
from io import BytesIO
from minio import Minio
from .base_extractor import BaseStructureExtractor
import logging

log = logging.getLogger(__name__)


class DbExtractor(BaseStructureExtractor):
    def __init__(
        self, conn_str: str, schema: str, table: str = None, query: str = None
    ):
        if table is None and query is None:
            raise ValueError(
                "Either 'table' or 'query' must be provided for DbExtractor."
            )

        self.conn_str = conn_str
        self.schema = schema
        self.table = table
        self.query = query
        self.engine = None
        self.conn = None

    def __enter__(self):
        self.engine = create_engine(self.conn_str)
        self.conn = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn is not None:
            self.conn.close()
        if self.engine is not None:
            self.engine.dispose()

    def extractor(self) -> pd.DataFrame:
        if self.query:
            return pd.read_sql(self.query, self.conn or self.engine)
        metadata = MetaData()
        tbl = Table(
            self.table,
            metadata,
            autoload_with=self.engine or self.conn,
            schema=self.schema,
        )
        stmt = select(tbl)
        with self.conn or self.engine.connect() as conn:
            result = conn.execute(stmt)

            rows = result.fetchall()
            cols = result.keys()
        return pd.DataFrame(rows, columns=cols)


class CsvExtractor(BaseStructureExtractor):
    def __init__(self, filepath: str):
        self.filepath = filepath

    def extractor(self) -> pd.DataFrame:
        return pd.read_csv(self.filepath)


class SourceBBCLocalExtractor(BaseStructureExtractor):
    def __init__(self, filepath: str):
        project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if filepath is None:
            self.filepath = os.path.join(
                project_path,
                "data",
                "raw",
                "source_pariza_sharif_BBC_news_summary",
                "BBC News Summary",
            )
        else:
            self.filepath = filepath

    def extractor(self) -> pd.DataFrame:
        data = []
        for root, _, files in os.walk(self.filepath):
            parts = Path(root).parts
            if len(parts) >= 2 and parts[-2] in {"News Articles", "Summaries"}:
                article_type = parts[-2]
                category = parts[-1]
                for filename in files:
                    full_path = os.path.join(root, filename)
                    try:
                        with open(full_path, "r", encoding="utf-8") as f:
                            content = f.read()
                    except Exception as e:
                        print(f"Warning: Could not read file {full_path}. Error: {e}")
                        content = ""
                    data.append(
                        {
                            "filename": filename,
                            "category": category,
                            "article_or_summary": article_type,
                            "text_content": content,
                        }
                    )
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df = df.pivot_table(
            index=["filename", "category"],
            columns="article_or_summary",
            values="text_content",
            aggfunc="first",
        ).reset_index()
        df.columns.name = None
        for col in ["News Articles", "Summaries"]:
            if col not in df.columns:
                df[col] = None
        return df[["filename", "category", "News Articles", "Summaries"]]


class DvcExtractor(BaseStructureExtractor):
    def __init__(
        self,
        path: str,
        repo: str = ".",
        remote: str = "storage",
        encoding: str = "utf-8",
        rev: str = None,
    ):
        self.path = path
        self.repo = repo
        self.remote = remote
        self.encoding = encoding
        self.rev = rev

    def extractor(self) -> pd.DataFrame:
        import dvc.api

        ext = Path(self.path).suffix.lower()
        mode = "rb" if ext == ".parquet" else "r"

        with dvc.api.open(
            self.path,
            repo=self.repo,
            remote=self.remote,
            mode=mode,
            encoding=None if ext == ".parquet" else self.encoding,
            rev=self.rev,
        ) as f:
            if ext == ".parquet":
                return pd.read_parquet(f)
            elif ext == ".csv":
                return pd.read_csv(f)
            elif ext == ".json":
                return pd.read_json(f)
            else:
                raise ValueError(f"Unsupported file extension: {ext}")


class MinioExtractor(BaseStructureExtractor):
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        object_name: str,
        secure: bool = False,
        encoding: str = "utf-8",
        version_id: str | None = None,
        region: str | None = None,
    ):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
        )
        self.bucket = bucket
        self.object_name = object_name
        self.encoding = encoding
        self.version_id = version_id

    def extractor(self) -> pd.DataFrame:
        ext = Path(self.object_name).suffix.lower()
        resp = self.client.get_object(
            self.bucket, self.object_name, version_id=self.version_id
        )
        try:
            data = resp.read()
        finally:
            try:
                resp.close()
                resp.release_conn()
            except Exception:
                pass

        if ext == ".parquet":
            return pd.read_parquet(BytesIO(data))  # ต้องมี pyarrow
        elif ext == ".csv":
            return pd.read_csv(BytesIO(data), encoding=self.encoding)
        elif ext == ".json":
            return pd.json_normalize(BytesIO(data))
        else:
            raise ValueError(f"Unsupported file extension for MinIO: {ext}")


class GetApiJsonUrlExtractor(BaseStructureExtractor):
    def __init__(
        self, url: str, params: dict = None, max_retry=5, base_delay=5, timeout=30
    ):
        self.url = url
        self.params = params
        self.max_retry = max_retry
        self.base_delay = base_delay
        self.timeout = timeout

    def extractor(self):
        import requests
        import time
        import random

        for attempt in range(1, self.max_retry + 1):
            try:
                log.info(f"[API] attempt #{attempt}")
                res = requests.get(self.url, params=self.params, timeout=self.timeout)
                res.raise_for_status()

                if "application/json" in res.headers.get("Content-Type", ""):
                    return res.json()

                raise ValueError("Response not JSON")

            except Exception as e:
                if attempt == self.max_retry:
                    log.error("API retry maxed out — fail")
                    raise

                delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(0, 2)
                log.warning(
                    f"[API] fail on attempt {attempt}: {e} — retry in {delay:.1f}s"
                )
                time.sleep(delay)


class DataExtractor:
    @staticmethod
    def get_extractor(cfg: dict) -> BaseStructureExtractor:
        try:
            if cfg["type"] == "db":
                return DbExtractor(
                    cfg["conn_str"], cfg["schema"], cfg["table"], cfg.get("query")
                )
            elif cfg["type"] == "csv":
                return CsvExtractor(cfg["path"])
            elif cfg["type"] == "api_url":
                return GetApiJsonUrlExtractor(
                    url=cfg["path"],
                    params=cfg.get("params"),
                    max_retry=cfg.get("max_retry", 3),
                    base_delay=cfg.get("base_delay", 5),
                    timeout=cfg.get("timeout", 30),
                )
            elif cfg["type"] == "local_bbc":
                return SourceBBCLocalExtractor(cfg["path"])
            elif cfg["type"] == "dvc":
                return DvcExtractor(
                    cfg["path"], cfg["repo"], cfg["remote"], cfg["encoding"], cfg["rev"]
                )
            elif cfg["type"].lower() == "minio":
                return MinioExtractor(
                    endpoint=cfg["endpoint"],
                    access_key=cfg["access_key"],
                    secret_key=cfg["secret_key"],
                    bucket=cfg["bucket"],
                    object_name=cfg["object"],
                    secure=cfg.get("secure", False),
                    encoding=cfg.get("encoding", "utf-8"),
                    version_id=cfg.get("version_id"),
                    region=cfg.get("region"),
                )
            else:
                raise ValueError(f"Unsupported Extractor type: {cfg['type']}")
        except KeyError as e:
            raise ValueError(f"Configuration is missing required key: {e}")
