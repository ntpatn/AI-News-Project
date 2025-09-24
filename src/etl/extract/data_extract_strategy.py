from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy import create_engine
import os
from pathlib import Path
from io import BytesIO
from minio import Minio

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BaseExtractor(ABC):
    @abstractmethod
    def extractor(self) -> pd.DataFrame:
        pass


class DbExtractor(BaseExtractor):
    def __init__(self, conn_str: str, table: str = None, query: str = None):
        if table is None and query is None:
            raise ValueError(
                "Either 'table' or 'query' must be provided for DbExtractor."
            )
        self.conn_str = conn_str
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
        if self.conn is None:
            self.engine = create_engine(self.conn_str)
            with self.engine.connect() as conn:
                if self.query:
                    return pd.read_sql(self.query, conn)
                else:
                    return pd.read_sql_table(self.table, conn)
        else:
            if self.query:
                return pd.read_sql(self.query, self.conn)
            else:
                return pd.read_sql_table(self.table, self.conn)


class CsvExtractor(BaseExtractor):
    def __init__(self, filepath: str):
        self.filepath = filepath

    def extractor(self) -> pd.DataFrame:
        return pd.read_csv(self.filepath)


class SourceBBCLocalExtractor(BaseExtractor):
    def __init__(self, filepath: str):
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


class DvcExtractor(BaseExtractor):
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


class MinioExtractor(BaseExtractor):
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

    # ถ้าคุณอยากเก็บเมธอด load() ไว้ใช้ภายในก็ได้
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
            # ถ้าเป็น JSON Lines ใช้ lines=True ตามไฟล์จริงของคุณ
            return pd.read_json(BytesIO(data))
        else:
            raise ValueError(f"Unsupported file extension for MinIO: {ext}")


class DataExtractor:
    @staticmethod
    def get_extractor(cfg: dict) -> BaseExtractor:
        try:
            if cfg["type"] == "db":
                return DbExtractor(cfg["conn_str"], cfg["table"], cfg.get("query"))
            elif cfg["type"] == "csv":
                return CsvExtractor(cfg["path"])
            elif cfg["type"] == "local_bbc":
                return SourceBBCLocalExtractor(cfg["path"])
            elif cfg["type"] == "dvc":
                return DvcExtractor(
                    cfg["path"], cfg["repo"], cfg["remote"], cfg["encoding"], cfg["rev"]
                )
            elif cfg["type"].lower() == "minio":  # รองรับ minIO/Minio/minio
                return MinioExtractor(
                    endpoint=cfg["endpoint"],
                    access_key=cfg["access_key"],
                    secret_key=cfg["secret_key"],
                    bucket=cfg["bucket"],
                    object_name=cfg["object"],  # << คีย์นี้ต้องมีใน cfg
                    secure=cfg.get("secure", False),
                    encoding=cfg.get("encoding", "utf-8"),
                    version_id=cfg.get("version_id"),
                    region=cfg.get("region"),
                )
            else:
                raise ValueError(f"Unsupported Extractor type: {cfg['type']}")
        except KeyError as e:
            raise ValueError(f"Configuration is missing required key: {e}")
