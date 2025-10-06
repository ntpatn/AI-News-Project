from __future__ import annotations
import io
import pandas as pd
import psycopg2
from .base_loader import BaseStructureLoader


class PostgresCsvCopyLoader(BaseStructureLoader):
    def __init__(self, dsn: str, schema: str, table: str):
        self.dsn = dsn
        self.schema = schema
        self.table = table

    def create(self, csv_text):
        from airflow.hooks.base import BaseHook

        csv_buffer = io.StringIO(csv_text)
        conn = BaseHook.get_connection(self.dsn)
        dsn = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                copy_sql = f"""
                    COPY {self.schema}.{self.table}
                    FROM STDIN
                    WITH (FORMAT csv, HEADER true)
                """
                cur.copy_expert(copy_sql, csv_buffer)

        return {"status": "success", "table": f"{self.schema}.{self.table}"}

    def update(self, df: pd.DataFrame, **kwargs):
        pass
