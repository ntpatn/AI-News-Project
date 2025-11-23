from urllib.parse import quote_plus
from airflow.hooks.base import BaseHook


def build_postgresql_dsn(conn_id: str) -> str:
    """Resolve PostgreSQL DSN from Airflow connection"""
    try:
        c = BaseHook.get_connection(conn_id)
        user = quote_plus(c.login or "")
        pwd = quote_plus(c.password or "")
        host = c.host or ""
        port = c.port or ""
        db = c.schema or ""
        return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    except Exception as e:
        raise ValueError(f"Failed to build DSN: {e}") from e
