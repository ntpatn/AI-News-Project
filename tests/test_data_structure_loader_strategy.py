# tests/etl/bronze/loader/test_data_structure_loader_strategy.py
import io
from unittest.mock import MagicMock, patch
import psycopg2
from psycopg2 import sql

from src.etl.bronze.loader.data_structure_loader_strategy import (
    PostgresUpsertLoader,
)


def test_chunk_csv_splits_batches():
    loader = PostgresUpsertLoader(
        "dsn", "sch", "tbl", ["id"], delimiter=",", batch_size=2
    )
    csv_buffer = io.StringIO("id,name\n1,a\n2,b\n3,c\n")
    chunks = list(loader._chunk_csv(csv_buffer, ["id", "name"], 2))
    assert len(chunks) == 2
    assert chunks[0].getvalue().strip().splitlines() == ["id,name", "1,a", "2,b"]


@patch(
    "src.etl.bronze.loader.data_structure_loader_strategy.build_upsert_condition"
)
def test_process_batch_runs_copy_and_upsert(build_upsert_condition):
    loader = PostgresUpsertLoader("dsn", "sch", "tbl", ["id"])
    build_upsert_condition.return_value = (
        ["id", "name"],
        [sql.SQL("id"), sql.SQL("name")],
        [],
        None,
    )
    cur = MagicMock()
    conn = MagicMock()
    cur.fetchone.return_value = [2]
    cur.rowcount = 2
    staged, upserted = loader._process_batch(
        cur,
        conn,
        "tmp",
        io.StringIO("id;name\n1;a\n2;b\n"),
        ["id", "name"],
        ["id", "name"],
        1,
    )
    cur.copy_expert.assert_called_once()
    cur.execute.assert_any_call("TRUNCATE tmp")
    assert (staged, upserted) == (2, 2)


def test_loader_happy_path(monkeypatch):
    loader = PostgresUpsertLoader("dsn", "sch", "tbl", ["id"], batch_size=1)
    monkeypatch.setattr(
        "src.etl.bronze.loader.data_structure_loader_strategy.prepare_csv_buffer",
        lambda text, delim: (io.StringIO("id;name\n1;a\n"), ["id", "name"]),
    )
    monkeypatch.setattr(
        "src.etl.bronze.loader.data_structure_loader_strategy.build_postgresql_dsn",
        lambda dsn: "postgresql://fake",
    )
    monkeypatch.setattr(
        "src.etl.bronze.loader.data_structure_loader_strategy.validate_columns",
        lambda cur, sch, tbl, header, conflict: (["id", "name"], ["id", "name"]),
    )
    monkeypatch.setattr(
        "src.etl.bronze.loader.data_structure_loader_strategy.build_upsert_condition",
        lambda *args, **kwargs: (
            ["id", "name"],
            [sql.SQL("id"), sql.SQL("name")],
            [],
            None,
        ),
    )

    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchone.return_value = [1]
    cursor.rowcount = 1

    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor

    monkeypatch.setattr("psycopg2.connect", lambda dsn: conn)

    result = loader.loader("id;name\n1;a\n")
    assert result["status"] == "success"
    assert result["rows_staged"] == 1
