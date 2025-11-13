import io
import csv
import psycopg2
import logging
from psycopg2 import errors, sql
from uuid import uuid4
from time import time
from typing import Iterator

from function.utils.csv_loader.csv_buffer import prepare_csv_buffer
from function.utils.csv_loader.validation import validate_columns
from function.utils.csv_loader.sql_condition.upsert_builder import build_upsert_condition

logger = logging.getLogger(__name__)


class PostgresUpsertLoader:
    """Batch UPSERT loader for PostgreSQL (COPY + ON CONFLICT)"""

    def __init__(
        self,
        dsn: str,
        schema: str,
        table: str,
        conflict_columns: list[str],
        *,
        delimiter: str = ";",
        batch_size: int = 50000,
    ):
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise ValueError("delimiter must be a single character")

        self.dsn = dsn
        self.schema = schema
        self.table = table
        self.conflict_columns = conflict_columns
        self.delimiter = delimiter
        self.batch_size = batch_size

    # -------------------------------------------------------------------------
    # CONNECTION
    # -------------------------------------------------------------------------
    def _build_dsn(self) -> str:
        """Resolve PostgreSQL DSN from Airflow connection"""
        from urllib.parse import quote_plus
        from airflow.hooks.base import BaseHook

        try:
            c = BaseHook.get_connection(self.dsn)
            user = quote_plus(c.login or "")
            pwd = quote_plus(c.password or "")
            host = c.host or ""
            port = c.port or ""
            db = c.schema or ""
            return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
        except Exception as e:
            raise ValueError(f"Failed to build DSN: {e}") from e

    # -------------------------------------------------------------------------
    # CSV CHUNKER
    # -------------------------------------------------------------------------
    def _chunk_csv(
        self, csv_buffer: io.StringIO, csv_cols: list[str], chunk_size: int
    ) -> Iterator[io.StringIO]:
        """Yield chunked CSV buffers for COPY"""
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer, delimiter=self.delimiter)

        while True:
            chunk = []
            for _ in range(chunk_size):
                try:
                    row = next(reader)
                    chunk.append(row)
                except StopIteration:
                    break

            if not chunk:
                break

            chunk_buffer = io.StringIO()
            writer = csv.DictWriter(
                chunk_buffer,
                fieldnames=csv_cols,
                delimiter=self.delimiter,
                lineterminator="\n",
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()
            for row in chunk:
                writer.writerow({c: row.get(c, "") for c in csv_cols})

            chunk_buffer.seek(0)
            yield chunk_buffer

    # -------------------------------------------------------------------------
    # PROCESS BATCH
    # -------------------------------------------------------------------------
    def _process_batch(
        self,
        cur,
        conn,
        staging: str,
        chunk_buffer: io.StringIO,
        csv_cols: list[str],
        database_table_cols: list[str],
        batch_num: int,
    ) -> tuple[int, int]:
        """COPY to staging + UPSERT into target"""
        copy_stmt = sql.SQL("""
            COPY {stg} ({cols})
            FROM STDIN WITH (
                FORMAT csv,
                HEADER true,
                DELIMITER {delim},
                QUOTE '"',
                NULL ''
            )
        """).format(
            stg=sql.Identifier(staging),
            cols=sql.SQL(", ").join(map(sql.Identifier, csv_cols)),
            delim=sql.Literal(self.delimiter),
        )

        cur.copy_expert(copy_stmt.as_string(conn), chunk_buffer)

        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {stg}")
            .format(stg=sql.Identifier(staging))
            .as_string(conn)
        )
        rows_staged = cur.fetchone()[0]

        if rows_staged == 0:
            logger.warning(f"Batch {batch_num}: No rows staged")
            return 0, 0

        logger.info(f"Batch {batch_num}: Staged {rows_staged} rows")

        target_cols, select_exprs, do_update_sets, update_where = (
            build_upsert_condition(
                self.schema,
                self.table,
                staging,
                database_table_cols,
                csv_cols,
                self.conflict_columns,
            )
        )

        if not do_update_sets:
            upsert_sql = sql.SQL("""
                INSERT INTO {sch}.{tbl} ({cols})
                SELECT {selects} FROM {stg}
                ON CONFLICT ({conflict}) DO NOTHING
            """).format(
                sch=sql.Identifier(self.schema),
                tbl=sql.Identifier(self.table),
                cols=sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                selects=sql.SQL(", ").join(select_exprs),
                stg=sql.Identifier(staging),
                conflict=sql.SQL(", ").join(map(sql.Identifier, self.conflict_columns)),
            )
        else:
            upsert_sql = sql.SQL("""
                INSERT INTO {sch}.{tbl} ({cols})
                SELECT {selects} FROM {stg}
                ON CONFLICT ({conflict})
                DO UPDATE SET {set_clause}
                WHERE {update_where}
            """).format(
                sch=sql.Identifier(self.schema),
                tbl=sql.Identifier(self.table),
                cols=sql.SQL(", ").join(map(sql.Identifier, target_cols)),
                selects=sql.SQL(", ").join(select_exprs),
                stg=sql.Identifier(staging),
                conflict=sql.SQL(", ").join(map(sql.Identifier, self.conflict_columns)),
                set_clause=sql.SQL(", ").join(do_update_sets),
                update_where=update_where,
            )

        cur.execute(upsert_sql.as_string(conn))
        rows_upserted = cur.rowcount

        cur.execute(
            sql.SQL("TRUNCATE {stg}")
            .format(stg=sql.Identifier(staging))
            .as_string(conn)
        )

        logger.info(f"Batch {batch_num}: Upserted {rows_upserted} rows")
        return rows_staged, rows_upserted

    # -------------------------------------------------------------------------
    # MAIN LOADER
    # -------------------------------------------------------------------------
    def loader(self, csv_text: str):
        """Main batch loader entrypoint"""
        start_time = time()

        # 1️⃣ Prepare buffer + header + DSN
        csv_buffer, header = prepare_csv_buffer(csv_text, self.delimiter)
        dsn = self._build_dsn()

        logger.info(
            f"Loading to {self.schema}.{self.table} (batch_size={self.batch_size})"
        )

        total_staged = 0
        total_upserted = 0
        batch_count = 0

        try:
            with psycopg2.connect(dsn) as conn:
                conn.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
                )
                conn.autocommit = False

                with conn.cursor() as cur:
                    # 2️⃣ Validate columns
                    csv_cols, database_table_cols = validate_columns(
                        cur,
                        self.schema,
                        self.table,
                        header,
                        self.conflict_columns,
                    )
                    logger.info(f"Matched columns: {csv_cols}")

                    cur.execute("SET LOCAL statement_timeout = '300s'")
                    cur.execute("SET LOCAL lock_timeout = '15s'")

                    # 3️⃣ Create temp staging
                    staging = f"tmp_staging_{uuid4().hex[:12]}"
                    temp_sql = sql.SQL(
                        "CREATE TEMP TABLE {stg} (LIKE {sch}.{tbl} INCLUDING DEFAULTS) ON COMMIT DROP"
                    ).format(
                        stg=sql.Identifier(staging),
                        sch=sql.Identifier(self.schema),
                        tbl=sql.Identifier(self.table),
                    )
                    cur.execute(temp_sql.as_string(conn))
                    logger.info(f"Created temp staging: {staging}")

                    # 4️⃣ Process chunks
                    for batch_num, chunk_buffer in enumerate(
                        self._chunk_csv(csv_buffer, csv_cols, self.batch_size), start=1
                    ):
                        try:
                            rows_staged, rows_upserted = self._process_batch(
                                cur,
                                conn,
                                staging,
                                chunk_buffer,
                                csv_cols,
                                database_table_cols,
                                batch_num,
                            )
                            total_staged += rows_staged
                            total_upserted += rows_upserted
                            batch_count += 1

                            conn.commit()  # commit per batch
                        except Exception as e:
                            logger.error(f"Batch {batch_num} failed: {e}")
                            conn.rollback()
                            raise

            duration = time() - start_time
            logger.info(
                f"Success: {total_upserted}/{total_staged} rows upserted "
                f"in {batch_count} batches ({duration:.2f}s)"
            )

            return {
                "status": "success",
                "table": f"{self.schema}.{self.table}",
                "rows_staged": total_staged,
                "rows_upserted": total_upserted,
                "batches_processed": batch_count,
                "duration_seconds": round(duration, 2),
                "rows_per_second": round(total_staged / duration, 2)
                if duration > 0
                else 0,
            }

        except errors.NotNullViolation as e:
            logger.error(f"NOT NULL violation: {e.diag.column_name}")
            raise RuntimeError(f"NOT NULL constraint: {e.diag.column_name}") from e
        except errors.UniqueViolation as e:
            logger.error(f"Unique constraint violation: {e}")
            raise RuntimeError("Unique constraint violated") from e
        except Exception as e:
            logger.exception(f"Load failed: {e}")
            raise
