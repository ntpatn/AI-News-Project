import io
import csv
import psycopg2
import logging
from psycopg2 import errors, sql
from uuid import uuid4
from time import time
from typing import Iterator

logger = logging.getLogger(__name__)


class PostgresUpsertLoader:
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

    # ... (existing methods: _build_dsn, _buffer, _validation) ...

    def _build_dsn(self) -> str:
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

    def _buffer(self, csv_text: str):
        try:
            text = csv_text.lstrip("\ufeff")
            csv_buffer = io.StringIO(text)
            line0 = csv_buffer.readline().rstrip("\n\r")
            header = [h.strip() for h in line0.split(self.delimiter)] if line0 else []
            if not header or len(header) != len(set(h.lower() for h in header)):
                raise ValueError("Invalid CSV header")
            csv_buffer.seek(0)
            dsn = self._build_dsn()
            return csv_buffer, header, dsn
        except Exception as e:
            raise ValueError(f"Failed to process CSV: {e}") from e

    def _validation(self, cur, header: list[str]) -> tuple[list[str], list[str]]:
        try:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (self.schema, self.table),
            )
            rows = cur.fetchall()
            if not rows:
                raise ValueError(f"Table {self.schema}.{self.table} not found")

            database_table_cols = [r[0] for r in rows]
            missing = [c for c in self.conflict_columns if c not in database_table_cols]
            if missing:
                raise ValueError(f"Conflict columns not in table: {missing}")

            tbl_map = {c.lower(): c for c in database_table_cols}
            csv_cols = []
            seen = set()
            for h in header:
                key = h.lower()
                if key in tbl_map:
                    real = tbl_map[key]
                    if real not in seen:
                        csv_cols.append(real)
                        seen.add(real)

            if not csv_cols:
                raise ValueError("CSV columns don't match table")

            missing_in_csv = [c for c in self.conflict_columns if c not in csv_cols]
            if missing_in_csv:
                raise ValueError(f"Conflict columns missing in CSV: {missing_in_csv}")

            return csv_cols, database_table_cols
        except Exception as e:
            raise ValueError(f"Validation error: {e}") from e

    def _chunk_csv(
        self, csv_buffer: io.StringIO, csv_cols: list[str], chunk_size: int
    ) -> Iterator[io.StringIO]:
        """
        แบ่ง CSV เป็น chunks ตาม batch_size
        Yields: StringIO buffer สำหรับแต่ละ chunk
        """
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

            # สร้าง CSV buffer สำหรับ chunk นี้
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

    def _upsert_condition(self, staging, database_table_cols, csv_cols):
        """Build UPSERT SQL components"""
        try:
            db_cols_set = set(database_table_cols)
            has_createdate = "createdate" in db_cols_set
            has_usercreate = "usercreate" in db_cols_set
            has_updatedate = "updatedate" in db_cols_set
            has_userupdate = "userupdate" in db_cols_set
            extras = ("createdate", "usercreate", "updatedate", "userupdate")

            target_cols = list(csv_cols) + [
                e for e in extras if e in db_cols_set and e not in csv_cols
            ]

            now_expr = sql.SQL("timezone('UTC', now())")
            select_exprs = []
            for c in target_cols:
                if c == "createdate" and has_createdate:
                    select_exprs.append(now_expr)
                elif c == "usercreate" and has_usercreate:
                    select_exprs.append(sql.Literal("system"))
                elif c in ("updatedate", "userupdate"):
                    select_exprs.append(sql.SQL("NULL"))
                elif c in csv_cols:
                    select_exprs.append(
                        sql.SQL("{stg}.{c}").format(
                            stg=sql.Identifier(staging), c=sql.Identifier(c)
                        )
                    )
                else:
                    select_exprs.append(sql.SQL("NULL"))

            update_cols = [
                c
                for c in csv_cols
                if c not in self.conflict_columns and c not in extras
            ]

            do_update_sets = [
                sql.SQL("{c}=EXCLUDED.{c}").format(c=sql.Identifier(c))
                for c in update_cols
            ]
            if has_updatedate:
                do_update_sets.append(
                    sql.SQL("updatedate=").join([sql.SQL(""), now_expr])
                )
            if has_userupdate:
                do_update_sets.append(
                    sql.SQL("userupdate=").join([sql.SQL(""), sql.Literal("system")])
                )

            change_predicates = [
                sql.SQL("{tbl}.{c} IS DISTINCT FROM EXCLUDED.{c}").format(
                    tbl=sql.Identifier(self.table), c=sql.Identifier(c)
                )
                for c in update_cols
            ]
            update_where = (
                sql.SQL(" OR ").join(change_predicates)
                if change_predicates
                else sql.SQL("TRUE")
            )

            return target_cols, select_exprs, do_update_sets, update_where
        except Exception as e:
            raise ValueError(f"UPSERT build failed: {e}") from e

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
        """
        ประมวลผล 1 batch
        Returns: (rows_staged, rows_upserted)
        """
        # COPY → staging
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

        # Count staged rows
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

        # Build UPSERT
        target_cols, select_exprs, do_update_sets, update_where = (
            self._upsert_condition(staging, database_table_cols, csv_cols)
        )

        # Execute UPSERT
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

        # Clear staging for next batch
        cur.execute(
            sql.SQL("TRUNCATE {stg}")
            .format(stg=sql.Identifier(staging))
            .as_string(conn)
        )

        logger.info(f"Batch {batch_num}: Upserted {rows_upserted} rows")
        return rows_staged, rows_upserted

    def loader(self, csv_text: str):
        """Main loader with batch processing"""
        start_time = time()
        csv_buffer, header, dsn = self._buffer(csv_text)

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
                    csv_cols, database_table_cols = self._validation(cur, header)
                    logger.info(f"Matched columns: {csv_cols}")

                    # Set timeouts
                    cur.execute("SET LOCAL statement_timeout = '300s'")
                    cur.execute("SET LOCAL lock_timeout = '15s'")

                    # Create temp staging table (reusable across batches)
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

                    # Process each batch
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

                            # Commit each batch (ถ้าต้องการ transactional ให้เอาออก)
                            conn.commit()

                        except Exception as e:
                            logger.error(f"Batch {batch_num} failed: {e}")
                            conn.rollback()
                            raise

                # Final commit (ถ้าไม่ commit per batch)
                # conn.commit()

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
            logger.error(f"Unique constraint violation : {e}")
            raise RuntimeError("Unique constraint violated") from e
        except Exception as e:
            logger.exception(f"Load failed : {e}")
            raise
