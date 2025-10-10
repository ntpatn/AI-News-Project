import io
import psycopg2
from .base_loader import BaseStructureLoader
from psycopg2 import errors


class PostgresUpsertLoader(BaseStructureLoader):
    def __init__(self, dsn: str, schema: str, table: str, conflict_columns: list[str]):
        self.dsn = dsn
        self.schema = schema
        self.table = table
        self.conflict_columns = conflict_columns

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
            raise ValueError(
                f"Failed to build DSN from connection id '{self.dsn}': {e}"
            )

    def _buffer(self, csv_text: str):
        import csv

        try:
            csv_buffer = io.StringIO(csv_text)
            csv_buffer.seek(0)
            reader = csv.reader(csv_buffer)
            header = next(reader, [])
            csv_buffer.seek(0)
            dsn = self._build_dsn()
            return csv_buffer, header, dsn
        except Exception as e:
            raise ValueError(f"Failed to process CSV text: {e}")

    def _validation(self, cur, header: list[str]) -> list[str]:
        try:
            # Fetch destination table columns (ordered by ordinal position)
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
                raise ValueError(
                    f"Destination table {self.schema}.{self.table} not found or has no columns."
                )
            print(f"Fetched {len(rows)} columns from destination table.and {rows}")
            database_table_cols = [r[0] for r in rows]
            print(f"Database table columns: {database_table_cols}")
            # Ensure conflict columns exist in the destination table
            missing_keys = [
                c for c in self.conflict_columns if c not in database_table_cols
            ]
            if missing_keys:
                raise ValueError(
                    f"conflict_columns not in destination columns: {missing_keys}"
                )
            print(f"missing_keys: {missing_keys}")
            # Determine the actual columns to use = intersection(header, table), keeping table order
            csv_cols = [c for c in database_table_cols if c in header]
            if not csv_cols:
                raise ValueError("CSV header does not match any destination columns.")
            print(f"CSV columns to be used: {csv_cols}")
            # Ensure conflict columns also exist in the CSV header (so ON CONFLICT can fire)
            missing_keys_in_csv = [
                c for c in self.conflict_columns if c not in csv_cols
            ]
            if missing_keys_in_csv:
                raise ValueError(
                    f"These conflict key(s) are missing in CSV header: {missing_keys_in_csv}"
                )
            print(f"missing_keys_in_csv: {missing_keys_in_csv}")
            return csv_cols
        except Exception as e:
            raise ValueError(f"Validation error: {e}")

    def loader(self, csv_text: str):
        from psycopg2 import sql
        from uuid import uuid4

        csv_buffer, header, dsn = self._buffer(csv_text)
        try:
            with psycopg2.connect(dsn) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    csv_cols = self._validation(cur, header)

                    # Set session-level timeouts to avoid long locks
                    cur.execute("SET LOCAL statement_timeout = '60s'")
                    cur.execute("SET LOCAL lock_timeout = '5s'")

                    # Create a temporary staging table (auto-dropped on commit)
                    staging = f"tmp_staging_{uuid4().hex[:12]}"
                    temp_sql = sql.SQL(
                        """CREATE TEMP TABLE {stg} (LIKE {sch}.{tbl} INCLUDING DEFAULTS) ON COMMIT DROP"""
                    ).format(
                        stg=sql.Identifier(staging),
                        sch=sql.Identifier(self.schema),
                        tbl=sql.Identifier(self.table),
                    )
                    cur.execute(temp_sql.as_string(conn))
                    print(f"header: {header}")
                    print(f"csv_cols: {csv_cols}")
                    print(f"staging table: {staging}")

                    # COPY CSV → staging with explicit column list
                    copy_stmt = sql.SQL("""
                        COPY {stg} ({cols})
                        FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')
                    """).format(
                        stg=sql.Identifier(staging),
                        cols=sql.SQL(", ").join(map(sql.Identifier, csv_cols)),
                    )

                    cur.copy_expert(copy_stmt.as_string(conn), csv_buffer)

                    # Count staged rows
                    q_count = sql.SQL("SELECT COUNT(*) FROM {stg}").format(
                        stg=sql.Identifier(staging)
                    )
                    cur.execute(q_count.as_string(conn))
                    rows_staged = cur.fetchone()[0]
                    if rows_staged == 0:
                        conn.rollback()
                        return {
                            "status": "no-op",
                            "table": f"{self.schema}.{self.table}",
                            "rows_staged": 0,
                            "rows_upserted": 0,
                        }

                    # Build UPSERT statement
                    update_cols = [
                        c for c in csv_cols if c not in self.conflict_columns
                    ]
                    print(f"update_cols: {update_cols}")
                    if update_cols:
                        insert_sql = sql.SQL("""
                            INSERT INTO {sch}.{tbl} ({cols})
                            SELECT {cols} FROM {stg}
                            ON CONFLICT ({conflict})
                            DO UPDATE SET {set_clause}
                        """).format(
                            sch=sql.Identifier(self.schema),
                            tbl=sql.Identifier(self.table),
                            cols=sql.SQL(", ").join(map(sql.Identifier, csv_cols)),
                            stg=sql.Identifier(staging),
                            conflict=sql.SQL(", ").join(
                                map(sql.Identifier, self.conflict_columns)
                            ),
                            set_clause=sql.SQL(", ").join(
                                sql.SQL("{c}=EXCLUDED.{c}").format(c=sql.Identifier(c))
                                for c in update_cols
                            ),
                        )
                    else:
                        insert_sql = sql.SQL("""
                            INSERT INTO {sch}.{tbl} ({cols})
                            SELECT {cols} FROM {stg}
                            ON CONFLICT ({conflict}) DO NOTHING
                        """).format(
                            sch=sql.Identifier(self.schema),
                            tbl=sql.Identifier(self.table),
                            cols=sql.SQL(", ").join(map(sql.Identifier, csv_cols)),
                            stg=sql.Identifier(staging),
                            conflict=sql.SQL(", ").join(
                                map(sql.Identifier, self.conflict_columns)
                            ),
                        )

                    cur.execute(insert_sql.as_string(conn))
                    rows_upserted = cur.rowcount

                conn.commit()

            return {
                "status": "success",
                "table": f"{self.schema}.{self.table}",
                "rows_staged": rows_staged,
                "rows_upserted": rows_upserted,
            }
        except errors.NotNullViolation as e:
            # Let DB enforce schema; surface a clear message if it fails
            raise RuntimeError(
                f"Insert failed (NOT NULL constraint): {e.diag.column_name}"
            ) from e
        except errors.UniqueViolation as e:
            # Should be rare since ON CONFLICT handles most cases
            raise RuntimeError("Insert failed (unique constraint)") from e
