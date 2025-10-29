# src/etl/bronze/load/utils/validation.py
def validate_columns(
    cur, schema: str, table: str, header: list[str], conflict_columns: list[str]
):
    """Validate CSV header against table columns"""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    rows = cur.fetchall()
    if not rows:
        raise ValueError(f"Table {schema}.{table} not found")

    database_table_cols = [r[0] for r in rows]
    tbl_map = {c.lower(): c for c in database_table_cols}

    csv_cols = []
    seen = set()
    for h in header:
        key = h.lower()
        if key in tbl_map and tbl_map[key] not in seen:
            csv_cols.append(tbl_map[key])
            seen.add(tbl_map[key])

    if not csv_cols:
        raise ValueError("CSV columns don't match table")

    for c in conflict_columns:
        if c not in database_table_cols:
            raise ValueError(f"Conflict column '{c}' not in table")
        if c not in csv_cols:
            raise ValueError(f"Conflict column '{c}' missing in CSV")

    return csv_cols, database_table_cols
