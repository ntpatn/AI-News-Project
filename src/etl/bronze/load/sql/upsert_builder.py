# src/etl/bronze/load/sql/upsert_builder.py
from psycopg2 import sql


def build_upsert_condition(
    schema, table, staging, database_table_cols, csv_cols, conflict_columns
):
    db_cols_set = set(database_table_cols)
    extras = ("createdate", "usercreate", "updatedate", "userupdate", "sf_id")

    now_expr = sql.SQL("timezone('UTC', now())")

    # Select & Target columns
    target_cols = list(csv_cols) + [
        e for e in extras if e in db_cols_set and e not in csv_cols
    ]
    select_exprs = []
    for c in target_cols:
        if c == "createdate":
            select_exprs.append(now_expr)
        elif c == "usercreate":
            select_exprs.append(sql.Literal("system"))
        elif c in ("updatedate", "userupdate"):
            select_exprs.append(sql.SQL("NULL"))
        elif c == "sf_id":
            select_exprs.append(
                sql.SQL("{stg}.{c}").format(
                    stg=sql.Identifier(staging), c=sql.Identifier(c)
                )
            )
        else:
            select_exprs.append(
                sql.SQL("{stg}.{c}").format(
                    stg=sql.Identifier(staging), c=sql.Identifier(c)
                )
            )

    update_cols = [c for c in csv_cols if c not in conflict_columns and c not in extras]
    do_update_sets = [
        sql.SQL("{c}=EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in update_cols
    ]

    if "updatedate" in db_cols_set:
        do_update_sets.append(sql.SQL("updatedate=timezone('UTC', now())"))
    if "userupdate" in db_cols_set:
        do_update_sets.append(sql.SQL("userupdate='system'"))

    predicates = [
        sql.SQL("{tbl}.{c} IS DISTINCT FROM EXCLUDED.{c}").format(
            tbl=sql.Identifier(table), c=sql.Identifier(c)
        )
        for c in update_cols
    ]
    update_where = sql.SQL(" OR ").join(predicates) if predicates else sql.SQL("TRUE")

    return target_cols, select_exprs, do_update_sets, update_where
