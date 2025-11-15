import pandas as pd
import great_expectations as gx
import logging
from typing import List, Dict, Any
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


def setup_gx_context_auto(
    data,
    *,
    name="asset",
    query=None,
    conn_str=None,
):
    context = gx.get_context()

    def _pandas_batch(df):
        ds = context.data_sources.add_pandas("pandas_ds")
        asset = ds.add_dataframe_asset(name=name)
        batch_def = asset.add_batch_definition_whole_dataframe("batch_def")
        return batch_def.get_batch(batch_parameters={"dataframe": df})

    def _spark_batch(df):
        ds = context.data_sources.add_spark("spark_ds")
        asset = ds.add_dataframe_asset(name=name)
        batch_def = asset.add_batch_definition_whole_dataframe("batch_def")
        return batch_def.get_batch(batch_parameters={"dataframe": df})

    def _parquet_batch(path):
        ds = context.data_sources.add_pandas("parquet_ds")
        asset = ds.add_parquet_asset(name=name, filepath=path)
        batch_def = asset.add_batch_definition_whole_dataframe("batch_def")
        return batch_def.get_batch({})

    def _postgres_batch():
        if not conn_str:
            raise AirflowException("Postgres mode requires `conn_str`.")
        if not query:
            raise AirflowException("Postgres mode requires `query`.")

        ds = context.data_sources.add_sqlalchemy(
            "postgres_ds",
            connection_string=conn_str,
        )
        asset = ds.add_query_asset(name=name, query=query)
        batch_def = asset.add_batch_definition("batch_def")
        return batch_def.get_batch({})

    if isinstance(data, pd.DataFrame):
        return _pandas_batch(data)

    if data.__class__.__module__.startswith("pyspark"):
        return _spark_batch(data)

    if isinstance(data, str) and data.lower().endswith(".parquet"):
        return _parquet_batch(data)

    if conn_str:
        return _postgres_batch()

    raise AirflowException(
        f"Unsupported input type: {type(data)} "
        "(expected DataFrame, parquet path, or Postgres + query)."
    )


def run_validations(batch, expectations: List[Any]) -> Dict[str, Any]:
    """
    Run all expectations and return aggregated results.

    Returns:
        {
            'passed': int,
            'failed': int,
            'total': int,
            'success_rate': float,
            'failures': List[dict]
        }
    """
    failed = []
    passed_count = 0
    total = len(expectations)

    for idx, exp in enumerate(expectations, 1):
        exp_type = exp.__class__.__name__
        exp_column = getattr(exp, "column", None) or "N/A"

        try:
            result = batch.validate(exp)
            success = getattr(result, "success", False)

            if success:
                passed_count += 1
                logger.info(f"✅ [{idx}/{total}] {exp_type} - {exp_column}")
            else:
                # บาง datasource ไม่มี result.result → ใช้ getattr แบบปลอดภัย
                detail = getattr(result, "result", None)
                failed.append(
                    {
                        "index": idx,
                        "expectation": exp_type,
                        "column": exp_column,
                        "result": detail if detail is not None else "No result payload",
                    }
                )
                logger.warning(f"❌ [{idx}/{total}] {exp_type} - {exp_column}")

        except Exception as e:
            # ครอบ error ของทุก datasource (spark/sql/pandas)
            failed.append(
                {
                    "index": idx,
                    "expectation": exp_type,
                    "column": exp_column,
                    "error": str(e),
                }
            )
            logger.error(f"❌ [{idx}/{total}] ERROR: {exp_type} - {str(e)}")

    return {
        "passed": passed_count,
        "failed": len(failed),
        "total": total,
        "success_rate": (passed_count / total) * 100 if total else 0,
        "failures": failed,
    }


def log_validation_summary(csv_path: str, df: pd.DataFrame, results: Dict[str, Any]):
    """Log validation summary (works with any batch type)."""

    # กัน df ไม่มี attribute เช่น spark DF (ไม่มี len(df.columns))
    try:
        row_count = len(df)
    except Exception:
        row_count = "N/A"

    try:
        col_count = len(df.columns)
    except Exception:
        col_count = "N/A"

    logger.info(
        f"""
    ═══════════════════════════════════════════
    📊 Validation Summary
    ═══════════════════════════════════════════
    File: {csv_path}
    Rows: {row_count} | Columns: {col_count}
    ───────────────────────────────────────────
    Expectations: {results.get("total")}
    ✅ Passed: {results.get("passed")}
    ❌ Failed: {results.get("failed")}
    📈 Success Rate: {results.get("success_rate", 0):.1f}%
    ═══════════════════════════════════════════
    """
    )


def raise_validation_error(results: Dict[str, Any]):
    """Raise AirflowException with detailed failures (universal)."""

    failures = results.get("failures", [])
    if not failures:
        return

    lines = ["Validation Failures:", "=" * 50]

    for f in failures:
        idx = f.get("index", "?")
        exp = f.get("expectation", "UnknownExpectation")
        col = f.get("column", "N/A")

        # รองรับผลลัพธ์หลากหลายแบบ
        err = f.get("error") or f.get("result") or "Unknown failure"

        lines.append(f"[{idx}] ❌ {exp} ({col}): {err}")

    summary = "\n".join(lines)
    raise AirflowException(f"Validation failed:\n{summary}")
