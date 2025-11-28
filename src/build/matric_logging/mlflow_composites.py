from __future__ import annotations
from typing import Any, Dict, Iterable, Optional
import numpy as np
import pandas as pd

from src.build.matric_logging.mlflow_utils import (
    log_metrics,
    log_dict,
    log_dataframe_csv,
)


def log_search_summary(est, top_k: int = 20, csv_path: str = "cv/cv_results_top.csv"):
    best_score = getattr(est, "best_score_", None)
    if best_score is not None:
        log_metrics({"best_score": float(best_score)})

    best_params = getattr(est, "best_params_", None)
    if isinstance(best_params, dict):
        log_dict(best_params, "best_params.json")

    cv = getattr(est, "cv_results_", None)
    if isinstance(cv, dict) and len(cv) > 0:
        df = pd.DataFrame(cv)
        cols = [
            c
            for c in df.columns
            if c.startswith(
                (
                    "param_",
                    "mean_test_",
                    "std_test_",
                    "rank_test_",
                    "mean_fit_time",
                    "mean_score_time",
                )
            )
        ]
        if "rank_test_score" in df.columns:
            df = df.sort_values("rank_test_score").head(top_k)
        log_dataframe_csv(df[cols], csv_path)


def log_classification_report(
    report: Dict[str, Any],
    *,
    prefix: str = "",
    save_json_path: str = "eval/classification_report.json",
):
    if "accuracy" in report:
        log_metrics({f"{prefix}accuracy": float(report["accuracy"])})
    for k in ("macro avg", "weighted avg"):
        if k in report:
            m = report[k]
            log_metrics(
                {
                    f"{prefix}precision_{k.replace(' ', '_')}": float(
                        m.get("precision", 0.0)
                    ),
                    f"{prefix}recall_{k.replace(' ', '_')}": float(
                        m.get("recall", 0.0)
                    ),
                    f"{prefix}f1_{k.replace(' ', '_')}": float(m.get("f1-score", 0.0)),
                }
            )
    for cls, m in report.items():
        if cls in ("accuracy", "macro avg", "weighted avg"):
            continue
        if isinstance(m, dict) and "f1-score" in m:
            log_metrics({f"{prefix}f1_class_{cls}": float(m["f1-score"])})
    log_dict(report, save_json_path)


def log_confusion_matrix(
    y_true,
    y_pred,
    labels: Optional[Iterable] = None,
    path: str = "eval/confusion_matrix.json",
):
    from sklearn.metrics import confusion_matrix

    cm = confusion_matrix(y_true, y_pred, labels=labels)
    log_dict(
        {"labels": list(labels) if labels is not None else None, "matrix": cm.tolist()},
        path,
    )


def log_regression_metrics(y_true, y_pred, prefix: str = "val_"):
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

    mse = mean_squared_error(y_true, y_pred)
    rmse = float(np.sqrt(mse))
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    log_metrics(
        {
            f"{prefix}mse": mse,
            f"{prefix}rmse": rmse,
            f"{prefix}mae": mae,
            f"{prefix}r2": r2,
        }
    )
