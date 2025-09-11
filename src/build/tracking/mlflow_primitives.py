from __future__ import annotations
from typing import Any, Dict, Optional
import contextlib
import json
import numpy as np
import pandas as pd

try:
    import mlflow
    import mlflow.sklearn

    _HAS_MLFLOW = True
except ImportError:
    _HAS_MLFLOW = False


def _to_py(v: Any) -> Any:
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.ndarray,)):
        return v.tolist()
    return v


def set_tracking(uri: str = "file:./mlruns", experiment: str = "default"):
    if not _HAS_MLFLOW:
        return
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment(experiment)


def start_run(run_name: Optional[str] = None, tags: Dict[str, str] | None = None):
    if not _HAS_MLFLOW:
        return contextlib.nullcontext()
    ctx = mlflow.start_run(run_name=run_name)
    if tags:
        mlflow.set_tags(tags)
    return ctx


def log_params(d: Dict[str, Any], prefix: str = ""):
    if not _HAS_MLFLOW:
        return
    for k, v in d.items():
        mlflow.log_param(f"{prefix}{k}", _to_py(v))


def log_metrics(d: Dict[str, float], prefix: str = ""):
    if not _HAS_MLFLOW:
        return
    for k, v in d.items():
        mlflow.log_metric(f"{prefix}{k}", float(v))


def log_dict(data: Dict[str, Any], path: str):
    if not _HAS_MLFLOW:
        return
    mlflow.log_text(json.dumps(data, ensure_ascii=False, indent=2), path)


def log_dataframe_csv(df: pd.DataFrame, path: str):
    if not _HAS_MLFLOW:
        return
    mlflow.log_text(df.to_csv(index=False), path)


def log_model(model, artifact_path: str = "model"):
    if not _HAS_MLFLOW:
        return
    mlflow.sklearn.log_model(model, artifact_path=artifact_path)
