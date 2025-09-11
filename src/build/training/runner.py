from __future__ import annotations
from typing import Any, Dict
from sklearn.base import BaseEstimator
from src.ml.registry import train_tuning as search

# mapping
_KIND2REG = {
    "search": search,
}


def run_search(
    kind: str, alias: str, pipe: BaseEstimator, X, y, space: Dict[str, Any], **kw
):
    if kind not in _KIND2REG:
        raise ValueError(f"Unknown kind {kind}. Must be one of {list(_KIND2REG)}")
    builder = _KIND2REG[kind].create(alias, **kw)
    est = builder(pipe, space)
    est.fit(X, y)
    return est
