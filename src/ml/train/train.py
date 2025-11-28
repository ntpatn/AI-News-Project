from __future__ import annotations
from typing import Dict, Any
from sklearn.base import BaseEstimator
import mlflow


@mlflow.trace(name="train_with_search")
def train_with_search(pipe: BaseEstimator, x_train, y_train, search) -> BaseEstimator:
    """ดึง best_params_ จาก search แล้ว set_params + fit สุดท้าย"""
    pipe.set_params(**search.best_params_)
    pipe.fit(x_train, y_train)
    return pipe


@mlflow.trace(name="train_with_params")
def train_with_params(
    pipe: BaseEstimator, X, y, best_params: Dict[str, Any]
) -> BaseEstimator:
    """ทางเลือก: ถ้ามี best_params เป็น dict อยู่แล้ว"""
    pipe.set_params(**best_params)
    pipe.fit(X, y)
    return pipe
