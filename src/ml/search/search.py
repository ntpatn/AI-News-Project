from __future__ import annotations
from typing import Any, Callable
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, StratifiedKFold
from sklearn.metrics import make_scorer, f1_score
from sklearn.base import BaseEstimator
from ml.registry import train_tuning as search


def _default_cv():
    return StratifiedKFold(n_splits=5, shuffle=True, random_state=2025)


def _default_scorer():
    return make_scorer(f1_score, average="weighted")


@search.register("grid")
def make_grid_search(**preset) -> Callable[[BaseEstimator], Any]:
    def builder(pipe: BaseEstimator, **override):
        if "space" not in override:
            raise ValueError("[grid] missing required key: 'space'")
        space = override.pop("space")
        kw = {
            "cv": _default_cv(),
            "scoring": _default_scorer(),
            **preset,
            **override,
        }
        return GridSearchCV(estimator=pipe, param_grid=space, **kw)

    return builder


@search.register("random", "rand", "randomized")
def make_random_search(**preset) -> Callable[[BaseEstimator], Any]:
    def builder(pipe: BaseEstimator, **override):
        if "space" not in override:
            raise ValueError("[random] missing required key: 'space'")
        space = override.pop("space")
        kw = {
            "cv": _default_cv(),
            "scoring": _default_scorer(),
            "n_iter": 20,
            "random_state": 42,
            **preset,
            **override,
        }
        return RandomizedSearchCV(estimator=pipe, param_distributions=space, **kw)

    return builder
