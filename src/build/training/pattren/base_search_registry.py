# base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import make_scorer, f1_score
from sklearn.base import BaseEstimator
import time
import logging

__all__ = [
    "SearchCommonConfig",
    "SearchStrategy",
    "_default_cv",
    "_default_scorer",
]

log = logging.getLogger(__name__)


def _default_cv():
    return StratifiedKFold(n_splits=5, shuffle=True, random_state=2025)


def _default_scorer():
    return make_scorer(f1_score, average="weighted")


@dataclass
class SearchCommonConfig:
    cv: Any = None
    scoring: Any = None
    n_jobs: int = -1
    verbose: int = 1
    refit: bool = True


class SearchStrategy(ABC):
    def __init__(self, *, common: Optional[SearchCommonConfig] = None):
        self.common = common or SearchCommonConfig()

    def search(self, pipe: BaseEstimator, X, y, space: Dict[str, Any], **opts):
        cv = self.common.cv or _default_cv()
        scoring = self.common.scoring or _default_scorer()

        kwargs = {
            "scoring": scoring,
            "cv": cv,
            "n_jobs": self.common.n_jobs,
            "verbose": self.common.verbose,
            "refit": self.common.refit,
            **opts,
        }

        t0 = time.time()
        est = self._build(pipe=pipe, space=space, **kwargs)
        log.debug(
            "Built search: %s with kwargs=%s",
            est.__class__.__name__,
            {k: v for k, v in kwargs.items() if k not in ("scoring", "cv")},
        )

        est.fit(X, y)
        log.info(
            "Search finished in %.2fs (%s)", time.time() - t0, est.__class__.__name__
        )
        return est

    @abstractmethod
    def _build(self, *, pipe: BaseEstimator, space: Dict[str, Any], **kwargs): ...
