from __future__ import annotations
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, StratifiedKFold
from sklearn.metrics import f1_score, make_scorer
from sklearn.base import BaseEstimator


# =====================
# Step 1: Strategy Interface
# =====================
class SearchStrategy(ABC):
    @abstractmethod
    def search(
        self,
        pipe: BaseEstimator,
        X,
        y,
        space: Dict[str, Any],
        *,
        cv=None,
        scoring=None,
        n_jobs: int = -1,
        random_state: int = 42,
        verbose: int = 1,
        refit: bool = False,
        n_iter: Optional[int] = None,
    ):
        pass


# Defaults
def _default_cv():
    return StratifiedKFold(n_splits=5, shuffle=True, random_state=2025)


def _default_scorer():
    return make_scorer(f1_score, average="weighted")


# =====================
# Step 2: Concrete Strategies
# =====================
class GridSearchStrategy(SearchStrategy):
    def search(
        self,
        pipe,
        X,
        y,
        space,
        *,
        cv=None,
        scoring=None,
        n_jobs=-1,
        random_state=42,
        verbose=1,
        refit=False,
        n_iter=None,
    ):
        cv = cv or _default_cv()
        scoring = scoring or _default_scorer()
        search = GridSearchCV(
            estimator=pipe,
            param_grid=space,
            scoring=scoring,
            cv=cv,
            n_jobs=n_jobs,
            verbose=verbose,
            refit=refit,
        )
        search.fit(X, y)
        return search


class RandomSearchStrategy(SearchStrategy):
    def search(
        self,
        pipe,
        X,
        y,
        space,
        *,
        cv=None,
        scoring=None,
        n_jobs=-1,
        random_state=42,
        verbose=1,
        refit=False,
        n_iter: Optional[int] = 20,
    ):
        cv = cv or _default_cv()
        scoring = scoring or _default_scorer()
        search = RandomizedSearchCV(
            estimator=pipe,
            param_distributions=space,
            n_iter=n_iter or 20,
            scoring=scoring,
            cv=cv,
            n_jobs=n_jobs,
            random_state=random_state,
            verbose=verbose,
            refit=refit,
        )
        search.fit(X, y)
        return search


# =====================
# Step 3: Context
# =====================
class ModelSearchContext:
    def __init__(self, strategy: SearchStrategy):
        self.strategy = strategy

    def run(self, pipe, X, y, space, **kwargs):
        return self.strategy.search(pipe, X, y, space, **kwargs)


# =====================
# Step 4: Usage
# =====================
if __name__ == "__main__":
    from sklearn.linear_model import LogisticRegression
    from sklearn.datasets import load_iris

    X, y = load_iris(return_X_y=True)
    pipe = LogisticRegression(max_iter=200)

    # ใช้ GridSearch
    grid_strategy = GridSearchStrategy()
    context = ModelSearchContext(grid_strategy)
    result = context.run(pipe, X, y, space={"C": [0.1, 1, 10]}, refit=True)
    print("Best params (grid):", result.best_params_)

    # ใช้ RandomSearch
    random_strategy = RandomSearchStrategy()
    context = ModelSearchContext(random_strategy)
    result = context.run(pipe, X, y, space={"C": [0.1, 1, 10]}, n_iter=5, refit=True)
    print("Best params (random):", result.best_params_)
