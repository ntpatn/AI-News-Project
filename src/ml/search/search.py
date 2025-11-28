# src/ml/search/registry.py
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, StratifiedKFold
from sklearn.metrics import f1_score, make_scorer
from src.registry import search


def _default_cv():
    return StratifiedKFold(n_splits=5, shuffle=True, random_state=2025)


def _default_scorer():
    return make_scorer(f1_score, average="weighted")


@search.register("grid")
def make_grid_search(**preset):
    def builder(pipe, space):
        return GridSearchCV(
            estimator=pipe,
            param_grid=space,
            cv=_default_cv(),
            scoring=_default_scorer(),
            **preset,
        )

    return builder


@search.register("random")
def make_random_search(**preset):
    def builder(pipe, space):
        return RandomizedSearchCV(
            estimator=pipe,
            param_distributions=space,
            cv=_default_cv(),
            scoring=_default_scorer(),
            n_iter=20,
            random_state=42,
            **preset,
        )

    return builder
