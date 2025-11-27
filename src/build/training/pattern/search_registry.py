# search_registry.py
from __future__ import annotations
from typing import Dict, Union, Literal, Callable, Type
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.base import BaseEstimator

from .base_search_registry import SearchStrategy, SearchCommonConfig

__all__ = [
    "register",
    "STRATEGY_REGISTRY",
    "resolve_strategy",
    "run_search",
]

STRATEGY_REGISTRY: Dict[str, SearchStrategy] = {}


def register(name: str) -> Callable[[Type[SearchStrategy]], Type[SearchStrategy]]:
    def _decorator(cls: Type[SearchStrategy]) -> Type[SearchStrategy]:
        STRATEGY_REGISTRY[name] = cls()
        return cls

    return _decorator


def resolve_strategy(s: Union[str, SearchStrategy]) -> SearchStrategy:
    if isinstance(s, str):
        try:
            return STRATEGY_REGISTRY[s]
        except KeyError:
            raise KeyError(
                f"Unknown strategy '{s}'. Available: {list(STRATEGY_REGISTRY)}"
            )
    return s


def run_search(
    pipe: BaseEstimator,
    X,
    y,
    space: dict,
    *,
    strategy: Union[Literal["grid", "random"], SearchStrategy] = "grid",
    common: SearchCommonConfig | None = None,
    **opts,
):
    strat = resolve_strategy(strategy)
    if common is not None:
        strat.common = common
    return strat.search(pipe, X, y, space, **opts)


@register("grid")
class GridSearchStrategy(SearchStrategy):
    def _build(self, *, pipe, space, **kwargs):
        return GridSearchCV(estimator=pipe, param_grid=space, **kwargs)


@register("random")
class RandomSearchStrategy(SearchStrategy):
    def _build(
        self, *, pipe, space, n_iter: int = 20, random_state: int = 42, **kwargs
    ):
        return RandomizedSearchCV(
            estimator=pipe,
            param_distributions=space,
            n_iter=n_iter,
            random_state=random_state,
            **kwargs,
        )
