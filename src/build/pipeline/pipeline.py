from typing import Dict, Any, Tuple, List
from sklearn.pipeline import Pipeline
from src.ml.registry import feature, reducer, selector, model
# import ml.feature
# import ml.redecer
# import ml.selector
# import ml.model

# mapping
_KIND2REG = {
    "fe": feature,
    "reducer": reducer,
    "selector": selector,
    "model": model,
}


def make_obj(kind: str, name: str, **kw) -> Any:
    kind = kind.lower()
    name = name.lower()
    if kind not in _KIND2REG:
        raise ValueError(f"Unknown kind '{kind}'. Must be one of {list(_KIND2REG)}")
    return _KIND2REG[kind].create(name, **kw)


def pipeline_from_steps_dict(step_dict: Dict[str, Any]) -> Pipeline:
    steps: List[Tuple[str, Any]] = [
        (k, v) for k, v in sorted(step_dict.items(), key=lambda x: x[0])
    ]
    if not steps:
        raise ValueError("Empty steps_dict")

    # validate
    last_name, last_obj = steps[-1]
    if not (hasattr(last_obj, "fit") and hasattr(last_obj, "predict")):
        raise ValueError(f"Last step '{last_name}' must be a model (has fit/predict).")

    # for nm, obj in steps[:-1]:
    #     if not (hasattr(obj, "fit") and hasattr(obj, "transform")):
    #         raise ValueError(f"Step '{nm}' must be a transformer (has fit/transform).")

    return Pipeline(steps)


def pipeline_from_yaml(path: str) -> Pipeline:
    import yaml

    cfg = yaml.safe_load(open(path, "r", encoding="utf-8"))
    steps_cfg = cfg["steps"]
    step_dict = {}
    for step_id, spec in steps_cfg.items():
        kind = spec["kind"]
        name = spec["name"]
        params = {k: v for k, v in spec.items() if k not in ("kind", "name")}
        step_dict[step_id] = make_obj(kind, name, **params)
    return pipeline_from_steps_dict(step_dict)
