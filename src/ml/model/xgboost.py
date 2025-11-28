from src.registry import model


@model.register("xgb", "xgboost")
def make_xgboost(
    objective="multi:softmax",
    n_estimators=300,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    tree_method="hist",
    eval_metric="mlogloss",
    n_jobs=-1,
    **kw,
):
    from xgboost import XGBClassifier

    params = dict(
        objective=objective,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        max_depth=max_depth,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        tree_method=tree_method,
        eval_metric=eval_metric,
        n_jobs=n_jobs,
    )
    params.update(kw)

    return XGBClassifier(**params)
