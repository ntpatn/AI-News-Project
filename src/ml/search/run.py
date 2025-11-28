from src.registry import search
import mlflow


@mlflow.trace(name="run_search")
def run_search(alias, pipeline, X, y, space, **kw):
    builder = search.create(alias)
    est = builder(pipeline, space)
    est.fit(X, y)
    return est
