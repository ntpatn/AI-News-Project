from ml.registry import model

@model.register("logreg", "logistic", meta={"family": "linear", "proba": True})
def make_logreg(**kw):
    from sklearn.linear_model import LogisticRegression

    return LogisticRegression(**kw)


@model.register("linearsvc", "svm_linear", meta={"family": "svm", "proba": False})
def make_linearsvc(**kw):
    from sklearn.svm import LinearSVC

    return LinearSVC(**kw)
