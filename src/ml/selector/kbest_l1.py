from src.registry import selector
from sklearn.feature_selection import (
    SelectKBest,
    chi2,
    mutual_info_classif,
    SelectFromModel,
)
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC


@selector.register("kbest_chi2", "chi2")
def make_kbest_chi2(**kw):
    return SelectKBest(score_func=chi2, **kw)


@selector.register("kbest_mi", "mutual_info")
def make_kbest_mi(**kw):
    return SelectKBest(score_func=mutual_info_classif, **kw)


@selector.register("l1_logreg")
def make_l1_logreg(threshold="median", max_features=None, **kw):
    base = LogisticRegression(penalty="l1", solver="liblinear", **kw)
    return SelectFromModel(base, threshold=threshold, max_features=max_features)


@selector.register("l1_svc")
def make_l1_svc(threshold="median", max_features=None, **kw):
    base = LinearSVC(penalty="l1", dual=False, **kw)
    return SelectFromModel(base, threshold=threshold, max_features=max_features)
