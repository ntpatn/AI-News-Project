from ml.registry import reducer
from sklearn.decomposition import TruncatedSVD, PCA


@reducer.register("svd", "tsvd", "lsa")
def make_svd(**kw):
    return TruncatedSVD(**kw)


@reducer.register("pca")
def make_pca(**kw):
    return PCA(**kw)
