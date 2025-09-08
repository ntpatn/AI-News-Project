from ml.registry import feature as fe
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer


@fe.register("tfidf", "tf-idf")
def make_tfidf(**kw):
    return TfidfVectorizer(**kw)


@fe.register("count", "bow", "bag_of_words")
def make_count(**kw):
    return CountVectorizer(**kw)
