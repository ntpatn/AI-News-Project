from src.registry import feature
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer


@feature.register("tfidf")
def make_tfidf(**kw):
    return TfidfVectorizer(**kw)


@feature.register("count", "bow", "bag_of_words")
def make_count(**kw):
    return CountVectorizer(**kw)
