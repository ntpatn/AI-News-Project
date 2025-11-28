from src.registry import feature
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


class SBertEmbedding(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        batch_size=32,
        show_progress_bar=False,
        device=None,
    ):
        self.model_name = model_name
        self.batch_size = batch_size
        self.show_progress_bar = show_progress_bar
        self.device = device

        # โหลด model ทันที
        self.model = SentenceTransformer(model_name, device=device)

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        embeddings = self.model.encode(
            list(X),
            batch_size=self.batch_size,
            show_progress_bar=self.show_progress_bar,
        )
        return np.asarray(embeddings, dtype="float32")


@feature.register("embed", "sbert", meta={"family": "text_embedding"})
def make_embedding(**kw):
    return SBertEmbedding(**kw)
