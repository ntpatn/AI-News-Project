import json
import numpy as np
from sentence_transformers import SentenceTransformer
import mlflow
import mlflow.pyfunc

mlflow.set_tracking_uri("http://localhost:5050")
# ------------------------------------------------------------
# Category encode/decode mapping
# ------------------------------------------------------------
VALID = [
    "business",
    "entertainment",
    "general",
    "health",
    "science",
    "sports",
    "technology",
    "politics",
]

encode_map = {cat: i for i, cat in enumerate(VALID)}
decode_map = {i: cat for i, cat in enumerate(VALID)}


# ------------------------------------------------------------
# Load embedding model (SBERT) — loaded once
# ------------------------------------------------------------
_sbert = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device="cuda")
_model = None
MODEL_NAME = "embed_xgb_model"


def load_best_model_from_registry():
    client = mlflow.tracking.MlflowClient()

    # เอา versions ทั้งหมดของ model นี้
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")

    if len(versions) == 0:
        raise ValueError(f"No model registered under name '{MODEL_NAME}'")

    best_version = None
    best_acc = -999

    # ดูทุก version แล้วหาอันที่ accuracy สูงสุด
    for v in versions:
        run_id = v.run_id
        metrics = mlflow.get_run(run_id).data.metrics

        acc = metrics.get("val_accuracy", None)
        if acc is None:
            continue

        if acc > best_acc:
            best_acc = acc
            best_version = v.version

    if best_version is None:
        raise ValueError("No version has val_accuracy metric logged.")

    print(f"[AutoSelect] Using {MODEL_NAME} version {best_version} (acc={best_acc})")

    model_uri = f"models:/{MODEL_NAME}/{best_version}"
    return mlflow.pyfunc.load_model(model_uri)


def get_model():
    """Load model once and reuse"""
    global _model
    if _model is None:
        _model = load_best_model_from_registry()
    return _model


# ------------------------------------------------------------
# Predict single text
# ------------------------------------------------------------
def predict_category(text: str):
    if not isinstance(text, str) or len(text.strip()) == 0:
        raise ValueError("Text input is empty")

    # 1) embed text
    emb = _sbert.encode([text]).tolist()

    # 2) load best model
    model = get_model()

    # 3) predict probabilities
    probs = model.predict(emb)[0]

    label = int(np.argmax(probs))
    confidence = float(np.max(probs))

    return {
        "category_encode": label,
        "category": decode_map[label],
        "confidence": confidence,
    }


# ------------------------------------------------------------
# Predict batch
# ------------------------------------------------------------
def predict_batch(text_list):
    """
    Batch inference for multiple texts
    """
    if not isinstance(text_list, list):
        raise ValueError("Input must be a list of texts")

    embeddings = _sbert.encode(text_list).tolist()
    model = get_model()
    probs = model.predict(embeddings)

    results = []
    for text, p in zip(text_list, probs):
        label = int(np.argmax(p))
        results.append(
            {
                "text": text,
                "category_encode": label,
                "category": decode_map[label],
                "confidence": float(np.max(p)),
            }
        )

    return results
