import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import umap
from sklearn.decomposition import PCA


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


# -----------------------------------------------------
# Load embeddings from gold file
# -----------------------------------------------------
def load_embedding_df(csv_path: str):
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")
    
    # clean category
    df["category"] = df["category"].astype(str).str.lower().str.strip()
    df["category"] = df["category"].apply(lambda c: c if c in VALID else "general")

    df["embedding_vec"] = df["embedding"].apply(json.loads)
    X = np.vstack(df["embedding_vec"].values)
    y = df["category"].values
    return df, X, y


# -----------------------------------------------------
# Plot UMAP
# -----------------------------------------------------
def plot_umap(X, y, save_path="embedding_umap.png"):
    reducer = umap.UMAP(n_neighbors=15, min_dist=0.1, random_state=42)
    X_emb = reducer.fit_transform(X)
    
    plt.figure(figsize=(10,7))
    for t in np.unique(y):
        idx = (y == t)
        plt.scatter(X_emb[idx, 0], X_emb[idx, 1], s=10, label=t, alpha=0.7)
    
    plt.title("UMAP projection of News Embeddings (2D)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()

    return save_path


# -----------------------------------------------------
# Plot PCA
# -----------------------------------------------------
def plot_pca(X, y, save_path="embedding_pca.png"):
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X)
    
    plt.figure(figsize=(10,7))
    for t in np.unique(y):
        idx = (y == t)
        plt.scatter(X_pca[idx, 0], X_pca[idx, 1], s=10, label=t, alpha=0.7)
    
    plt.title("PCA projection of News Embeddings (2D)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()

    return save_path


# -----------------------------------------------------
# Master function
# -----------------------------------------------------
def generate_all_plots(csv_path, output_dir="/opt/airflow/reports"):
    df, X, y = load_embedding_df(csv_path)
    
    umap_path = f"{output_dir}/embedding_umap.png"
    pca_path = f"{output_dir}/embedding_pca.png"

    print("Generating UMAP...")
    plot_umap(X, y, umap_path)

    print("Generating PCA...")
    plot_pca(X, y, pca_path)

    return umap_path, pca_path
