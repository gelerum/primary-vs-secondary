import json
import pandas as pd
import matplotlib.pyplot as plt
from dvc.api import params_show
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from umap import UMAP
from tqdm import tqdm


def main():
    df_train = pd.read_parquet("data/interim/05_train.parquet")

    # Предполагаем, что признаки уже отмасштабированы (StandardScaler)
    # Если нет - PCA и UMAP выдадут шум.

    params = params_show()["06_ksearch"]
    k_min = params["min_k"]
    k_max = params["max_k"]

    # Семпл для метрик и визуализации (10к - оптимально для UMAP)
    sample_size = 10_000
    if len(df_train) > sample_size:
        X_sample = df_train.sample(sample_size, random_state=42)
    else:
        X_sample = df_train

    results = []
    best_k_sil = k_min
    max_silhouette = -1
    best_model = None

    print(f"Starting k-search ({k_min}-{k_max})...")

    for k in tqdm(range(k_min, k_max + 1)):
        model = MiniBatchKMeans(
            n_clusters=k,
            random_state=42,
            batch_size=8192,
            n_init=3,
            max_no_improvement=10,
        )
        model.fit(df_train)

        inertia = model.inertia_
        sample_labels = model.predict(X_sample)
        score = silhouette_score(X_sample, sample_labels) if k > 1 else 0

        results.append(
            {"k": int(k), "silhouette": float(score), "inertia": float(inertia)}
        )

        if score > max_silhouette:
            max_silhouette = score
            best_k_sil = k
            # Сохраняем лучшую модель для визуализации
            best_model = model

    # --- Подготовка данных для PCA и UMAP ---
    print(f"Generating PCA and UMAP for best k={best_k_sil}...")
    labels_sample = best_model.predict(X_sample)

    # PCA
    pca = PCA(n_components=2, random_state=42)
    X_pca = pca.fit_transform(X_sample)

    # UMAP (может занять 30-60 секунд на 10к точках)
    umap_model = UMAP(n_neighbors=15, min_dist=0.1, random_state=42)
    X_umap = umap_model.fit_transform(X_sample)

    # --- ВИЗУАЛИЗАЦИЯ (сетка 2x2) ---
    res_df = pd.DataFrame(results)
    fig, axs = plt.subplots(2, 2, figsize=(20, 15))

    # 1. Локоть
    axs[0, 0].plot(res_df["k"], res_df["inertia"], marker="o", color="green")
    axs[0, 0].set_title("Elbow Method")
    axs[0, 0].set_ylabel("Inertia")
    axs[0, 0].grid(True, alpha=0.3)

    # 2. Силуэт
    axs[0, 1].plot(res_df["k"], res_df["silhouette"], marker="o", color="blue")
    axs[0, 1].axvline(
        x=best_k_sil, color="red", linestyle="--", label=f"Best k={best_k_sil}"
    )
    axs[0, 1].set_title("Silhouette Score")
    axs[0, 1].set_ylabel("Score")
    axs[0, 1].legend()
    axs[0, 1].grid(True, alpha=0.3)

    # 3. PCA Scatter
    scatter1 = axs[1, 0].scatter(
        X_pca[:, 0], X_pca[:, 1], c=labels_sample, cmap="viridis", s=5, alpha=0.6
    )
    axs[1, 0].set_title(f"PCA Projection (k={best_k_sil})")
    plt.colorbar(scatter1, ax=axs[1, 0])

    # 4. UMAP Scatter
    scatter2 = axs[1, 1].scatter(
        X_umap[:, 0], X_umap[:, 1], c=labels_sample, cmap="viridis", s=5, alpha=0.6
    )
    axs[1, 1].set_title(f"UMAP Projection (k={best_k_sil})")
    plt.colorbar(scatter2, ax=axs[1, 1])

    plt.tight_layout()
    plt.savefig("report/06_ksearch_plot.png")

    # Сохраняем метрики
    with open("report/06_ksearch_metrics.json", "w") as f:
        json.dump(
            {"best_k": int(best_k_sil), "max_silhouette": float(max_silhouette)}, f
        )


if __name__ == "__main__":
    main()
