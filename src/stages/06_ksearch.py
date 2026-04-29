import json
import pandas as pd
import matplotlib.pyplot as plt
from dvc.api import params_show
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
from tqdm import tqdm


def main():
    df_train = pd.read_parquet("data/interim/05_train.parquet")

    params = params_show()["06_ksearch"]
    k_min = params["min_k"]
    k_max = params["max_k"]

    sample_size = 10_000
    if len(df_train) > sample_size:
        X_sample = df_train.sample(sample_size, random_state=42)
    else:
        X_sample = df_train

    results = []
    best_k = k_min
    max_silhouette = -1

    print(f"Starting k-search ({k_min}-{k_max}) on all features...")

    for k in tqdm(range(k_min, k_max + 1)):
        # MiniBatchKMeans критичен для 1 млн строк
        model = MiniBatchKMeans(
            n_clusters=k,
            random_state=42,
            batch_size=8192,
            n_init=3,
            max_no_improvement=10,
        )
        model.fit(df_train)

        sample_labels = model.predict(X_sample)
        score = silhouette_score(X_sample, sample_labels)

        results.append({"k": int(k), "silhouette": float(score)})

        if score > max_silhouette:
            max_silhouette = score
            best_k = k

    with open("report/06_ksearch_metrics.json", "w") as f:
        json.dump(
            {
                "best_k": int(best_k),
                "max_silhouette": float(max_silhouette),
                "all_results": results,
            },
            f,
            indent=4,
        )

    res_df = pd.DataFrame(results)
    plt.figure(figsize=(12, 7))
    plt.plot(res_df["k"], res_df["silhouette"], marker="o", linestyle="-", color="blue")
    plt.axvline(x=best_k, color="red", linestyle="--", label=f"Best k={best_k}")
    plt.title("Silhouette Score: All Features Clustering")
    plt.xlabel("Number of clusters (k)")
    plt.ylabel("Silhouette Score")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig("report/06_ksearch_plot.png")

    print(f"Best k: {best_k} with score {max_silhouette:.4f}")


if __name__ == "__main__":
    main()
