import argparse
import json
import pandas as pd
import matplotlib.pyplot as plt
from dvc.api import params_show
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
from tqdm import tqdm
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder


def main():
    parser = argparse.ArgumentParser(description="Load a training dataset.")

    # Define the file path argument
    parser.add_argument("filepath", type=str, help="Path to the training parquet file")

    # Parse the arguments from the command line
    args = parser.parse_args()

    # Use the provided argument to load the dataframe
    df_train = pd.read_parquet(args.filepath)

    date_cols = ["date", "year", "month", "day"]
    price_cols = ["price_per_square_meter_normalized", "price_normalized"]
    num_cols = [
        c
        for c in df_train.select_dtypes(include="number").columns
        if c not in date_cols and c not in price_cols
    ]
    cat_cols = [
        c
        for c in df_train.select_dtypes(include="category").columns
        if c not in ["market_type"]
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), num_cols),
            ("cat", OneHotEncoder(), cat_cols),
        ]
    )

    params = params_show()["ksearch"]
    k_min = params["min_k"]
    k_max = params["max_k"]

    # Семпл для метрик и визуализации (10к - оптимально для UMAP)
    sample_size = 10_000
    if len(df_train) > sample_size:
        X_sample = df_train.sample(sample_size, random_state=42)
    else:
        X_sample = df_train

    df_train = preprocessor.fit_transform(df_train)
    X_sample = preprocessor.transform(X_sample)

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
            best_model = model

    res_df = pd.DataFrame(results)
    fig, axs = plt.subplots(1, 2, figsize=(20, 15))

    axs[0].plot(res_df["k"], res_df["inertia"], marker="o", color="green")
    axs[0].set_title("Elbow Method")
    axs[0].set_ylabel("Inertia")
    axs[0].grid(True, alpha=0.3)

    axs[1].plot(res_df["k"], res_df["silhouette"], marker="o", color="blue")
    axs[1].axvline(
        x=best_k_sil, color="red", linestyle="--", label=f"Best k={best_k_sil}"
    )
    axs[1].set_title("Silhouette Score")
    axs[1].set_ylabel("Score")
    axs[1].legend()
    axs[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("report/ksearch_plot.png")

    with open("report/ksearch_metrics.json", "w") as f:
        json.dump(
            {"best_k": int(best_k_sil), "max_silhouette": float(max_silhouette)}, f
        )


if __name__ == "__main__":
    main()
