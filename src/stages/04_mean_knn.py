import pandas as pd

from src.mean_knn.mean_knn import compute_mean_knn_by_geo, compute_ratio_knn


def main():
    df = pd.read_parquet("data/interim/03_price_discounted.parquet")

    df_primary = df[df["market_type"] == "primary"].copy()

    df_secondary = df[df["market_type"] == "secondary"].copy()

    df_primary, coords_primary, _ = compute_mean_knn_by_geo(df_primary, k=30, h=500)
    df_secondary, _, tree_secondary = compute_mean_knn_by_geo(df_secondary, k=30, h=500)

    df_primary = compute_ratio_knn(
        df_primary,
        df_secondary,
        coords_primary,
        tree_secondary,
        k=30,
    )

    df_mean_knn = pd.concat([df_primary, df_secondary])

    df_mean_knn.to_parquet("data/interim/04_mean_knn.parquet", index=False)


if __name__ == "__main__":
    main()
