from dvc.api import params_show
import pandas as pd

from src.wnir.wnir import compute_wnir, compute_ratio_wnir


def main():
    params = params_show()["02_geocode"]

    df = pd.read_parquet("data/interim/03_price_discounted.parquet")

    df_primary = df[df["market_type"] == "primary"].copy()

    df_secondary = df[df["market_type"] == "secondary"].copy()

    h = params["h"]
    R = 500
    # Rs = params["R"].values()
    # for r in Rs:
    #     R = 500
    df_primary, tree_primary = compute_wnir(df_primary, R=R, h=h)
    df_secondary, tree_secondary = compute_wnir(df_secondary, R=R, h=h)

    df_primary = compute_ratio_wnir(df_primary, df_secondary, tree_secondary, R=R)

    df_mean_knn = pd.concat([df_primary, df_secondary])

    df_mean_knn.to_parquet("data/interim/04_wnir.parquet", index=False)


if __name__ == "__main__":
    main()
