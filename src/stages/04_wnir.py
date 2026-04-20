from dvc.api import params_show
import pandas as pd
from sklearn.neighbors import BallTree
import numpy as np

from src.wnir.wnir import compute_wnir_for_market, compute_ratio_wnir


def main():
    params = params_show()["04_wnir"]

    df = pd.read_parquet("data/interim/03_price_discounted.parquet")

    df_primary = df[df["market_type"] == "primary"].copy()
    df_secondary = df[df["market_type"] == "secondary"].copy()

    coords_rad_primary = np.radians(df_primary[["latitude", "longitude"]].values)
    tree_primary = BallTree(coords_rad_primary, metric="haversine")

    coords_rad_secondary = np.radians(df_secondary[["latitude", "longitude"]].values)
    tree_secondary = BallTree(coords_rad_secondary, metric="haversine")

    h = params["h"]
    Rs = params["R"].values()

    for r in Rs:
        df_primary = compute_wnir_for_market(df_primary, tree_primary, R=r, h=h)
        df_secondary = compute_wnir_for_market(df_secondary, tree_secondary, R=r, h=h)

        new_cols_primary, new_cols_secondary = compute_ratio_wnir(
            df_primary, df_secondary, tree_primary, tree_secondary, R=r
        )

        df_primary = df_primary.join(new_cols_primary)
        df_secondary = df_secondary.join(new_cols_secondary)

    df_wnir = pd.concat([df_primary, df_secondary])

    df_wnir.to_parquet("data/interim/04_wnir.parquet", index=False)


if __name__ == "__main__":
    main()
