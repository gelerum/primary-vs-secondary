from dvc.api import params_show
import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree

from src.wnir.wnir import compute_wnir_for_market, compute_ratio_wnir


def main():
    params = params_show()["04_wnir"]
    h = params["h"]
    Rs = list(params["R"].values())  # convert dict_values to list if needed

    df = pd.read_parquet("data/interim/03_price_discounted.parquet")

    df_primary = df[df["market_type"] == "primary"].copy()
    df_secondary = df[df["market_type"] == "secondary"].copy()

    # Pre-compute coordinates in radians (float32 for memory)
    coords_primary = np.radians(
        df_primary[["latitude", "longitude"]].values.astype(np.float32)
    )
    coords_secondary = np.radians(
        df_secondary[["latitude", "longitude"]].values.astype(np.float32)
    )

    # Trees for ratio computation (built on full data)
    tree_primary = BallTree(coords_primary, metric="haversine", leaf_size=40)
    tree_secondary = BallTree(coords_secondary, metric="haversine", leaf_size=40)

    for r in Rs:
        print(f"Processing R = {r} ...")

        # WNIR computation (rebuilds tree internally on filtered subset)
        df_primary = compute_wnir_for_market(df_primary, R=r, h=h, leaf_size=40)
        df_secondary = compute_wnir_for_market(df_secondary, R=r, h=h, leaf_size=40)

        # Ratio computation
        new_cols_primary, new_cols_secondary = compute_ratio_wnir(
            df_primary, df_secondary, tree_primary, tree_secondary, R=r
        )

        df_primary = df_primary.join(new_cols_primary)
        df_secondary = df_secondary.join(new_cols_secondary)

    # Combine and save
    df_wnir = pd.concat([df_primary, df_secondary], ignore_index=False)
    df_wnir.to_parquet("data/interim/04_wnir.parquet", index=False)

    print("WNIR stage completed successfully.")


if __name__ == "__main__":
    main()
