import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

EARTH_RADIUS = 6371000


def compute_wnir(
    df: pd.DataFrame,
    R: float,
    h: float,
    price_col: str = "price_per_square_meter_normalized",
):
    df = df.copy()

    output_col = "wnir"
    count_col = "wnir_neighbours_count"

    coord_counts = df.groupby(["latitude", "longitude"])["latitude"].transform("count")
    mask = coord_counts < 5000

    if not mask.any():
        df[output_col] = np.nan
        df[count_col] = 0
        return df

    df_clear = df.loc[mask]

    coords = df_clear[["latitude", "longitude"]].values
    coords_rad = np.radians(coords)
    values = df_clear[price_col].values

    tree = BallTree(coords_rad, metric="haversine")
    radius_rad = R / EARTH_RADIUS

    indices_array = tree.query_radius(coords_rad, r=radius_rad, return_distance=False)

    n = len(df_clear)
    wnir = np.full(n, np.nan)
    neighbor_counts = np.zeros(n, dtype=np.int32)

    for i, inds in enumerate(indices_array):
        if len(inds) <= 1:
            continue

        center = coords_rad[i]

        neighbors = coords_rad[inds]

        dlat = neighbors[:, 0] - center[0]
        dlon = neighbors[:, 1] - center[1]

        a = (
            np.sin(dlat / 2) ** 2
            + np.cos(center[0]) * np.cos(neighbors[:, 0]) * np.sin(dlon / 2) ** 2
        )
        dists = 2 * EARTH_RADIUS * np.arcsin(np.sqrt(a))

        mask_self = dists > 0
        if not mask_self.any():
            continue

        dists = dists[mask_self]
        neighbor_vals = values[inds][mask_self]

        neighbor_counts[i] = len(dists)

        weights = np.exp(-dists / h)

        wnir[i] = np.dot(weights, neighbor_vals) / weights.sum()

    df[output_col] = np.nan
    df[count_col] = -1

    df.loc[mask, output_col] = wnir
    df.loc[mask, count_col] = neighbor_counts

    return df, tree


def compute_ratio_wnir(
    df_primary: pd.DataFrame, df_secondary: pd.DataFrame, tree_secondary, R: float
) -> pd.DataFrame:

    df_primary = df_primary.copy()

    coords_primary = np.radians(df_primary[["latitude", "longitude"]].values)

    values_primary = df_primary["wnir"].values
    values_secondary = df_secondary["wnir"].values

    valid_secondary_mask = ~np.isnan(values_secondary)

    radius_rad = R / EARTH_RADIUS

    indices_array = tree_secondary.query_radius(
        coords_primary, r=radius_rad, return_distance=False
    )

    n = len(df_primary)
    ratio = np.full(n, np.nan)

    for i, inds in enumerate(indices_array):
        if len(inds) == 0:
            continue

        valid_inds = inds[valid_secondary_mask[inds]]

        if len(valid_inds) == 0:
            continue

        secondary_mean = values_secondary[valid_inds].mean()

        primary_val = values_primary[i]

        if not np.isnan(primary_val) and secondary_mean != 0:
            ratio[i] = primary_val / secondary_mean

    df_primary["primary_to_secondary_wnir_ratio"] = ratio

    return df_primary
