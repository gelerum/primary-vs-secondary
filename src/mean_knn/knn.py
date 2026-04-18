import numpy as np
from sklearn.neighbors import BallTree

EARTH_RADIUS = 6371000


def compute_mean_knn_by_geo(df, k: int, h: float):
    coords = np.radians(df[["latitude", "longitude"]].to_numpy(dtype=np.float32))
    tree = BallTree(coords, metric="haversine", leaf_size=40)

    k += 1

    dist, ind = tree.query(coords, k=k)
    dist_m = dist * EARTH_RADIUS

    prices = df["price_normalized"].values
    prices_per_m2 = df["price_per_square_meter_normalized"].values

    dist_m = dist_m[:, 1:]
    ind = ind[:, 1:]

    max_dist_knn = np.max(dist_m, axis=1)
    df["max_distance_knn"] = max_dist_knn

    dist_m[dist_m == 0] = 1

    weights = np.exp(-dist_m / h)

    weighted_price = np.sum(weights * prices[ind], axis=1) / np.sum(weights, axis=1)
    weighted_price_per_m2 = np.sum(weights * prices_per_m2[ind], axis=1) / np.sum(
        weights, axis=1
    )

    df["knn_weighted_price_normalized"] = weighted_price
    df["knn_weighted_price_per_square_meter_normalized"] = weighted_price_per_m2

    return df, coords, tree


def compute_ratio_knn(df_primary, df_secondary, coords_primary, tree_secondary, k):
    dist, ind = tree_secondary.query(coords_primary, k=k)

    secondary_vals = df_secondary[
        "knn_weighted_price_per_square_meter_normalized"
    ].values[ind]
    secondary_mean = secondary_vals.mean(axis=1)

    primary_vals = df_primary["knn_weighted_price_per_square_meter_normalized"].values

    ratio = primary_vals / secondary_mean

    df_primary["primary_to_secondary_price_ratio"] = ratio

    return df_primary
