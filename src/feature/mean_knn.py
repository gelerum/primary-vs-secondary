import numpy as np
from sklearn.neighbors import BallTree


def compute_mean_knn_by_geo(df, k: int, h: float):
    coords = np.radians(df[["latitude", "longitude"]].to_numpy(dtype=np.float32))
    tree = BallTree(coords, metric="haversine", leaf_size=40)

    k += 1  # берем на одну больше, так как саму точку не учитываем

    # 2. строим дерево

    dist, ind = tree.query(coords, k=k)
    dist_m = dist * 6371000

    prices = df["price"].values

    # удаляем первый столбец (сама точка)
    dist_m = dist_m[:, 1:]
    ind = ind[:, 1:]

    dist_m[dist_m == 0] = 1

    weights = np.exp(-dist_m / h)

    weighted_price = np.sum(weights * prices[ind], axis=1) / np.sum(weights, axis=1)

    df["knn_weighted_price"] = weighted_price

    df["knn_weighted_price_per_square_meter"] = df["knn_weighted_price"] / df["area"]

    return df, coords, tree


def compute_ratio_knn(
    df_primary,
    df_secondary,
    coords_primary,
    tree_secondary,
    k,
):
    dist, ind = tree_secondary.query(coords_primary, k=k)

    secondary_vals = df_secondary["knn_weighted_price"].values[ind]
    secondary_mean = secondary_vals.mean(axis=1)

    primary_vals = df_primary["knn_weighted_price"].values

    ratio = primary_vals / secondary_mean

    df_primary["primary_to_secondary_price_ratio"] = ratio

    return df_primary
