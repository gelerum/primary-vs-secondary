import gc
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree
from tqdm import tqdm

EARTH_RADIUS = 6371000.0  # meters


def _calculate_wnir_for_batch(
    query_coords_rad: np.ndarray,
    history_tree: BallTree,
    history_values: np.ndarray,
    R: float,
    h: float,
) -> tuple[np.ndarray, np.ndarray]:
    radius_rad = R / EARTH_RADIUS
    indices_array, dists_array_rad = history_tree.query_radius(
        query_coords_rad, r=radius_rad, return_distance=True, sort_results=True
    )

    n_query = len(query_coords_rad)
    wnir = np.full(n_query, np.nan, dtype=np.float32)
    neighbor_counts = np.zeros(n_query, dtype=np.int32)

    for i, (inds, dists_rad) in enumerate(zip(indices_array, dists_array_rad)):
        if len(inds) == 0:
            continue

        dists_m = (dists_rad * EARTH_RADIUS).astype(np.float32)
        neighbor_vals = history_values[inds]
        neighbor_counts[i] = len(dists_m)

        weights = np.exp(-dists_m / h)
        weight_sum = weights.sum()

        if weight_sum > 0:
            wnir[i] = np.dot(weights, neighbor_vals) / weight_sum

    return wnir, neighbor_counts


def _calculate_extended_context_stats(coords_rad, tree, values, R):
    radius_rad = R / EARTH_RADIUS
    indices = tree.query_radius(coords_rad, r=radius_rad)

    n = len(coords_rad)
    res = {
        "mean": np.full(n, np.nan),
        "std": np.full(n, np.nan),
        "min": np.full(n, np.nan),
        "max": np.full(n, np.nan),
        "median": np.full(n, np.nan),
        "count": np.zeros(n),
    }

    for i, inds in enumerate(indices):
        if len(inds) > 0:
            v = values[inds]
            res["count"][i] = len(inds)
            res["mean"][i] = np.mean(v)
            res["min"][i] = np.min(v)
            res["max"][i] = np.max(v)
            res["median"][i] = np.median(v)
            if len(inds) > 1:
                res["std"][i] = np.std(v)
            else:
                res["std"][i] = 0.0

    return pd.DataFrame(res)


def process_markets_in_batches(
    df: pd.DataFrame,
    Rs: list[float],
    h: float,
    batch_size: int,
    price_col: str = "price_per_square_meter_normalized",
) -> pd.DataFrame:

    is_primary_mask = df["market_type"] == "primary"

    # Подготавливаем список всех колонок согласно запросу
    cols = []
    for r in Rs:
        cols.append(f"wnir_p_value_{r}")
        cols.append(f"wnir_s_value_{r}")  # Новая колонка
        cols.extend(
            [
                f"wnir_s_mean_{r}",
                f"wnir_s_std_{r}",
                f"wnir_s_min_{r}",
                f"wnir_s_max_{r}",
                f"wnir_s_median_{r}",
                f"wnir_s_count_{r}",
            ]
        )

    final_results = pd.DataFrame(
        index=df[is_primary_mask].index, columns=cols, dtype=np.float32
    )

    history_p = {
        "coords": np.empty((0, 2), dtype=np.float32),
        "vals": np.empty(0, dtype=np.float32),
    }
    history_s = {
        "coords": np.empty((0, 2), dtype=np.float32),
        "vals": np.empty(0, dtype=np.float32),
    }
    tree_p, tree_s = None, None

    for i in tqdm(range(0, len(df), batch_size), desc="Processing timeline"):
        batch = df.iloc[i : i + batch_size]
        batch_coords = np.radians(
            batch[["latitude", "longitude"]].values.astype(np.float32)
        )
        batch_p_mask = (batch["market_type"] == "primary").values

        query_coords_p = batch_coords[batch_p_mask]
        query_indices_p = batch.index[batch_p_mask]

        if len(query_coords_p) > 0:
            for r in Rs:
                # 1. Считаем по СВОЕМУ рынку (Primary -> Primary)
                if tree_p:
                    wnir_p, _ = _calculate_wnir_for_batch(
                        query_coords_p, tree_p, history_p["vals"], r, h
                    )
                    final_results.loc[query_indices_p, f"wnir_p_value_{r}"] = wnir_p

                # 2. Считаем по ВТОРИЧНОМУ рынку (Primary -> Secondary)
                if tree_s:
                    # Считаем WNIR (взвешенное значение)
                    wnir_s, _ = _calculate_wnir_for_batch(
                        query_coords_p, tree_s, history_s["vals"], r, h
                    )
                    final_results.loc[query_indices_p, f"wnir_s_value_{r}"] = wnir_s

                    # Считаем остальные статистики
                    stats_s = _calculate_extended_context_stats(
                        query_coords_p, tree_s, history_s["vals"], r
                    )
                    stats_s.columns = [
                        f"wnir_s_{c}_{r}"
                        for c in ["mean", "std", "min", "max", "median", "count"]
                    ]
                    final_results.loc[query_indices_p, stats_s.columns] = stats_s.values

        # Обновление истории
        p_in_batch = batch[batch["market_type"] == "primary"]
        s_in_batch = batch[batch["market_type"] == "secondary"]

        if not p_in_batch.empty:
            history_p["coords"] = np.vstack(
                [history_p["coords"], batch_coords[batch_p_mask]]
            )
            history_p["vals"] = np.concatenate(
                [history_p["vals"], p_in_batch[price_col].values]
            )
            tree_p = BallTree(history_p["coords"], metric="haversine")

        if not s_in_batch.empty:
            history_s["coords"] = np.vstack(
                [history_s["coords"], batch_coords[~batch_p_mask]]
            )
            history_s["vals"] = np.concatenate(
                [history_s["vals"], s_in_batch[price_col].values]
            )
            tree_s = BallTree(history_s["coords"], metric="haversine")

    del history_p, history_s, tree_p, tree_s
    gc.collect()

    return final_results
