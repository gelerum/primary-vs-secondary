import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

EARTH_RADIUS = 6371000


def compute_wnir_for_market(
    df: pd.DataFrame,
    tree: BallTree,
    R: float,
    h: float,
    price_col: str = "price_per_square_meter_normalized",
) -> pd.DataFrame:
    df = df.copy()
    output_col = f"wnir_{R}"
    count_col = f"wnir_neighbours_count_{R}"

    coord_counts = df.groupby(["latitude", "longitude"])["latitude"].transform("count")
    mask = coord_counts < 5000

    if not mask.any():
        df[output_col] = np.nan
        df[count_col] = 0
        return df

    df_clear = df.loc[mask]
    coords_rad = np.radians(
        df_clear[["latitude", "longitude"]].values.astype(np.float32)
    )
    values = df_clear[price_col].values.astype(np.float32)

    radius_rad = R / EARTH_RADIUS

    indices_array, dists_array_rad = tree.query_radius(
        coords_rad, r=radius_rad, return_distance=True, sort_results=True
    )

    n = len(df_clear)
    wnir = np.full(n, np.nan, dtype=np.float32)
    neighbor_counts = np.zeros(n, dtype=np.int32)

    for i, (inds, dists_rad) in enumerate(zip(indices_array, dists_array_rad)):
        if len(inds) <= 1:
            continue

        valid_inds = inds[1:]
        dists_m = (dists_rad[1:] * EARTH_RADIUS).astype(np.float32)

        if len(dists_m) == 0:
            continue

        neighbor_vals = values[valid_inds]
        neighbor_counts[i] = len(dists_m)

        weights = np.exp(-dists_m / h)
        wnir[i] = np.dot(weights, neighbor_vals) / weights.sum()

    df[output_col] = np.nan
    df[count_col] = -1

    df.loc[mask, output_col] = wnir
    df.loc[mask, count_col] = neighbor_counts

    return df


def compute_ratio_wnir(
    df_primary: pd.DataFrame,
    df_secondary: pd.DataFrame,
    tree_primary: BallTree,
    tree_secondary: BallTree,
    R: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    wnir_col = f"wnir_{R}"

    def _vectorized_calculate_stats(
        df_target: pd.DataFrame,
        df_context: pd.DataFrame,
        tree_context: BallTree,
    ) -> pd.DataFrame:
        ratio_col = f"wnir_ratio_mean_{R}"
        context_cols = {
            "mean": f"wnir_context_mean_{R}",
            "std": f"wnir_context_std_{R}",
            "min": f"wnir_context_min_{R}",
            "max": f"wnir_context_max_{R}",
        }

        coords_target = np.radians(
            df_target[["latitude", "longitude"]].values.astype(np.float32)
        )
        values_context = df_context[wnir_col].astype("float32")
        radius_rad = R / EARTH_RADIUS

        indices_array = tree_context.query_radius(
            coords_target, r=radius_rad, return_distance=False
        )

        target_indices = np.repeat(
            df_target.index, [len(inds) for inds in indices_array]
        )
        context_indices = np.concatenate(indices_array)

        long_df = pd.DataFrame(
            {
                "target_idx": target_indices,
                "context_original_idx": df_context.index[context_indices],
            }
        )

        long_df = long_df.join(
            values_context.rename("context_wnir"), on="context_original_idx"
        )

        if long_df.empty:
            empty_data = {
                col: np.nan for col in list(context_cols.values()) + [ratio_col]
            }
            return pd.DataFrame(empty_data, index=df_target.index)

        stats = long_df.groupby("target_idx")["context_wnir"].agg(
            ["mean", "std", "min", "max"]
        )
        stats = stats.rename(columns=context_cols)
        stats[context_cols["std"]] = stats[context_cols["std"]].fillna(0)

        results = df_target.join(stats)

        target_val = results[wnir_col]
        context_mean = results[context_cols["mean"]]
        results[ratio_col] = np.divide(
            target_val, context_mean, where=context_mean != 0, dtype=np.float32
        )

        return results[list(context_cols.values()) + [ratio_col]]

    new_cols_primary = _vectorized_calculate_stats(
        df_primary, df_secondary, tree_secondary
    )

    new_cols_secondary = _vectorized_calculate_stats(
        df_secondary, df_primary, tree_primary
    )

    return new_cols_primary, new_cols_secondary
