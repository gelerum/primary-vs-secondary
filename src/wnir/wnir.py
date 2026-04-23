import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

EARTH_RADIUS = 6371000  # meters

def get_all_types_of_wnir(
        df: pd.DataFrame,
        r: float,
        h: float,
        price_col: str = 'price_per_square_meter_normalized'
) -> pd.DataFrame:

    df_primary = df[df["market_type"] == "primary"].copy()
    df_secondary = df[df["market_type"] == "secondary"].copy()

    df_primary = compute_wnir(df_primary, r, h, price_col=price_col)
    df_primary = compute_wnir(df_primary, r, h, df_secondary=df_secondary, price_col=price_col)
    df_primary = compute_wnir_ratio(df_primary, r)

    df_wnir = pd.concat([df_primary, df_secondary])

    return df_wnir

def compute_wnir(
        df_primary: pd.DataFrame,
        r: float,
        h: float,
        df_secondary: pd.DataFrame = None,
        price_col: str = 'price_per_square_meter_normalized'
) -> pd.DataFrame:

    if df_secondary is None:
        output_col = f'wnir_primary_{r}'
        count_col = f'neighbours_count_primary_{r}'

        coords_target = np.radians(df_primary[['latitude', 'longitude']].values)
        values_target = df_primary[price_col].values

        tree = BallTree(coords_target, metric='haversine')
        indices = tree.query_radius(coords_target, r=r / EARTH_RADIUS, return_distance=False)

        mode = 1
    else:
        output_col = f'wnir_secondary_{r}'
        count_col = f'neighbours_count_secondary_{r}'

        coords_target = np.radians(df_primary[['latitude', 'longitude']].values)
        coords_source = np.radians(df_secondary[['latitude', 'longitude']].values)

        values_source = df_secondary[price_col].values

        tree = BallTree(coords_source, metric='haversine')
        indices = tree.query_radius(coords_target, r=r/EARTH_RADIUS, return_distance=False)

        mode = 0

    n = len(df_primary)

    wnir = np.full(n, np.nan)
    neighbor_counts = np.zeros(n)

    for i in range(n):
        inds = indices[i]

        if len(inds) == 0:
            continue

        center = coords_target[i]

        if mode:
            neighbors = coords_target[inds]
            vals = values_target[inds]
        else:
            neighbors = coords_source[inds]
            vals = values_source[inds]

        dlat = neighbors[:, 0] - center[0]
        dlon = neighbors[:, 1] - center[1]

        a = np.sin(dlat / 2) ** 2 + np.cos(center[0]) * np.cos(neighbors[:, 0]) * np.sin(dlon / 2) ** 2
        dists = 2 * EARTH_RADIUS * np.arcsin(np.sqrt(a))

        if mode_self:
            mask = dists > 0
            if not mask.any():
                continue
            dists = dists[mask]
            vals = vals[mask]

        if len(dists) == 0:
            continue

        weights = np.exp(-dists / h)

        wnir[i] = np.dot(weights, vals) / weights.sum()
        neighbor_counts[i] = len(dists)

    df_primary[output_col] = wnir
    df_primary[count_col] = neighbor_counts

    return df_primary

def compute_wnir_ratio(
        df_primary: pd.DataFrame,
        r: int
) -> pd.DataFrame:
    df_primary[f'wnir_ratio_{r}'] = df_primary[f'wnir_primary_{r}']/df_primary[f'wnir_secondary_{r}']

    return df_primary


def compute_wnir_for_market(
    df: pd.DataFrame,
    R: float,
    h: float,
    price_col: str = "price_per_square_meter_normalized",
    leaf_size: int = 40,
) -> pd.DataFrame:
    """
    Compute Weighted Nearest Item Ratio (WNIR) for a market (primary or secondary).
    - Skips coordinate groups with >= 5000 identical locations.
    - Rebuilds BallTree on filtered data to guarantee index alignment.
    """
    df = df.copy()
    output_col = f"wnir_{R}"
    count_col = f"wnir_neighbours_count_{R}"

    # Count duplicates per exact coordinate
    coord_counts = df.groupby(["latitude", "longitude"])["latitude"].transform("count")
    mask = coord_counts < 5000

    if not mask.any():
        df[output_col] = np.nan
        df[count_col] = 0
        return df

    df_clear = df.loc[mask].copy()

    # Build coordinates and tree ONLY on clear data
    coords_rad = np.radians(
        df_clear[["latitude", "longitude"]].values.astype(np.float32)
    )
    tree = BallTree(coords_rad, metric="haversine", leaf_size=leaf_size)

    values = df_clear[price_col].values.astype(np.float32)

    radius_rad = R / EARTH_RADIUS

    # Query all points at once (returns list of arrays)
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

        # Exponential kernel weights
        weights = np.exp(-dists_m / h)
        weight_sum = weights.sum()

        if weight_sum > 0:
            wnir[i] = np.dot(weights, neighbor_vals) / weight_sum

    # Write results back
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
    """
    Compute context statistics and ratio between primary and secondary markets.
    """
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
        values_context = df_context[wnir_col].astype(np.float32).values

        radius_rad = R / EARTH_RADIUS

        indices_array = tree_context.query_radius(
            coords_target, r=radius_rad, return_distance=False
        )

        if len(indices_array) == 0 or all(len(inds) == 0 for inds in indices_array):
            empty = {col: np.nan for col in list(context_cols.values()) + [ratio_col]}
            return pd.DataFrame(empty, index=df_target.index)

        # Build long format for groupby
        target_indices = np.repeat(
            df_target.index.to_numpy(), [len(inds) for inds in indices_array]
        )
        context_indices = np.concatenate(indices_array)

        long_df = pd.DataFrame(
            {
                "target_idx": target_indices,
                "context_original_idx": df_context.index[context_indices],
            }
        )

        long_df = long_df.join(
            pd.Series(values_context, index=df_context.index, name="context_wnir"),
            on="context_original_idx",
        )

        # Aggregate statistics
        stats = long_df.groupby("target_idx")["context_wnir"].agg(
            ["mean", "std", "min", "max"]
        )
        stats = stats.rename(columns=context_cols)
        stats[context_cols["std"]] = stats[context_cols["std"]].fillna(0.0)

        # Join back to target
        results = df_target.join(stats)

        # Compute ratio (safe division)
        target_val = results[wnir_col].values
        context_mean = results[context_cols["mean"]].values
        ratio = np.divide(
            target_val,
            context_mean,
            out=np.full_like(target_val, np.nan, dtype=np.float32),
            where=context_mean != 0,
        )
        results[ratio_col] = ratio

        return results[list(context_cols.values()) + [ratio_col]]

    # Compute both directions
    new_cols_primary = _vectorized_calculate_stats(
        df_primary, df_secondary, tree_secondary
    )
    new_cols_secondary = _vectorized_calculate_stats(
        df_secondary, df_primary, tree_primary
    )

    return new_cols_primary, new_cols_secondary
