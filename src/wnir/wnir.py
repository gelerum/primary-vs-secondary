# src/wnir/wnir.py

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree
from tqdm import tqdm
import gc

EARTH_RADIUS = 6371000.0  # meters


def _calculate_wnir_for_batch(
    query_coords_rad: np.ndarray,
    history_tree: BallTree,
    history_values: np.ndarray,
    R: float,
    h: float,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Вспомогательная функция для расчета WNIR для одного пакета (query)
    на основе исторического дерева (history).
    """
    radius_rad = R / EARTH_RADIUS

    # Находим соседей для точек из пакета в историческом дереве
    indices_array, dists_array_rad = history_tree.query_radius(
        query_coords_rad, r=radius_rad, return_distance=True, sort_results=True
    )

    n_query = len(query_coords_rad)
    wnir = np.full(n_query, np.nan, dtype=np.float32)
    neighbor_counts = np.zeros(n_query, dtype=np.int32)

    for i, (inds, dists_rad) in enumerate(zip(indices_array, dists_array_rad)):
        # У нас нет "себя" в историческом дереве, поэтому не нужно отбрасывать inds[0]
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


def _calculate_context_stats_for_batch(
    query_coords_rad: np.ndarray,
    context_history_tree: BallTree,
    context_history_values: np.ndarray,
    R: float,
) -> pd.DataFrame:
    """
    Расчет контекстных статистик (среднее, std и т.д.) для пакета точек
    на основе исторического дерева контекстного рынка.
    """
    radius_rad = R / EARTH_RADIUS

    indices_array = context_history_tree.query_radius(
        query_coords_rad, r=radius_rad, return_distance=False
    )

    n_query = len(query_coords_rad)
    means = np.full(n_query, np.nan, dtype=np.float32)
    stds = np.full(n_query, np.nan, dtype=np.float32)
    mins = np.full(n_query, np.nan, dtype=np.float32)
    maxs = np.full(n_query, np.nan, dtype=np.float32)

    for i, inds in enumerate(indices_array):
        if len(inds) > 0:
            neighbor_vals = context_history_values[inds]
            means[i] = np.nanmean(neighbor_vals)
            if len(inds) > 1:
                stds[i] = np.nanstd(neighbor_vals)
            else:
                stds[i] = 0.0
            mins[i] = np.nanmin(neighbor_vals)
            maxs[i] = np.nanmax(neighbor_vals)

    return pd.DataFrame(
        {
            f"wnir_context_mean_{R}": means,
            f"wnir_context_std_{R}": stds,
            f"wnir_context_min_{R}": mins,
            f"wnir_context_max_{R}": maxs,
        }
    )


def process_markets_in_batches(
    df: pd.DataFrame,  # Передаем ОБЩИЙ отсортированный по дате датасет
    R: float,
    h: float,
    batch_size: int,
    price_col: str = "price_per_square_meter_normalized",
) -> tuple[pd.DataFrame, pd.DataFrame]:

    wnir_col = f"wnir_{R}"
    count_col = f"wnir_neighbours_count_{R}"
    ratio_col = f"wnir_ratio_mean_{R}"

    # Создаем маски для разделения результатов
    is_primary = df["market_type"] == "primary"
    results_p = pd.DataFrame(index=df[is_primary].index)
    results_s = pd.DataFrame(index=df[~is_primary].index)

    history_p = {
        "coords_rad": np.empty((0, 2), dtype=np.float32),
        "values": np.empty(0, dtype=np.float32),
    }
    history_s = {
        "coords_rad": np.empty((0, 2), dtype=np.float32),
        "values": np.empty(0, dtype=np.float32),
    }
    tree_p, tree_s = None, None

    # Идем по ОБЩЕМУ таймлайну
    for i in tqdm(range(0, len(df), batch_size), desc="Processing batches"):
        # 1. Берем общий батч (строго один временной отрезок)
        batch = df.iloc[i : i + batch_size]

        # 2. Разделяем батч на рынки
        batch_p = batch[batch["market_type"] == "primary"]
        batch_s = batch[batch["market_type"] == "secondary"]

        # 3. Обработка первички (используем ТЕКУЩИЕ деревья)
        # 2. Обработка пакета первичного рынка
        if not batch_p.empty:
            coords_rad_p = np.radians(
                batch_p[["latitude", "longitude"]].values.astype(np.float32)
            )

            # Собственный рынок (WNIR)
            if tree_p:
                wnir, counts = _calculate_wnir_for_batch(
                    coords_rad_p, tree_p, history_p["values"], R, h
                )
                results_p.loc[batch_p.index, wnir_col] = wnir
                results_p.loc[batch_p.index, count_col] = counts

            # Контекстный рынок (Статистики)
            if tree_s:
                stats_df = _calculate_context_stats_for_batch(
                    coords_rad_p, tree_s, history_s["values"], R
                )
                # ИСПРАВЛЕНИЕ: Вместо join используем loc для записи данных батча в общую таблицу
                results_p.loc[batch_p.index, stats_df.columns] = stats_df.values

        # 3. Обработка пакета вторичного рынка
        if not batch_s.empty:
            coords_rad_s = np.radians(
                batch_s[["latitude", "longitude"]].values.astype(np.float32)
            )

            # Собственный рынок (WNIR)
            if tree_s:
                wnir, counts = _calculate_wnir_for_batch(
                    coords_rad_s, tree_s, history_s["values"], R, h
                )
                results_s.loc[batch_s.index, wnir_col] = wnir
                results_s.loc[batch_s.index, count_col] = counts

            # Контекстный рынок (Статистики)
            if tree_p:
                stats_df = _calculate_context_stats_for_batch(
                    coords_rad_s, tree_p, history_p["values"], R
                )
                # ИСПРАВЛЕНИЕ: То же самое для вторичного рынка
                results_s.loc[batch_s.index, stats_df.columns] = stats_df.values

        # 5. ТОЛЬКО ТЕПЕРЬ обновляем историю (чтобы батч не предсказывал сам себя)
        if not batch_p.empty:
            values_p = batch_p[price_col].values.astype(np.float32)
            history_p["coords_rad"] = np.vstack([history_p["coords_rad"], coords_rad_p])
            history_p["values"] = np.concatenate([history_p["values"], values_p])
            tree_p = BallTree(history_p["coords_rad"], metric="haversine")

        if not batch_s.empty:
            values_s = batch_s[price_col].values.astype(np.float32)
            history_s["coords_rad"] = np.vstack([history_s["coords_rad"], coords_rad_s])
            history_s["values"] = np.concatenate([history_s["values"], values_s])
            tree_s = BallTree(history_s["coords_rad"], metric="haversine")

        gc.collect()
    # 5. Вычисляем итоговое отношение WNIR
    # Это делается в конце, когда все WNIR и контекстные средние посчитаны
    for df_res, market in [(results_p, "primary"), (results_s, "secondary")]:
        if not df_res.empty:
            target_val = df_res[wnir_col].values
            context_mean = df_res[f"wnir_context_mean_{R}"].values

            # Безопасное деление
            ratio = np.divide(
                target_val,
                context_mean,
                out=np.full_like(target_val, np.nan, dtype=np.float32),
                where=(context_mean != 0) & ~np.isnan(context_mean),
            )
            df_res[ratio_col] = ratio

    # Заполняем пропуски в служебных колонках, чтобы типы данных были корректны
    for col in [count_col, f"wnir_context_std_{R}"]:
        if col in results_p.columns:
            results_p[col] = results_p[col].fillna(0)
        if col in results_s.columns:
            results_s[col] = results_s[col].fillna(0)

    return results_p, results_s
