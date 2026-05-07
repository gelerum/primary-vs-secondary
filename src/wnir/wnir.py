import torch
import numpy as np
import pandas as pd
from tqdm import tqdm
import gc

import math

EARTH_RADIUS = 6371000.0


def get_nearest_train_indices_gpu(
    target_coords_rad: torch.Tensor,
    source_coords_rad: torch.Tensor,
    max_distance_meters: float = 5000.0,  # <-- ДОБАВИЛИ ПОРОГ (например, 5 км)
    batch_size: int = 2048,
    device: str = "cuda",
):
    """
    Возвращает индексы ближайших точек и маску валидности
    (True, если сосед найден в пределах max_distance_meters).
    """
    n_targets = len(target_coords_rad)
    nearest_indices = torch.zeros(n_targets, dtype=torch.long, device=device)
    valid_mask = torch.zeros(n_targets, dtype=torch.bool, device=device)

    # Переводим метры в базис 'a', который мы считаем внутри функции,
    # чтобы не считать арксинусы и корни для всей матрицы
    # a = sin^2(d / 2R)
    a_threshold = math.sin(max_distance_meters / (2.0 * EARTH_RADIUS)) ** 2

    h_lat = source_coords_rad[:, 0].unsqueeze(0)
    h_lon = source_coords_rad[:, 1].unsqueeze(0)

    for i in range(0, n_targets, batch_size):
        q_chunk = target_coords_rad[i : i + batch_size]
        q_lat = q_chunk[:, 0].unsqueeze(1)
        q_lon = q_chunk[:, 1].unsqueeze(1)

        dlat = torch.sub(h_lat, q_lat).mul_(0.5).sin_().pow_(2)
        dlon = torch.sub(h_lon, q_lon).mul_(0.5).sin_().pow_(2)
        dlon.mul_(torch.cos(q_lat)).mul_(torch.cos(h_lat))

        a = dlat.add_(dlon)

        # Вместо argmin берем min, чтобы получить и индекс, и само расстояние (в виде 'a')
        min_a, min_idx = torch.min(a, dim=1)

        nearest_indices[i : i + batch_size] = min_idx
        valid_mask[i : i + batch_size] = min_a <= a_threshold

        del dlat, dlon, a, q_chunk, q_lat, q_lon, min_a, min_idx

    return nearest_indices, valid_mask


def calculate_and_impute_wnir(
    df_group: pd.DataFrame,
    Rs: list,
    h: float,
    batch_size: int,
    suffix: str,
    device: torch.device,
    fill_nearest_threshold,
) -> pd.DataFrame:
    """
    Выполняет расчет WNIR для переданного куска данных (df_group),
    переименовывает колонки (добавляя suffix) и заполняет пропуски.
    Возвращает DataFrame только для Primary рынка с новыми колонками.
    """
    print(f"\n--- Processing WNIR for: {suffix} ---")

    # 1. Считаем фичи
    df_results = process_markets_in_batches(df_group, Rs=Rs, h=h, batch_size=batch_size)

    # Переименовываем колонки, добавляя постфикс (_all или _clusterName)
    rename_dict = {col: f"{col}_{suffix}" for col in df_results.columns}
    df_results = df_results.rename(columns=rename_dict)
    new_cols = list(df_results.columns)

    # 2. Выделяем первичку для этой группы
    df_group_primary = df_group[df_group["market_type"] == "primary"].copy()

    # Мержим результаты по индексам (индексы сохранены из оригинального датафрейма)
    for col in new_cols:
        df_group_primary[col] = df_results[col]

    del df_results
    gc.collect()

    # 3. Заполняем пропуски на GPU
    print(f"Filling missing values for {suffix}...")
    coords_np = df_group_primary[["latitude", "longitude"]].values.astype(np.float32)
    coords_tensor = torch.from_numpy(coords_np).to(device) * (torch.pi / 180.0)

    for r in Rs:
        r_str = str(int(r)) if float(r).is_integer() else str(r)

        # Названия колонок с учетом постфикса
        price_cols = [
            f"wnir_p_value_{r_str}_{suffix}",
            f"wnir_s_value_{r_str}_{suffix}",
            f"wnir_s_mean_{r_str}_{suffix}",
            f"wnir_s_min_{r_str}_{suffix}",
            f"wnir_s_max_{r_str}_{suffix}",
            f"wnir_s_median_{r_str}_{suffix}",
        ]
        count_col = f"wnir_s_count_{r_str}_{suffix}"
        std_col = f"wnir_s_std_{r_str}_{suffix}"

        # Заполняем count и std нулями
        if count_col in df_group_primary.columns:
            df_group_primary[count_col] = df_group_primary[count_col].fillna(0)
        if std_col in df_group_primary.columns:
            df_group_primary[std_col] = df_group_primary[std_col].fillna(0)

        existing_price_cols = [
            col for col in price_cols if col in df_group_primary.columns
        ]
        if not existing_price_cols:
            continue

        base_col = existing_price_cols[0]

        # Маски для Nearest Neighbor
        source_mask = (df_group_primary["set_type"] == "train") & (
            df_group_primary[base_col].notna()
        )
        target_mask = (df_group_primary["set_type"].isin(["valid", "test"])) & (
            df_group_primary[base_col].isna()
        )

        if source_mask.any() and target_mask.any():
            source_idx = np.where(source_mask)[0]
            target_idx = np.where(target_mask)[0]

            source_coords = coords_tensor[source_idx]
            target_coords = coords_tensor[target_idx]

            with torch.no_grad():
                # Указываем максимальное расстояние, например 10000 метров (10 км)
                nearest_relative_indices, valid_mask = get_nearest_train_indices_gpu(
                    target_coords,
                    source_coords,
                    max_distance_meters=fill_nearest_threshold,
                    device=device,
                )

            # Оставляем только те точки valid/test, для которых нашелся БЛИЗКИЙ сосед
            valid_mask_cpu = valid_mask.cpu().numpy()

            # Фильтруем индексы Target (реципиентов) и Source (доноров)
            target_idx_filtered = target_idx[valid_mask_cpu]
            nearest_relative_indices_filtered = nearest_relative_indices.cpu().numpy()[
                valid_mask_cpu
            ]
            nearest_absolute_indices = source_idx[nearest_relative_indices_filtered]

            # Копируем фичи ТОЛЬКО для тех, кто прошел проверку по дистанции
            if len(target_idx_filtered) > 0:
                df_group_primary.loc[
                    df_group_primary.index[target_idx_filtered], existing_price_cols
                ] = df_group_primary.loc[
                    df_group_primary.index[nearest_absolute_indices],
                    existing_price_cols,
                ].values

        # Fallback: заполняем оставшиеся пропуски (в самом train) средним по train этого кластера
        for col in existing_price_cols:
            train_mean = df_group_primary[df_group_primary["set_type"] == "train"][
                col
            ].mean()
            # Если train_mean = NaN (например в кластере вообще нет трейна), оставляем NaN или можно заполнить 0
            df_group_primary[col] = df_group_primary[col].fillna(train_mean)

    del coords_tensor
    torch.cuda.empty_cache()
    gc.collect()

    # Возвращаем только новые сгенерированные колонки, чтобы присоединить их к глобальному датафрейму
    return df_group_primary[new_cols]


def haversine_distance(
    query_coords_rad: torch.Tensor, hist_coords_rad: torch.Tensor
) -> torch.Tensor:
    """
    Вычисляет расстояние (в метрах) между точками.
    ВАЖНО: Координаты на входе УЖЕ должны быть в радианах!
    """
    q_lat = query_coords_rad[:, 0].unsqueeze(1)  # (N, 1)
    q_lon = query_coords_rad[:, 1].unsqueeze(1)
    h_lat = hist_coords_rad[:, 0].unsqueeze(0)  # (1, M)
    h_lon = hist_coords_rad[:, 1].unsqueeze(0)

    dlat = torch.sub(h_lat, q_lat)
    dlat.mul_(0.5).sin_().pow_(2)

    dlon = torch.sub(h_lon, q_lon)
    dlon.mul_(0.5).sin_().pow_(2)

    dlon.mul_(torch.cos(q_lat)).mul_(torch.cos(h_lat))

    a = dlat.add_(dlon)
    del dlon

    a.clamp_(0.0, 1.0)
    a.sqrt_().asin_().mul_(2.0 * EARTH_RADIUS)

    return a


@torch.no_grad()
def process_markets_in_batches(
    df: pd.DataFrame,
    Rs: list[float],
    h: float,
    batch_size: int = 16000,
    price_col: str = "price_per_square_meter_normalized",
    device: str = "cuda",
) -> pd.DataFrame:

    device = torch.device(device)
    torch.cuda.empty_cache()

    Rs = torch.tensor(Rs, device=device, dtype=torch.float32)
    h_tensor = torch.tensor(h, device=device, dtype=torch.float32)

    primary_mask = df["market_type"] == "primary"
    primary_indices = df.index[primary_mask].copy()

    col_list = []
    for r in Rs.cpu().tolist():
        r_str = str(int(r)) if float(r).is_integer() else str(r)
        col_list.extend(
            [
                f"wnir_p_value_{r_str}",
                f"wnir_s_value_{r_str}",
                f"wnir_s_mean_{r_str}",
                f"wnir_s_std_{r_str}",
                f"wnir_s_min_{r_str}",
                f"wnir_s_max_{r_str}",
                f"wnir_s_median_{r_str}",
                f"wnir_s_count_{r_str}",
            ]
        )

    results = pd.DataFrame(index=primary_indices, columns=col_list, dtype=np.float32)

    # История теперь включает и индексы из оригинального датафрейма для защиты от утечек
    hist_p_coord, hist_p_val, hist_p_idx = None, None, None
    hist_s_coord, hist_s_val, hist_s_idx = None, None, None

    sub_batch_size = 128

    for start in tqdm(range(0, len(df), batch_size), desc="WNIR GPU"):
        batch = df.iloc[start : start + batch_size]

        # ИСПРАВЛЕНИЕ: Конвертируем градусы в радианы прямо здесь!
        coords_np = batch[["latitude", "longitude"]].values.astype(np.float32)
        coords = torch.from_numpy(coords_np).to(device) * (torch.pi / 180.0)

        is_primary = batch["market_type"].values == "primary"
        query_mask = torch.from_numpy(is_primary).to(device)
        curr_idx = torch.from_numpy(batch.index.values).to(
            device
        )  # Абсолютные индексы строк (время)

        # 1. Извлекаем данные текущего батча
        p_coords = coords[query_mask]
        p_vals = torch.from_numpy(
            batch.loc[is_primary, price_col].values.astype(np.float32)
        ).to(device)
        p_idx = curr_idx[query_mask]

        s_coords = coords[~query_mask]
        s_vals = torch.from_numpy(
            batch.loc[~is_primary, price_col].values.astype(np.float32)
        ).to(device)
        s_idx = curr_idx[~query_mask]

        # 2. ИСПРАВЛЕНИЕ ЛОГИКИ: Добавляем текущий батч в историю ДО расчетов
        if len(p_coords) > 0:
            hist_p_coord = (
                p_coords
                if hist_p_coord is None
                else torch.cat([hist_p_coord, p_coords])
            )
            hist_p_val = (
                p_vals if hist_p_val is None else torch.cat([hist_p_val, p_vals])
            )
            hist_p_idx = p_idx if hist_p_idx is None else torch.cat([hist_p_idx, p_idx])

        if len(s_coords) > 0:
            hist_s_coord = (
                s_coords
                if hist_s_coord is None
                else torch.cat([hist_s_coord, s_coords])
            )
            hist_s_val = (
                s_vals if hist_s_val is None else torch.cat([hist_s_val, s_vals])
            )
            hist_s_idx = s_idx if hist_s_idx is None else torch.cat([hist_s_idx, s_idx])

        # 3. Обработка запросов (первичка)
        if len(p_coords) > 0:
            for i in range(0, len(p_coords), sub_batch_size):
                sub_qc = p_coords[i : i + sub_batch_size]
                sub_idx = p_idx[i : i + sub_batch_size]
                max_idx = sub_idx.max()

                # Primary market
                if hist_p_coord is not None:
                    # Оптимизация: берем историю только до максимального индекса текущего саб-батча
                    valid_p_mask = hist_p_idx < max_idx
                    h_p_c = hist_p_coord[valid_p_mask]
                    if len(h_p_c) > 0:
                        dists = haversine_distance(sub_qc, h_p_c)
                        h_p_v = hist_p_val[valid_p_mask]
                        h_p_i = hist_p_idx[valid_p_mask]
                        _compute_features(
                            dists,
                            h_p_v,
                            Rs,
                            h_tensor,
                            results,
                            sub_idx,
                            "p",
                            device,
                            h_p_i,
                        )
                        del dists

                # Secondary market
                if hist_s_coord is not None:
                    valid_s_mask = hist_s_idx < max_idx
                    h_s_c = hist_s_coord[valid_s_mask]
                    if len(h_s_c) > 0:
                        dists = haversine_distance(sub_qc, h_s_c)
                        h_s_v = hist_s_val[valid_s_mask]
                        h_s_i = hist_s_idx[valid_s_mask]
                        _compute_features(
                            dists,
                            h_s_v,
                            Rs,
                            h_tensor,
                            results,
                            sub_idx,
                            "s",
                            device,
                            h_s_i,
                        )
                        del dists

    torch.cuda.empty_cache()
    gc.collect()
    return results


def _compute_features(
    dists: torch.Tensor,
    values: torch.Tensor,
    Rs: torch.Tensor,
    h: torch.Tensor,
    results: pd.DataFrame,
    orig_idx: torch.Tensor,
    market_type: str,
    device,
    hist_idx: torch.Tensor,
):
    # ГАРАНТИЯ ОТ УТЕЧКИ: Строгая маска времени. Точка видит только те точки,
    # чей оригинальный индекс (время) строго меньше ее собственного.
    time_mask = hist_idx.unsqueeze(0) < orig_idx.unsqueeze(1)
    orig_idx_cpu = orig_idx.cpu().numpy()

    exp_dists = torch.exp(-dists / h)
    exp_dists.mul_(time_mask)  # Обнуляем веса для будущего и самой себя

    v = values.unsqueeze(0).expand(len(orig_idx), -1)

    for r_tensor in Rs:
        r = float(r_tensor.item())
        r_str = str(int(r)) if float(r).is_integer() else str(r)

        # Маска радиуса + времени
        mask = (dists <= r) & time_mask
        counts = mask.sum(dim=1, dtype=torch.float32)

        weights = exp_dists * mask
        weight_sum = weights.sum(dim=1)

        weighted_sum = torch.mv(weights, values)

        wnir = torch.where(
            weight_sum > 1e-8,
            weighted_sum / weight_sum,
            torch.tensor(float("nan"), device=device, dtype=torch.float32),
        )

        if market_type == "p":
            results.loc[orig_idx_cpu, f"wnir_p_value_{r_str}"] = wnir.cpu().numpy()
        else:
            results.loc[orig_idx_cpu, f"wnir_s_value_{r_str}"] = wnir.cpu().numpy()

        if market_type == "s":
            # 1. Mean
            sum_v = torch.mv(mask.to(torch.float32), values)
            mean_vals = torch.where(
                counts > 0, sum_v / counts, torch.tensor(float("nan"), device=device)
            )
            results.loc[orig_idx_cpu, f"wnir_s_mean_{r_str}"] = mean_vals.cpu().numpy()

            # 2. Min
            inf_tensor = torch.tensor(float("inf"), device=device)
            masked_for_min = torch.where(mask, v, inf_tensor)
            min_vals = torch.min(masked_for_min, dim=1).values
            results.loc[orig_idx_cpu, f"wnir_s_min_{r_str}"] = (
                torch.where(min_vals == inf_tensor, float("nan"), min_vals)
                .cpu()
                .numpy()
            )
            del masked_for_min

            # 2b. Max
            masked_for_max = torch.where(mask, v, -inf_tensor)
            max_vals = torch.max(masked_for_max, dim=1).values
            results.loc[orig_idx_cpu, f"wnir_s_max_{r_str}"] = (
                torch.where(max_vals == -inf_tensor, float("nan"), max_vals)
                .cpu()
                .numpy()
            )
            del masked_for_max

            # 3. Std
            diff_sq = torch.sub(v, mean_vals.unsqueeze(1))
            diff_sq.square_().mul_(mask)
            sum_diff_sq = diff_sq.sum(dim=1)
            del diff_sq

            var_vals = torch.where(
                counts > 1, sum_diff_sq / (counts - 1), torch.tensor(0.0, device=device)
            )
            results.loc[orig_idx_cpu, f"wnir_s_std_{r_str}"] = (
                var_vals.sqrt().cpu().numpy()
            )

            # 4. Count
            results.loc[orig_idx_cpu, f"wnir_s_count_{r_str}"] = counts.cpu().numpy()

            # 5. Median (ИСПРАВЛЕНИЕ: считаем прямо на GPU через torch.nanmedian)
            masked_for_median = torch.where(
                mask, v, torch.tensor(float("nan"), device=device)
            )
            # Для строк, где все NaN, torch.nanmedian корректно вернет NaN
            medians = torch.nanmedian(masked_for_median, dim=1).values.cpu().numpy()
            results.loc[orig_idx_cpu, f"wnir_s_median_{r_str}"] = medians
            del masked_for_median

        del mask, weights
