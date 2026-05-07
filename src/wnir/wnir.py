import torch
import numpy as np
import pandas as pd
from tqdm import tqdm
import gc

EARTH_RADIUS = 6371000.0


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
