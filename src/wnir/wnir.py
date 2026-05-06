import torch
import numpy as np
import pandas as pd
from tqdm import tqdm
import gc
import warnings

EARTH_RADIUS = 6371000.0


def haversine_distance(
    query_coords: torch.Tensor, hist_coords: torch.Tensor
) -> torch.Tensor:
    """
    Вычисляет расстояние между всеми query-точками и всеми точками истории.
    Используются in-place операции для минимизации потребления VRAM.
    """
    q_lat = query_coords[:, 0].unsqueeze(1)  # (N, 1)
    q_lon = query_coords[:, 1].unsqueeze(1)
    h_lat = hist_coords[:, 0].unsqueeze(0)  # (1, M)
    h_lon = hist_coords[:, 1].unsqueeze(0)

    # In-place вычисления для экономии памяти
    # 1. Считаем dlat: dlat = h_lat - q_lat, затем in-place применяем (sin(dlat * 0.5))**2
    dlat = torch.sub(h_lat, q_lat)
    dlat.mul_(0.5).sin_().pow_(2)

    # 2. Считаем dlon: dlon = h_lon - q_lon, затем in-place (sin(dlon * 0.5))**2
    dlon = torch.sub(h_lon, q_lon)
    dlon.mul_(0.5).sin_().pow_(2)

    # 3. dlon = cos(q_lat) * cos(h_lat) * dlon
    # В in-place умножении используется broadcasting
    dlon.mul_(torch.cos(q_lat)).mul_(torch.cos(h_lat))

    # 4. a = dlat + dlon
    # dlat теперь содержит 'a', память от dlon можно освободить
    a = dlat.add_(dlon)
    del dlon

    # Ограничиваем a от 0 до 1 для защиты от NaN в arcsin из-за погрешностей float
    a.clamp_(0.0, 1.0)

    # 5. c = 2 * arcsin(sqrt(a))
    # Математически эквивалентно 2 * atan2(sqrt(a), sqrt(1-a)), но требует меньше памяти
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

    # Подготовка колонок
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

    # История
    hist_p_coord = None
    hist_p_val = None
    hist_s_coord = None
    hist_s_val = None

    # Радикально снижаем размер саб-батча: 128 гарантирует отсутствие OOM
    # даже если размер истории вырастет до нескольких миллионов точек
    sub_batch_size = 128

    for start in tqdm(range(0, len(df), batch_size), desc="WNIR GPU"):
        batch = df.iloc[start : start + batch_size]
        coords_np = batch[["latitude", "longitude"]].values.astype(np.float32)
        coords = torch.from_numpy(coords_np).to(device)

        is_primary = batch["market_type"].values == "primary"
        query_mask = torch.from_numpy(is_primary).to(device)
        query_coords = coords[query_mask]
        query_orig_idx = batch.index[is_primary]

        if len(query_coords) > 0:
            for i in range(0, len(query_coords), sub_batch_size):
                sub_qc = query_coords[i : i + sub_batch_size]
                sub_idx = query_orig_idx[i : i + sub_batch_size]

                # Primary market
                if hist_p_coord is not None and len(hist_p_coord) > 0:
                    dists = haversine_distance(sub_qc, hist_p_coord)
                    _compute_features(
                        dists, hist_p_val, Rs, h_tensor, results, sub_idx, "p", device
                    )
                    del dists

                # Secondary market
                if hist_s_coord is not None and len(hist_s_coord) > 0:
                    dists = haversine_distance(sub_qc, hist_s_coord)
                    _compute_features(
                        dists, hist_s_val, Rs, h_tensor, results, sub_idx, "s", device
                    )
                    del dists

        # === Обновление истории ===
        p_coords = coords[query_mask]
        p_vals = torch.from_numpy(batch.loc[is_primary, price_col].values.copy()).to(
            device
        )

        s_coords = coords[~query_mask]
        s_vals = torch.from_numpy(batch.loc[~is_primary, price_col].values.copy()).to(
            device
        )

        if len(p_coords) > 0:
            hist_p_coord = (
                p_coords
                if hist_p_coord is None
                else torch.cat([hist_p_coord, p_coords])
            )
            hist_p_val = (
                p_vals if hist_p_val is None else torch.cat([hist_p_val, p_vals])
            )

        if len(s_coords) > 0:
            hist_s_coord = (
                s_coords
                if hist_s_coord is None
                else torch.cat([hist_s_coord, s_coords])
            )
            hist_s_val = (
                s_vals if hist_s_val is None else torch.cat([hist_s_val, s_vals])
            )

    torch.cuda.empty_cache()
    gc.collect()
    return results


def _compute_features(
    dists: torch.Tensor,
    values: torch.Tensor,
    Rs: torch.Tensor,
    h: torch.Tensor,
    results: pd.DataFrame,
    orig_idx,
    market_type: str,
    device="cuda",
):
    exp_dists = torch.exp(-dists / h)
    v = values.unsqueeze(0).expand(len(orig_idx), -1)

    for r_tensor in Rs:
        r = float(r_tensor.item())
        r_str = str(int(r)) if float(r).is_integer() else str(r)

        mask = dists <= r
        counts = mask.sum(dim=1, dtype=torch.float32)

        weights = exp_dists * mask
        weight_sum = weights.sum(dim=1)

        # ОПТИМИЗАЦИЯ: матричное умножение (N, M) @ (M) -> (N).
        # Не создает промежуточную матрицу (N, M), экономит гигабайты памяти
        weighted_sum = torch.mv(weights, values)

        wnir = torch.where(
            weight_sum > 1e-8,
            weighted_sum / weight_sum,
            torch.tensor(float("nan"), device=device, dtype=torch.float32),
        )

        if market_type == "p":
            results.loc[orig_idx, f"wnir_p_value_{r_str}"] = wnir.cpu().numpy()
        else:
            results.loc[orig_idx, f"wnir_s_value_{r_str}"] = wnir.cpu().numpy()

        if market_type == "s":
            # 1. Mean (тоже через torch.mv для экономии памяти)
            sum_v = torch.mv(mask.to(torch.float32), values)
            mean_vals = torch.where(
                counts > 0, sum_v / counts, torch.tensor(float("nan"), device=device)
            )
            results.loc[orig_idx, f"wnir_s_mean_{r_str}"] = mean_vals.cpu().numpy()

            # 2. Min
            inf_tensor = torch.tensor(float("inf"), device=device)
            masked_for_min = torch.where(mask, v, inf_tensor)
            min_vals = torch.min(masked_for_min, dim=1).values
            results.loc[orig_idx, f"wnir_s_min_{r_str}"] = (
                torch.where(min_vals == inf_tensor, float("nan"), min_vals)
                .cpu()
                .numpy()
            )
            del masked_for_min

            # 2b. Max
            masked_for_max = torch.where(mask, v, -inf_tensor)
            max_vals = torch.max(masked_for_max, dim=1).values
            results.loc[orig_idx, f"wnir_s_max_{r_str}"] = (
                torch.where(max_vals == -inf_tensor, float("nan"), max_vals)
                .cpu()
                .numpy()
            )
            del masked_for_max

            # 3. Std (считаем с in-place вычитанием для экономии памяти)
            diff_sq = torch.sub(v, mean_vals.unsqueeze(1))
            diff_sq.square_().mul_(mask)
            sum_diff_sq = diff_sq.sum(dim=1)
            del diff_sq

            var_vals = torch.where(
                counts > 1, sum_diff_sq / (counts - 1), torch.tensor(0.0, device=device)
            )
            results.loc[orig_idx, f"wnir_s_std_{r_str}"] = var_vals.sqrt().cpu().numpy()

            # 4. Count
            results.loc[orig_idx, f"wnir_s_count_{r_str}"] = counts.cpu().numpy()

            # 5. Median
            masked_np = (
                torch.where(mask, v, torch.tensor(float("nan"), device=device))
                .cpu()
                .numpy()
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                medians = np.nanmedian(masked_np, axis=1)
            results.loc[orig_idx, f"wnir_s_median_{r_str}"] = medians
            del masked_np

        del mask, weights
