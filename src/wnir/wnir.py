import torch
import numpy as np
import pandas as pd
from tqdm import tqdm
import gc

EARTH_RADIUS = 6371000.0


def haversine_distance(
    query_coords: torch.Tensor, hist_coords: torch.Tensor
) -> torch.Tensor:
    """
    Вычисляет расстояние между всеми query-точками и всеми точками истории.
    query_coords: (N, 2), hist_coords: (M, 2) -> возвращает (N, M)
    """
    q_lat = query_coords[:, 0].unsqueeze(1)  # (N, 1)
    q_lon = query_coords[:, 1].unsqueeze(1)
    h_lat = hist_coords[:, 0].unsqueeze(0)  # (1, M)
    h_lon = hist_coords[:, 1].unsqueeze(0)

    dlat = h_lat - q_lat
    dlon = h_lon - q_lon

    a = (
        torch.sin(dlat * 0.5) ** 2
        + torch.cos(q_lat) * torch.cos(h_lat) * torch.sin(dlon * 0.5) ** 2
    )
    c = 2 * torch.atan2(torch.sqrt(a), torch.sqrt(1 - a))

    return EARTH_RADIUS * c


@torch.no_grad()
def process_markets_in_batches(
    df: pd.DataFrame,
    Rs: list[float],
    h: float,
    batch_size: int = 16000,  # оптимально для RTX 3060 12GB
    price_col: str = "price_per_square_meter_normalized",
    device: str = "cuda",
) -> pd.DataFrame:

    device = torch.device(device)
    torch.cuda.empty_cache()

    Rs = torch.tensor(Rs, device=device, dtype=torch.float32)
    h = torch.tensor(h, device=device, dtype=torch.float32)

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

    for start in tqdm(range(0, len(df), batch_size), desc="WNIR GPU"):
        batch = df.iloc[start : start + batch_size]
        coords_np = batch[["latitude", "longitude"]].values.astype(np.float32)
        coords = torch.from_numpy(coords_np).to(device)

        is_primary = batch["market_type"].values == "primary"
        query_mask = torch.from_numpy(is_primary).to(device)
        query_coords = coords[query_mask]
        query_orig_idx = batch.index[is_primary]

        if len(query_coords) > 0:
            # Primary market
            if hist_p_coord is not None and len(hist_p_coord) > 0:
                dists = haversine_distance(query_coords, hist_p_coord)
                _compute_features(
                    dists, hist_p_val, Rs, h, results, query_orig_idx, "p"
                )

            # Secondary market
            if hist_s_coord is not None and len(hist_s_coord) > 0:
                dists = haversine_distance(query_coords, hist_s_coord)
                _compute_features(
                    dists, hist_s_val, Rs, h, results, query_orig_idx, "s"
                )

        # === Обновление истории ===
        p_coords = coords[query_mask]
        p_vals = torch.from_numpy(batch.loc[is_primary, price_col].values.copy()).to(
            device
        )  # .copy() убирает warning

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
):
    for r_tensor in Rs:
        r = float(r_tensor.item())
        r_str = str(int(r))

        mask = dists <= r
        counts = mask.sum(dim=1, dtype=torch.float32)

        weights = torch.exp(-dists / h) * mask
        weight_sum = weights.sum(dim=1)

        weighted_sum = (weights * values).sum(dim=1)
        wnir = torch.where(weight_sum > 1e-8, weighted_sum / weight_sum, torch.nan)

        # WNIR value
        if market_type == "p":
            results.loc[orig_idx, f"wnir_p_value_{r_str}"] = wnir.cpu().numpy()
        else:
            results.loc[orig_idx, f"wnir_s_value_{r_str}"] = wnir.cpu().numpy()

        if market_type == "s":
            v = values.unsqueeze(0).expand(len(orig_idx), -1)

            # Для mean/median/std оставляем NaN
            masked = torch.where(
                mask, v, torch.tensor(float("nan"), device=values.device)
            )

            results.loc[orig_idx, f"wnir_s_mean_{r_str}"] = (
                torch.nanmean(masked, dim=1).cpu().numpy()
            )

            # --- БЕЗОПАСНЫЙ MIN ---
            # Заменяем False в маске на +inf, чтобы они не учитывались при поиске минимума
            masked_for_min = torch.where(
                mask, v, torch.tensor(float("inf"), device=values.device)
            )
            min_vals = torch.min(masked_for_min, dim=1).values
            # Возвращаем NaN туда, где не было ни одного валидного значения (остался +inf)
            min_vals = torch.where(
                min_vals == float("inf"),
                torch.tensor(float("nan"), device=values.device),
                min_vals,
            )
            results.loc[orig_idx, f"wnir_s_min_{r_str}"] = min_vals.cpu().numpy()

            # --- БЕЗОПАСНЫЙ MAX ---
            # Заменяем False в маске на -inf
            masked_for_max = torch.where(
                mask, v, torch.tensor(float("-inf"), device=values.device)
            )
            max_vals = torch.max(masked_for_max, dim=1).values
            max_vals = torch.where(
                max_vals == float("-inf"),
                torch.tensor(float("nan"), device=values.device),
                max_vals,
            )
            results.loc[orig_idx, f"wnir_s_max_{r_str}"] = max_vals.cpu().numpy()

            results.loc[orig_idx, f"wnir_s_count_{r_str}"] = counts.cpu().numpy()

            # std
            std_val = torch.where(
                counts > 1, torch.nanstd(masked, dim=1), torch.zeros_like(counts)
            )
            results.loc[orig_idx, f"wnir_s_std_{r_str}"] = std_val.cpu().numpy()

            # median
            results.loc[orig_idx, f"wnir_s_median_{r_str}"] = (
                torch.nanmedian(masked, dim=1).values.cpu().numpy()
            )
