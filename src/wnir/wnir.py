import torch
import numpy as np
import pandas as pd
from tqdm import tqdm
import gc

EARTH_RADIUS = 6371000.0


def haversine_torch(lat1, lon1, lat2, lon2):
    lat1 = torch.deg2rad(lat1)
    lon1 = torch.deg2rad(lon1)
    lat2 = torch.deg2rad(lat2)
    lon2 = torch.deg2rad(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        torch.sin(dlat * 0.5) ** 2
        + torch.cos(lat1) * torch.cos(lat2) * torch.sin(dlon * 0.5) ** 2
    )
    c = 2 * torch.atan2(torch.sqrt(a), torch.sqrt(1 - a))
    return EARTH_RADIUS * c


@torch.no_grad()
def process_markets_in_batches(
    df: pd.DataFrame,
    Rs: list[float],
    h: float,
    batch_size: int = 18000,  # оптимально для RTX 3060 12GB
    price_col: str = "price_per_square_meter_normalized",
    device: str = "cuda",
) -> pd.DataFrame:

    device = torch.device(device)
    torch.cuda.empty_cache()

    Rs = torch.tensor(Rs, device=device, dtype=torch.float32)
    h = torch.tensor(h, device=device, dtype=torch.float32)

    primary_mask = df["market_type"] == "primary"
    primary_indices = df.index[primary_mask].copy()

    # Создаём колонки
    cols = []
    for r in map(
        str,
        Rs.cpu().numpy().astype(int)
        if all(x.is_integer() for x in Rs.cpu().numpy())
        else Rs.cpu().numpy(),
    ):
        cols.extend(
            [
                f"wnir_p_value_{r}",
                f"wnir_s_value_{r}",
                f"wnir_s_mean_{r}",
                f"wnir_s_std_{r}",
                f"wnir_s_min_{r}",
                f"wnir_s_max_{r}",
                f"wnir_s_median_{r}",
                f"wnir_s_count_{r}",
            ]
        )

    results = pd.DataFrame(index=primary_indices, columns=cols, dtype=np.float32)

    # История на GPU
    hist_p = {"coord": None, "val": None}
    hist_s = {"coord": None, "val": None}

    for start_idx in tqdm(range(0, len(df), batch_size), desc="WNIR GPU"):
        batch = df.iloc[start_idx : start_idx + batch_size]
        coords = torch.from_numpy(
            batch[["latitude", "longitude"]].values.astype(np.float32)
        ).to(device)

        is_p = batch["market_type"].values == "primary"
        query_coords = coords[is_p]
        query_idx = batch.index[is_p]

        if len(query_coords) > 0:
            # Расчёт по Primary
            if hist_p["coord"] is not None:
                dists = haversine_torch(
                    query_coords[:, 0],
                    query_coords[:, 1],
                    hist_p["coord"][:, 0],
                    hist_p["coord"][:, 1],
                )
                _update_results(dists, hist_p["val"], Rs, h, results, query_idx, "p")

            # Расчёт по Secondary
            if hist_s["coord"] is not None:
                dists = haversine_torch(
                    query_coords[:, 0],
                    query_coords[:, 1],
                    hist_s["coord"][:, 0],
                    hist_s["coord"][:, 1],
                )
                _update_results(dists, hist_s["val"], Rs, h, results, query_idx, "s")

        # === Обновляем историю ===
        p_mask = torch.from_numpy(is_p).to(device)

        if p_mask.any():
            new_coords = coords[p_mask]
            new_vals = torch.from_numpy(batch.loc[is_p, price_col].values).to(device)
            hist_p["coord"] = (
                new_coords
                if hist_p["coord"] is None
                else torch.cat([hist_p["coord"], new_coords])
            )
            hist_p["val"] = (
                new_vals
                if hist_p["val"] is None
                else torch.cat([hist_p["val"], new_vals])
            )

        s_mask = ~p_mask
        if s_mask.any():
            new_coords = coords[s_mask]
            new_vals = torch.from_numpy(batch.loc[~is_p, price_col].values).to(device)
            hist_s["coord"] = (
                new_coords
                if hist_s["coord"] is None
                else torch.cat([hist_s["coord"], new_coords])
            )
            hist_s["val"] = (
                new_vals
                if hist_s["val"] is None
                else torch.cat([hist_s["val"], new_vals])
            )

    torch.cuda.empty_cache()
    gc.collect()
    return results


def _update_results(dists, values, Rs, h, results_df, orig_idx, market_type):
    for r_tensor in Rs:
        r = float(r_tensor.item())
        r_str = str(int(r))

        mask = dists <= r
        counts = mask.sum(dim=1, dtype=torch.float32)

        weights = torch.exp(-dists / h) * mask
        weight_sum = weights.sum(dim=1)

        weighted_sum = (weights * values).sum(dim=1)
        wnir = torch.where(weight_sum > 1e-8, weighted_sum / weight_sum, torch.nan)

        prefix = f"wnir_{market_type}_"
        results_df.loc[orig_idx, f"{prefix}value_{r_str}"] = wnir.cpu().numpy()

        if market_type == "s":
            v = values.unsqueeze(0).expand(len(orig_idx), -1)
            masked_v = torch.where(mask, v, torch.nan)

            results_df.loc[orig_idx, f"wnir_s_mean_{r_str}"] = (
                masked_v.nanmean(dim=1).cpu().numpy()
            )
            results_df.loc[orig_idx, f"wnir_s_min_{r_str}"] = (
                masked_v.nanmin(dim=1).cpu().numpy()
            )
            results_df.loc[orig_idx, f"wnir_s_max_{r_str}"] = (
                masked_v.nanmax(dim=1).cpu().numpy()
            )
            results_df.loc[orig_idx, f"wnir_s_count_{r_str}"] = counts.cpu().numpy()

            # std и median делаем чуть умнее
            results_df.loc[orig_idx, f"wnir_s_std_{r_str}"] = (
                torch.where(counts > 1, masked_v.nanstd(dim=1, correction=0), 0.0)
                .cpu()
                .numpy()
            )

            # Медиана — самое медленное. Можно отключить, если не критична
            results_df.loc[orig_idx, f"wnir_s_median_{r_str}"] = (
                torch.nanmedian(masked_v, dim=1).values.cpu().numpy()
            )
