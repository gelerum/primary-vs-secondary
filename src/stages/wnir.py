import pandas as pd
from dvc.api import params_show
import gc
import torch
import numpy as np
from src.wnir.wnir import process_markets_in_batches


def get_nearest_train_indices_gpu(
    target_coords_rad: torch.Tensor,
    source_coords_rad: torch.Tensor,
    batch_size: int = 2048,
    device: str = "cuda",
) -> torch.Tensor:
    """
    Быстрый поиск индексов ближайших точек на GPU.
    target_coords_rad - точки, для которых ищем соседей (N, 2)
    source_coords_rad - точки, СРЕДИ которых ищем (M, 2)
    """
    nearest_indices = torch.zeros(
        len(target_coords_rad), dtype=torch.long, device=device
    )

    h_lat = source_coords_rad[:, 0].unsqueeze(0)  # (1, M)
    h_lon = source_coords_rad[:, 1].unsqueeze(0)  # (1, M)

    # Идем батчами по target, чтобы не получить OutOfMemory,
    # если точек сотни тысяч
    for i in range(0, len(target_coords_rad), batch_size):
        q_chunk = target_coords_rad[i : i + batch_size]
        q_lat = q_chunk[:, 0].unsqueeze(1)  # (B, 1)
        q_lon = q_chunk[:, 1].unsqueeze(1)  # (B, 1)

        # Считаем только базис Haversine (без sqrt и arcsin),
        # так как нам нужен только argmin (минимум)
        dlat = torch.sub(h_lat, q_lat).mul_(0.5).sin_().pow_(2)
        dlon = torch.sub(h_lon, q_lon).mul_(0.5).sin_().pow_(2)
        dlon.mul_(torch.cos(q_lat)).mul_(torch.cos(h_lat))

        a = dlat.add_(dlon)  # (B, M)

        nearest_indices[i : i + batch_size] = torch.argmin(a, dim=1)

        del dlat, dlon, a, q_chunk, q_lat, q_lon

    return nearest_indices


def main():
    params = params_show()["wnir"]
    h = params["h"]
    Rs = list(params["R"].values())
    batch_size = params.get("batch_size", 20000)

    print("Loading data...")
    df_train = pd.read_parquet("data/interim/price_discount_train.parquet")
    df_train["set_type"] = "train"
    df_valid = pd.read_parquet("data/interim/price_discount_valid.parquet")
    df_valid["set_type"] = "valid"
    df_test = pd.read_parquet("data/interim/price_discount_test.parquet")
    df_test["set_type"] = "test"

    df = pd.concat([df_train, df_valid, df_test], axis=0, ignore_index=True)

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    gc.collect()

    df_results = process_markets_in_batches(df, Rs=Rs, h=h, batch_size=batch_size)

    print("Filtering primary market and freeing memory...")
    df_primary = df[df["market_type"] == "primary"].drop(columns=["market_type"]).copy()

    del df
    gc.collect()

    print("Transferring calculated features...")
    for col in df_results.columns:
        df_primary[col] = df_results[col]

    del df_results
    gc.collect()

    # =========================================================================
    # НОВЫЙ БЛОК: ЗАПОЛНЕНИЕ ПРОПУСКОВ ЧЕРЕЗ БЛИЖАЙШЕГО СОСЕДА ИЗ TRAIN НА GPU
    # =========================================================================
    print("Filling missing values via Nearest Neighbor (GPU)...")

    device = torch.device("cuda")
    # Подготавливаем координаты всех точек в радианах и отправляем на GPU
    coords_np = df_primary[["latitude", "longitude"]].values.astype(np.float32)
    coords_tensor = torch.from_numpy(coords_np).to(device) * (torch.pi / 180.0)

    for r in Rs:
        r_str = str(int(r)) if float(r).is_integer() else str(r)

        price_cols = [
            f"wnir_p_value_{r_str}",
            f"wnir_s_value_{r_str}",
            f"wnir_s_mean_{r_str}",
            f"wnir_s_min_{r_str}",
            f"wnir_s_max_{r_str}",
            f"wnir_s_median_{r_str}",
        ]

        # 1. Заполняем count и std нулями (это логическое правило для всех сетов)
        count_col = f"wnir_s_count_{r_str}"
        std_col = f"wnir_s_std_{r_str}"

        if count_col in df_primary.columns:
            df_primary[count_col] = df_primary[count_col].fillna(0)
        if std_col in df_primary.columns:
            df_primary[std_col] = df_primary[std_col].fillna(0)

        # 2. Ищем пропуски в ценовых колонках.
        # Берем только те колонки, которые реально есть в датафрейме
        existing_price_cols = [col for col in price_cols if col in df_primary.columns]
        if not existing_price_cols:
            continue

        base_col = existing_price_cols[0]

        # ИСТОЧНИК (Source): точки из train, у которых НЕТ пропуска в этом радиусе
        source_mask = (df_primary["set_type"] == "train") & (
            df_primary[base_col].notna()
        )

        # ЦЕЛЬ (Target): точки из valid или test, у которых ЕСТЬ пропуск
        target_mask = (df_primary["set_type"].isin(["valid", "test"])) & (
            df_primary[base_col].isna()
        )

        if source_mask.any() and target_mask.any():
            source_idx = np.where(source_mask)[0]
            target_idx = np.where(target_mask)[0]

            source_coords = coords_tensor[source_idx]
            target_coords = coords_tensor[target_idx]

            # Ищем индексы ближайших соседей на GPU
            with torch.no_grad():
                nearest_relative_indices = get_nearest_train_indices_gpu(
                    target_coords, source_coords, device=device
                )

            # Переводим относительные индексы обратно в абсолютные индексы датафрейма
            nearest_absolute_indices = source_idx[
                nearest_relative_indices.cpu().numpy()
            ]

            # Копируем все ценовые фичи из ближайшей точки train в точки valid/test
            df_primary.loc[df_primary.index[target_idx], existing_price_cols] = (
                df_primary.loc[
                    df_primary.index[nearest_absolute_indices], existing_price_cols
                ].values
            )

        # 3. Дополнительно (на всякий случай): если пропуски остались в самом train
        # (например, это самая первая точка по времени и ей не у кого брать историю),
        # заполняем их средним по train, чтобы не сломать модель NaN-ами.
        for col in existing_price_cols:
            train_mean = df_primary[df_primary["set_type"] == "train"][col].mean()
            df_primary[col] = df_primary[col].fillna(train_mean)

    # Очищаем VRAM
    del coords_tensor
    torch.cuda.empty_cache()
    gc.collect()
    # =========================================================================

    print("Saving splits...")
    for stype in ["train", "valid", "test"]:
        out_path = f"data/interim/wnir_{stype}.parquet"
        df_primary[df_primary["set_type"] == stype].drop(
            columns=["set_type"]
        ).to_parquet(out_path, index=False)

    print("WNIR stage completed successfully.")


if __name__ == "__main__":
    main()
