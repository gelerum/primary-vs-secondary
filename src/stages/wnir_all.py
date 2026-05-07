import pandas as pd
from dvc.api import params_show
import gc
import torch
from src.wnir.wnir import calculate_and_impute_wnir


def main():
    params = params_show()["wnir"]
    h = params["h"]
    Rs = list(params["R"].values())
    batch_size = params.get("batch_size", 20000)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

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

    # ИЗМЕНЕНИЕ: Теперь мы сохраняем ВСЕ данные (и primary, и secondary)
    # Колонку "market_type" тоже оставляем, иначе вы потом не отличите их друг от друга!
    df_master = df.copy()

    # =========================================================================
    # 1. РАСЧЕТ ДЛЯ ВСЕХ ДАННЫХ (постфикс _all)
    # =========================================================================
    new_features_all = calculate_and_impute_wnir(
        df_group=df,
        Rs=Rs,
        h=h,
        batch_size=batch_size,
        suffix="all",
        device=device,
        fill_nearest_threshold=params["fill_nearest_threshold"],
    )

    # Присоединяем к мастер-датафрейму по индексу
    # Так как в new_features_all есть индексы только для primary,
    # Pandas автоматически проставит NaN во всех колонках wnir для secondary.
    df_master = df_master.join(new_features_all)

    print("\nSaving splits...")
    for stype in ["train", "valid", "test"]:
        out_path = f"data/interim/wnir_all_{stype}.parquet"

        # Сохраняем обратно в соответствующие выборки
        df_master[df_master["set_type"] == stype].drop(columns=["set_type"]).to_parquet(
            out_path, index=False
        )

    print("WNIR stage completed successfully.")


if __name__ == "__main__":
    main()
