# main.py

import pandas as pd
from dvc.api import params_show
import gc

# Импортируем новую функцию из нашего модуля
from src.wnir.wnir import process_markets_in_batches


def main():
    params = params_show()["wnir"]
    h = params["h"]
    Rs = list(params["R"].values())
    batch_size = params.get("batch_size")

    print("Loading and preparing data...")
    # Предполагается, что в данных есть колонка 'date' или 'created_at'
    # Убедитесь, что она имеет формат datetime
    df_train = pd.read_parquet("data/interim/price_discount_train.parquet")
    df_train["set_type"] = "train"
    df_valid = pd.read_parquet("data/interim/price_discount_valid.parquet")
    df_valid["set_type"] = "valid"
    df_test = pd.read_parquet("data/interim/price_discount_test.parquet")
    df_test["set_type"] = "test"

    df = pd.concat([df_train, df_valid, df_test], axis=0, ignore_index=True)

    if "date" not in df.columns:
        raise ValueError(
            "DataFrame must contain a 'date' column for temporal processing."
        )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date", ascending=True).reset_index(drop=True)
    gc.collect()

    print(f"Processing with batch size: {batch_size}")

    results_list = []

    for r in Rs:
        print(f"\nProcessing R = {r} ...")

        # Запускаем основную функцию пакетной обработки
        res_p, res_s = process_markets_in_batches(df, R=r, h=h, batch_size=batch_size)

        res_combined = pd.concat([res_p, res_s])

        # Собираем только новые колонки
        new_cols = [col for col in res_combined.columns if col.endswith(f"_{r}")]
        results_list.append(res_combined[new_cols])

    print("\nCombining final results...")
    if results_list:
        df = df.join(pd.concat(results_list, axis=1))

    for r in Rs:
        wnir_col = f"wnir_{r}"
        count_col = f"wnir_neighbours_count_{r}"
        context_mean = f"wnir_context_mean_{r}"
        context_std = f"wnir_context_std_{r}"
        context_min = f"wnir_context_min_{r}"
        context_max = f"wnir_context_max_{r}"
        ratio_col = f"wnir_ratio_mean_{r}"
        df[wnir_col] = df[wnir_col].fillna(df[wnir_col].mean())
        df[count_col] = df[count_col].fillna(0)
        df[context_mean] = df[context_mean].fillna(df[context_mean].mean())
        df[context_std] = df[context_std].fillna(0)
        df[context_min] = df[context_min].fillna(df[context_min].mean())
        df[context_max] = df[context_max].fillna(df[context_max].mean())
        df[ratio_col] = df[ratio_col].fillna(1)

    print("Saving data to parquet...")

    df[df["set_type"] == "train"].drop(columns=["set_type"]).to_parquet(
        "data/interim/wnir_train.parquet", index=False
    )
    df[df["set_type"] == "valid"].drop(columns=["set_type"]).to_parquet(
        "data/interim/wnir_valid.parquet", index=False
    )
    df[df["set_type"] == "test"].drop(columns=["set_type"]).to_parquet(
        "data/interim/wnir_test.parquet", index=False
    )

    print("WNIR stage completed successfully.")


if __name__ == "__main__":
    main()
