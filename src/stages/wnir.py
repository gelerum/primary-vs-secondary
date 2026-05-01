import pandas as pd
from dvc.api import params_show
import gc
from src.wnir.wnir import process_markets_in_batches


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

    df = pd.concat([df_train, df_valid, df_test], axis=0, ignore_index=True).sample(
        10_000
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    gc.collect()

    # Считаем признаки (вернет DF только для строк primary)
    df_results = process_markets_in_batches(df, Rs=Rs, h=h, batch_size=batch_size)

    print("Filtering primary market and freeing memory...")
    # ИСПРАВЛЕНО: добавляем .copy(), чтобы не работать с view
    df_primary = df[df["market_type"] == "primary"].drop(columns=["market_type"]).copy()

    # Удаляем оригинал, чтобы освободить место для новых колонок и заполнения
    del df
    gc.collect()

    print("Transferring calculated features...")
    for col in df_results.columns:
        df_primary[col] = df_results[col]

    del df_results
    gc.collect()

    # Внутри main() после добавления колонок к df_primary:

    print("Filling missing values...")
    for r in Rs:
        # Список колонок, которые заполняем средним (цены)
        price_cols = [
            f"wnir_p_value_{r}",
            f"wnir_s_value_{r}",
            f"wnir_s_mean_{r}",
            f"wnir_s_min_{r}",
            f"wnir_s_max_{r}",
            f"wnir_s_median_{r}",
        ]
        for col in price_cols:
            if col in df_primary.columns:
                df_primary[col] = df_primary[col].fillna(
                    df_primary[df_primary["set_type"] == "train"][col].mean()
                )

        # Колонки, которые заполняем нулем
        if f"wnir_s_count_{r}" in df_primary.columns:
            df_primary[f"wnir_s_count_{r}"] = df_primary[f"wnir_s_count_{r}"].fillna(0)

        if f"wnir_s_std_{r}" in df_primary.columns:
            df_primary[f"wnir_s_std_{r}"] = df_primary[f"wnir_s_std_{r}"].fillna(0)

    print("Saving splits...")
    for stype in ["train", "valid", "test"]:
        out_path = f"data/interim/wnir_{stype}.parquet"
        # ИСПРАВЛЕНО: сохраняем из df_primary
        df_primary[df_primary["set_type"] == stype].drop(
            columns=["set_type"]
        ).to_parquet(out_path, index=False)

    print("WNIR stage completed successfully.")


if __name__ == "__main__":
    main()
