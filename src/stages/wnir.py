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

    df = pd.concat([df_train, df_valid, df_test], axis=0, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    gc.collect()

    # Считаем признаки (вернет DF только для строк primary)
    df_results = process_markets_in_batches(df, Rs=Rs, h=h, batch_size=batch_size)

    print("Joining and filtering primary market...")
    # Оставляем только первичку и удаляем тип рынка
    df = df[df["market_type"] == "primary"].drop(columns=["market_type"])
    df = df.join(df_results)

    # Заполнение пропусков
    for r in Rs:
        # Для цен используем среднее по колонке
        for suffix in [
            "wnir_p",
            "wnir_s_mean",
            "wnir_s_min",
            "wnir_s_max",
            "wnir_s_median",
        ]:
            col = f"{suffix}_{r}"
            df[col] = df[col].fillna(df[col].mean())

        # Для счетчиков и отклонения — 0
        df[f"wnir_s_count_{r}"] = df[f"wnir_s_count_{r}"].fillna(0)
        df[f"wnir_s_std_{r}"] = df[f"wnir_s_std_{r}"].fillna(0)

    print("Saving splits...")
    for stype in ["train", "valid", "test"]:
        out_path = f"data/interim/wnir_{stype}.parquet"
        df[df["set_type"] == stype].drop(columns=["set_type"]).to_parquet(
            out_path, index=False
        )

    print("Done.")


if __name__ == "__main__":
    main()
