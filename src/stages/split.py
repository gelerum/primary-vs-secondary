import pandas as pd


def main():
    df = pd.read_parquet("data/interim/administrative_district.parquet")

    df_train = df[df["year"] < 2024]
    df_valid = df[(df["year"] >= 2024) & (df["year"] < 2025)]
    df_test = df[df["year"] >= 2025]

    df_train.to_parquet(
        "data/interim/split_train.parquet", index=False, engine="pyarrow"
    )
    df_valid.to_parquet(
        "data/interim/split_valid.parquet", index=False, engine="pyarrow"
    )
    df_test.to_parquet("data/interim/split_test.parquet", index=False, engine="pyarrow")


if __name__ == "__main__":
    main()
