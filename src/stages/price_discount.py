import pandas as pd


def main():
    df_train = pd.read_parquet("data/interim/04_train.parquet", engine="pyarrow")
    df_valid = pd.read_parquet("data/interim/04_valid.parquet", engine="pyarrow")
    df_test = pd.read_parquet("data/interim/04_test.parquet", engine="pyarrow")


if __name__ == "__main__":
    main()
