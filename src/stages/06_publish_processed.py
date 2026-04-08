import pandas as pd


def main():
    df = pd.read_parquet("data/interim/05_administrative_district.parquet")

    df.to_parquet("data/processed/final.parquet", index=False)
    df.to_csv("data/processed/final.csv", index=False)


if __name__ == "__main__":
    main()
