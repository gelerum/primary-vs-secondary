import pandas as pd


def main():
    df = pd.read_parquet("data/interim/04_mean_knn.parquet")

    df.to_parquet("data/processed/final.parquet", index=False)
    df.to_csv("data/processed/final.csv", index=False)


if __name__ == "__main__":
    main()
