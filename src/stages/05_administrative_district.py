import pandas as pd

from src.administrative_district.enrich_df import add_administrative_district


def main():
    df = pd.read_parquet("data/interim/04_wnir.parquet")

    df = add_administrative_district(df, "data/complimentary/ao.shp")

    df.to_parquet("data/interim/05_administrative_district.parquet", index=False)


if __name__ == "__main__":
    main()
