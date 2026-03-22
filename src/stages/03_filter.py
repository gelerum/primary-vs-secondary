import pandas as pd

from src.filter.filters import (
    filter_by_area,
    filter_by_floor,
    filter_by_geo,
    filter_by_price,
)


def main():
    df_clean = pd.read_parquet("data/interim/02_geocoded.parquet")

    df_filtered_by_geo = filter_by_geo(df_clean, 36.90, 38.05, 55.15, 56.05)

    df_filtered_by_area = filter_by_area(df_filtered_by_geo, 17.5, 1100)

    df_filter_by_floor = filter_by_floor(df_filtered_by_area, -4, 85)

    df_filter_by_price = filter_by_price(df_filter_by_floor, 1_100_000, 7_300_000_000)

    df_filtered = df_filter_by_price

    df_filtered.to_parquet("data/interim/03_filtered.parquet", index=False)


if __name__ == "__main__":
    main()
