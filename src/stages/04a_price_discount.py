import pandas as pd

from src.feature.price_discount import discount_prices


def main():
    df = pd.read_parquet("data/interim/03_filtered.parquet")

    df = discount_prices(df)

    df.to_parquet("data/interim/04a_price_discounted.parquet", index=False)


if __name__ == "__main__":
    main()
