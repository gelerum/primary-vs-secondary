import pandas as pd

from src.price_discount.price_discount import discount_prices


def main():
    df = pd.read_parquet("data/interim/02_geocoded.parquet")

    df = discount_prices(df)

    df.to_parquet("data/interim/03_price_discounted.parquet", index=False)


if __name__ == "__main__":
    main()
