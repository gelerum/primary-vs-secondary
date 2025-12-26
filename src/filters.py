from pandas import DataFrame


def drop_nan_addresses(df: DataFrame):
    return df.loc[~df["address"].isna()].reset_index(drop=True).copy()


def drop_nan_prices(df: DataFrame):
    return (
        df.loc[df["price"].notna() & df["price_per_square_meter"].notna()]
        .reset_index(drop=True)
        .copy()
    )
