from pandas import DataFrame


def drop_nan_addresses(df: DataFrame):
    return df.loc[~df["address"].isna()].reset_index(drop=True).copy()


def drop_nan_prices(df: DataFrame):
    return (
        df.loc[df["price"].notna() & df["price_per_square_meter"].notna()]
        .reset_index(drop=True)
        .copy()
    )


def filter_by_geo(df, longitude_min, longitude_max, latitude_min, latitude_max):
    df_filtered = df[
        (df["longitude"] >= longitude_min)
        & (df["longitude"] <= longitude_max)
        & (df["latitude"] >= latitude_min)
        & (df["latitude"] <= latitude_max)
    ].reset_index(drop=True)
    return df_filtered
