from pandas import DataFrame


def drop_nan_rows(df):
    return df.dropna(how="all")


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


def filter_by_area(df, area_min, area_max):
    df_filtered = df[(df["area"] >= area_min) & (df["area"] <= area_max)]

    return df_filtered


def filter_by_floor(df, floor_min, floor_max):
    df_filtered = df[(df["floor"] >= floor_min) & (df["floor"] <= floor_max)]

    return df_filtered


def filter_by_price(df, price_min, price_max):
    df_filtered = df[(df["price"] >= price_min) & (df["price"] <= price_max)]

    return df_filtered


def drop_duplicates(df):
    return df.drop_duplicates()


def select_residential(df):
    return df[df["housing_type"] == "residential"].drop(columns=["housing_type"])
