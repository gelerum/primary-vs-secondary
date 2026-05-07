from pandas import DataFrame
import pandas as pd


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


def filter_by_address_len(df, min_len):
    df_filtered = df[df["address"].str.len() >= min_len]
    return df_filtered


def filter_after_2017(df, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df[df[date_col] > "2017-12-31"]


def filter_common_moscow_geo_point(df, longitude, latitude):
    df_filtered = df[~((df["longitude"] == longitude) & (df["latitude"] == latitude))]
    return df_filtered


def filter_by_area(df, area_min, area_max):
    df_filtered = df[(df["area"] >= area_min) & (df["area"] <= area_max)]

    return df_filtered


def filter_by_floor(df, floor_min, floor_max):
    df_filtered = df[(df["floor"] >= floor_min) & (df["floor"] <= floor_max)]

    return df_filtered


def filter_by_price(df, price_min, price_max):
    df_filtered = df[(df["price"] >= price_min) & (df["price"] <= price_max)]

    # дополнительная фильтрация по 99-му перцентилю, чтобы отсеять выбросы
    return df_filtered.quantile(0.99)


def filter_by_build_year(df, min_value, max_value):
    df_filtered = df.copy()

    if min_value is not None:
        df_filtered = df_filtered[df_filtered["price"] >= min_value]

    if max_value is not None:
        df_filtered = df_filtered[df_filtered["price"] <= max_value]

    return df_filtered


def drop_duplicates(df):
    return df.drop_duplicates()


def select_residential(df):
    return df[df["housing_type"] == "residential"].drop(columns=["housing_type"])
