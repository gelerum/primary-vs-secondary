import numpy as np
import pandas as pd


DF1_ADAPTER = {
    "address": pd.NA,
    "longitude": np.nan,
    "latitude": np.nan,
    "area": lambda df: pd.to_numeric(df["SFA, м2"], errors="coerce"),
    "room_count": pd.NA,
    "floor": lambda df: pd.to_numeric(df["Этаж"], errors="coerce"),
    "floor_count": lambda df: pd.to_numeric(
        df["Максимальное кол-во этажей в этой секциии"], errors="coerce"
    ),
    "market_type": lambda df: "secondary",
    "housing_type": pd.NA,
    "flat_type": "Вид помещения",
    "ceiling_height": np.nan,
    "build_year": np.nan,
    "balcony": "Количество балконов",
    "price": lambda df: pd.to_numeric(df["Стоимость"], errors="coerce"),
    "price_per_square_meter": lambda df: pd.to_numeric(df["Стоимость"], errors="coerce")
    / pd.to_numeric(df["SFA, м2"], errors="coerce"),
    "date": "Дата последней брони",
}

DF2_ADAPTER = {
    "address": "address",
    "longitude": np.nan,
    "latitude": np.nan,
    "area": lambda df: pd.to_numeric(df["area"], errors="coerce"),
    "room_count": lambda df: pd.to_numeric(df["room_count"], errors="coerce"),
    "floor": lambda df: pd.to_numeric(df["floor"], errors="coerce"),
    "floor_count": lambda df: pd.to_numeric(df["floor_count"], errors="coerce"),
    "market_type": lambda df: "secondary",
    "housing_type": pd.NA,
    "flat_type": "flat_type",
    "ceiling_height": lambda df: pd.to_numeric(df["ceiling_height"], errors="coerce"),
    "build_year": "build_year",
    "balcony": "balcony",
    "price": lambda df: pd.to_numeric(df["price"], errors="coerce"),
    "price_per_square_meter": lambda df: pd.to_numeric(
        df["price_per_square_meter"], errors="coerce"
    ),
    "date": "actualized_at",
}

DF3_ADAPTER = {
    "address": lambda df: (
        df["street_name"].astype(str) + ", " + df["house_number"].astype(str)
    ),
    "longitude": np.nan,
    "latitude": np.nan,
    "area": "area",
    "room_count": lambda df: pd.to_numeric(df["room_count"], errors="coerce"),
    "floor": lambda df: pd.to_numeric(df["floor"], errors="coerce"),
    "floor_count": lambda df: pd.to_numeric(df["floor_count"], errors="coerce"),
    "market_type": lambda df: "secondary",
    "housing_type": pd.NA,
    "flat_type": "flat_type",
    "ceiling_height": lambda df: pd.to_numeric(df["ceiling_height"], errors="coerce"),
    "build_year": "build_year",
    "balcony": pd.NA,
    "price": lambda df: pd.to_numeric(df["price"], errors="coerce"),
    "price_per_square_meter": lambda df: pd.to_numeric(
        df["price_per_square_meter"], errors="coerce"
    ),
    "date": "created_at",
}

DF4_ADAPTER = {
    "address": "Адрес корпуса",
    "longitude": lambda df: pd.to_numeric(df["longitude"], errors="coerce"),
    "latitude": lambda df: pd.to_numeric(df["latitude"], errors="coerce"),
    "area": lambda df: pd.to_numeric(df["Площадь согласно ЕГРН"], errors="coerce"),
    "room_count": lambda df: pd.to_numeric(df["Количество комнат"], errors="coerce"),
    "floor": lambda df: pd.to_numeric(df["Этаж"], errors="coerce"),
    "floor_count": np.nan,
    "market_type": lambda df: "primary",
    "housing_type": pd.NA,
    "flat_type": "Тип объекта",
    "ceiling_height": np.nan,
    "build_year": lambda df: pd.to_datetime(
        df["Дата договора"], errors="coerce"
    ).dt.year,
    "balcony": pd.NA,
    "price": lambda df: pd.to_numeric(df["Площадь согласно ЕГРН"], errors="coerce")
    * pd.to_numeric(df["Цена за кв. метр"], errors="coerce"),
    "price_per_square_meter": lambda df: pd.to_numeric(
        df["Цена за кв. метр"], errors="coerce"
    ),
    "date": "Дата договора",
}


def _adapt_dataframe(df, adapter):
    adapted = pd.DataFrame()

    for canonical_col, source in adapter.items():
        if isinstance(source, str):
            adapted[canonical_col] = df[source] if source in df.columns else pd.NA
        elif callable(source):
            adapted[canonical_col] = source(df)
        else:
            adapted[canonical_col] = source

    return adapted


def adapt_dataframes(dfs, adapters):
    return [_adapt_dataframe(df, adapter) for df, adapter in zip(dfs, adapters)]
