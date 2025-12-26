import numpy as np
import pandas as pd

FLAT_TYPE_MAP = {
    "Квартира": "flat",
    "квартира": "flat",
    "Паркинг": "parking",
    "машино-место": "parking",
    "Кладовая": "storage",
    "кладовка": "storage",
    "Офис": "office",
    "нежилое помещение": "non_residential",
    "апартаменты": "flat",
    "Без типа": pd.NA,
    "FLAT": "flat",
    "STUDIO": "studio",
    "studio": "studio",
    "Обычная планировка": "flat",
    "Кухня-гостиная": "flat",
    "Свободная планировка": "flat",
    "Мастер-спальня": "flat",
    "euro": "flat",
    np.nan: pd.NA,
    "no": pd.NA,
}


def _assign_housing_type(flat_type):
    if pd.isna(flat_type):
        return pd.NA
    if flat_type in ["flat", "studio"]:
        return "residential"
    elif flat_type in ["parking", "storage", "office"]:
        return "non_residential"
    return pd.NA


def normalize_datasets(dfs):
    for df in dfs:
        if "balcony" in df.columns:
            df["balcony"] = pd.to_numeric(df["balcony"], errors="coerce")
            df["balcony"] = df["balcony"].fillna(0) > 0

        if "flat_type" in df.columns:
            df["flat_type"] = df["flat_type"].map(FLAT_TYPE_MAP)

        df["housing_type"] = df["flat_type"].apply(_assign_housing_type)

    for df in dfs:
        if "date" in df.columns:
            df["date"] = pd.to_datetime(
                df["date"], errors="coerce", infer_datetime_format=True
            )
            df["date"] = df["date"].dt.normalize()

    for df in dfs:
        df = df.dropna(how="all")
        df = df.drop_duplicates()

    return dfs
