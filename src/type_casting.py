from src.constants import CANONICAL_COLUMNS


def cast_types(df):
    for col, dtype in CANONICAL_COLUMNS.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    return df
