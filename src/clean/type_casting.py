def cast_types(df, COLUMN_SCHEME):
    for col, dtype in COLUMN_SCHEME.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    return df
