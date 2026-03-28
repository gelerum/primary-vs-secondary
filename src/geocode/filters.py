def filter_by_geo(df, longitude_min, longitude_max, latitude_min, latitude_max):
    df_filtered = df[
        (df["longitude"] >= longitude_min)
        & (df["longitude"] <= longitude_max)
        & (df["latitude"] >= latitude_min)
        & (df["latitude"] <= latitude_max)
    ].reset_index(drop=True)

    return df_filtered
