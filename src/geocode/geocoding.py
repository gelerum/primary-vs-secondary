import pandas as pd
from .geocode_parser import geocode_df_yandex


def geocode_addresses(
    df,
    api_keys_path,
    checkpoint_path,
):
    api_keys = pd.read_csv(api_keys_path)["key"].astype(str).str.strip().tolist()

    df_to_geocode = df.loc[df["latitude"].isna() | df["longitude"].isna(), ["address"]]
    df_geocoded = geocode_df_yandex(
        df_to_geocode, api_keys, checkpoint_path=checkpoint_path
    )
    df_geocoded = df_geocoded.loc[
        df_geocoded["latitude"].notna() & df_geocoded["longitude"].notna()
    ]

    lat_map = df_geocoded.set_index("address")["latitude"].to_dict()
    lon_map = df_geocoded.set_index("address")["longitude"].to_dict()

    df["latitude"] = df["latitude"].fillna(df["address"].map(lat_map))
    df["longitude"] = df["longitude"].fillna(df["address"].map(lon_map))

    df = df.loc[
        df["latitude"].notna()
        & df["longitude"].notna()
        & (df["latitude"] != 0)
        & (df["longitude"] != 0)
    ].reset_index(drop=True)

    return df
