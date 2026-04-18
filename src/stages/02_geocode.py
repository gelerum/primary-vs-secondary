import pandas as pd

from src.geocode.filters import filter_by_geo
from src.geocode.geocoding import geocode_addresses
from dvc.api import params_show


def main():
    params = params_show()["02_geocode"]

    df_clean = pd.read_parquet("data/interim/01_cleaned.parquet")

    # Захардкодил очкистку от нулевых координат, чтобы не вызывать геокодинг. Не помню, почему его не хочу запускать. Мб из-за того что нужно преедавать api адреса на сервер
    df_clean = df_clean.dropna(subset=["latitude", "longitude"])

    df_geocoded = geocode_addresses(
        df_clean,
        api_keys_path="secrets/geocoding/ya_api_keys.csv",
        checkpoint_path="data/cache/geocodes_checkpoint.parquet",
    )

    df_filtered_by_geo = filter_by_geo(
        df_geocoded,
        params["geo"]["longitude"]["min"],
        params["geo"]["longitude"]["max"],
        params["geo"]["latitude"]["min"],
        params["geo"]["latitude"]["max"],
    )

    df_filtered_by_geo.to_parquet("data/interim/02_geocoded.parquet", index=False)


if __name__ == "__main__":
    main()
