import pandas as pd

from src.geocode.geocoding import geocode_addresses


def main():
    df_clean = pd.read_parquet("data/interim/01_cleaned.parquet")

    df_geocoded = geocode_addresses(
        df_clean,
        api_keys_path="secrets/geocoding/ya_api_keys.csv",
        checkpoint_path="data/cache/geocodes_checkpoint.parquet",
    )

    df_geocoded.to_parquet("data/interim/02_geocoded.parquet", index=False)


if __name__ == "__main__":
    main()
