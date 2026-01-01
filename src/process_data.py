import pandas as pd
from src.data_loader import read_dfs
from src.adapters import (
    adapt_dataframes,
    DF1_ADAPTER,
    DF2_ADAPTER,
    DF3_ADAPTER,
    DF4_ADAPTER,
)
from src.normalization import normalize_datasets
from src.filters import drop_nan_addresses, drop_nan_prices, filter_by_geo
from src.geocoding import geocode_addresses


def main():
    dfs = read_dfs()

    adapters = [DF1_ADAPTER, DF2_ADAPTER, DF3_ADAPTER, DF4_ADAPTER]
    dfs = adapt_dataframes(dfs, adapters)

    dfs = normalize_datasets(dfs)

    df_combined = pd.concat(dfs, ignore_index=True).drop_duplicates()

    df_residential = df_combined[df_combined["housing_type"] == "residential"].drop(
        columns=["housing_type"]
    )

    df_clean = drop_nan_addresses(df_residential)
    df_clean = geocode_addresses(
        df_clean,
        api_keys_path="data/geocoding/ya_api_keys.csv",
        checkpoint_path="data/geocoding/geocodes_checkpoint.parquet",
    )
    df_final = drop_nan_prices(df_clean)

    df_final = filter_by_geo(df_final, 36.90, 38.05, 55.15, 56.05)

    df_final.to_parquet(
        "data/processed/housing_residential_processed.parquet", index=False
    )
    print(f"Final dataset: {df_final.shape[0]} rows x {df_final.shape[1]} columns")


if __name__ == "__main__":
    main()
