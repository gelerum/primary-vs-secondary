import pandas as pd

from src.adapters import (
    DF1_ADAPTER,
    DF2_ADAPTER,
    DF3_ADAPTER,
    DF4_ADAPTER,
    adapt_dataframes,
)
from src.data_loader import read_dfs
from src.normalization import normalize_price
from src.filters import (
    drop_nan_addresses,
    drop_nan_prices,
    filter_by_area,
    filter_by_floor,
    filter_by_geo,
    filter_by_price,
)
from src.geocoding import geocode_addresses
from src.normalization import normalize_datasets
from src.type_casting import cast_types
from src.feautures import compute_mean_knn_by_geo, compute_ratio_knn


def main():
    dfs = read_dfs()

    adapters = [DF1_ADAPTER, DF2_ADAPTER, DF3_ADAPTER, DF4_ADAPTER]
    dfs = adapt_dataframes(dfs, adapters)

    dfs = normalize_datasets(dfs)

    dfs = [df.dropna(how="all") for df in dfs]

    df_combined = pd.concat(dfs, ignore_index=True).drop_duplicates()

    df_residential = df_combined[df_combined["housing_type"] == "residential"].drop(
        columns=["housing_type"]
    )

    df_clean = drop_nan_addresses(df_residential)

    df_clean = drop_nan_prices(df_clean)

    df_clean = cast_types(df_clean)

    df_clean = geocode_addresses(
        df_clean,
        api_keys_path="secrets/geocoding/ya_api_keys.csv",
        checkpoint_path="data/geocoding/geocodes_checkpoint.parquet",
    )
    df_filtered_by_geo = filter_by_geo(df_clean, 36.90, 38.05, 55.15, 56.05)

    df_filtered_by_area = filter_by_area(df_filtered_by_geo, 17.5, 1100)

    df_filter_by_floor = filter_by_floor(df_filtered_by_area, -4, 85)

    df_filter_by_price = filter_by_price(df_filter_by_floor, 1_100_000, 7_300_000_000)

    df_price_normalized = normalize_price(df_filter_by_price)

    df_primary = df_price_normalized[
        df_price_normalized["market_type"] == "primary"
    ].copy()

    df_secondary = df_price_normalized[
        df_price_normalized["market_type"] == "secondary"
    ].copy()

    df_primary, coords_primary, _ = compute_mean_knn_by_geo(df_primary, k=30, h=500)
    df_secondary, _, tree_secondary = compute_mean_knn_by_geo(df_secondary, k=30, h=500)

    df_primary = compute_ratio_knn(
        df_primary,
        df_secondary,
        coords_primary,
        tree_secondary,
        k=30,
    )

    df_mean_knn = pd.concat([df_primary, df_secondary])
    df_mean_knn = df_mean_knn.sort_index()

    df_final = df_mean_knn

    path_to_save = "data/processed/v3/"

    df_final.to_parquet(
        path_to_save + "housing_residential_processed.parquet", index=False
    )
    df_final.to_csv(path_to_save + "housing_residential_processed.csv", index=False)

    print(f"Final dataset: {df_final.shape[0]} rows x {df_final.shape[1]} columns")


if __name__ == "__main__":
    main()
