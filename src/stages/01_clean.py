from src.clean.adapters import (
    DF1_ADAPTER,
    DF2_ADAPTER,
    DF3_ADAPTER,
    DF4_ADAPTER,
    adapt_dataframes,
)
from src.clean.concat import concat_dfs
from src.clean.data_loader import read_dfs
from src.filter.filters import (
    drop_duplicates,
    drop_nan_addresses,
    drop_nan_prices,
    drop_nan_rows,
    select_residential,
)
from src.clean.normalization import normalize_datasets
from src.clean.type_casting import cast_types


def main():

    dfs = read_dfs()

    adapters = [DF1_ADAPTER, DF2_ADAPTER, DF3_ADAPTER, DF4_ADAPTER]
    dfs = adapt_dataframes(dfs, adapters)

    dfs = normalize_datasets(dfs)

    df_combined = concat_dfs(dfs)

    df_residential = select_residential(df_combined)

    df_no_dups = drop_duplicates(df_residential)

    df_no_nan_rows = drop_nan_rows(df_no_dups)

    df_no_nan_addresses = drop_nan_addresses(df_no_nan_rows)

    df_no_nan_prices = drop_nan_prices(df_no_nan_addresses)

    df_type_casted = cast_types(df_no_nan_prices)

    df_clean = df_type_casted

    df_clean.to_parquet("data/interim/01_cleaned.parquet", index=False)


if __name__ == "__main__":
    main()
