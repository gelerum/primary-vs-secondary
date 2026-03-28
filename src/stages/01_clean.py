from src.clean.adapters import (
    DF1_ADAPTER,
    DF2_ADAPTER,
    DF3_ADAPTER,
    DF4_ADAPTER,
    adapt_dataframes,
)
from src.clean.concat import concat_dfs
from src.clean.data_loader import read_dfs
from src.clean.filters import (
    drop_duplicates,
    drop_nan_addresses,
    drop_nan_prices,
    drop_nan_rows,
    filter_by_area,
    filter_by_build_year,
    filter_by_floor,
    filter_by_price,
    select_residential,
)
from src.clean.normalization import normalize_datasets
from src.clean.type_casting import cast_types
from dvc.api import params_show


def main():
    params = params_show()["01_clean"]

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

    df_filtered_by_area = filter_by_area(
        df_type_casted,
        params["area"]["min"],
        params["area"]["max"],
    )
    df_filter_by_floor = filter_by_floor(
        df_filtered_by_area,
        params["floor"]["min"],
        params["floor"]["max"],
    )
    df_filter_by_price = filter_by_price(
        df_filter_by_floor,
        params["price"]["min"],
        params["price"]["max"],
    )
    df_filter_by_build_year = filter_by_build_year(
        df_filter_by_price,
        params["build_year"]["min"],
        params["build_year"]["max"],
    )

    df_clean = df_filter_by_build_year

    df_clean.to_parquet("data/interim/01_cleaned.parquet", index=False)


if __name__ == "__main__":
    main()
