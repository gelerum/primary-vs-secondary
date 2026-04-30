from src.clean.constants import INTERIM_CLEAN_COLUMNS
from src.clean.adapters import (
    DF1_ADAPTER,
    DF2_ADAPTER,
    DF3_ADAPTER,
    DF4_ADAPTER,
    DF5_ADAPTER,
    adapt_dataframes,
)
from src.clean.concat import concat_dfs
from src.clean.data_loader import read_dfs
from src.clean.filters import (
    drop_duplicates,
    drop_nan_addresses,
    drop_nan_prices,
    drop_nan_rows,
    filter_by_address_len,
    filter_by_area,
    filter_by_build_year,
    filter_by_floor,
    filter_by_price,
    filter_common_moscow_geo_point,
    select_residential,
    filter_after_2017,
)
from src.clean.normalization import normalize_datasets
from src.clean.type_casting import cast_types
from dvc.api import params_show


def main():
    params = params_show()["clean"]

    dfs = read_dfs()

    adapters = [DF1_ADAPTER, DF2_ADAPTER, DF3_ADAPTER, DF4_ADAPTER, DF5_ADAPTER]
    dfs = adapt_dataframes(dfs, adapters)

    dfs = normalize_datasets(dfs)

    df_combined = concat_dfs(dfs)

    df_residential = select_residential(df_combined)

    df_no_dups = drop_duplicates(df_residential)

    df_no_nan_rows = drop_nan_rows(df_no_dups)
    df_no_nan_addresses = drop_nan_addresses(df_no_nan_rows)
    df_no_nan_prices = drop_nan_prices(df_no_nan_addresses)

    df_type_casted = cast_types(df_no_nan_prices, INTERIM_CLEAN_COLUMNS)

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

    df_filter_by_address_len = filter_by_address_len(
        df_filter_by_build_year,
        params["address"]["min_len"],
    )
    df_filter_common_moscow_geo_point = filter_common_moscow_geo_point(
        df_filter_by_address_len,
        params["common_moscow_point"]["longitude"],
        params["common_moscow_point"]["latitude"],
    )

    df_drop_nans_room_count_build_year = df_filter_common_moscow_geo_point.dropna(
        subset=["room_count", "build_year"]
    )

    df_drop_columns_floor_count_ceiling_height = (
        df_drop_nans_room_count_build_year.drop(
            columns=["floor_count", "ceiling_height"]
        )
    )

    df_fileter_after_2017 = filter_after_2017(
        df_drop_columns_floor_count_ceiling_height, "date"
    )

    df_fileter_after_2017["year"] = df_fileter_after_2017["date"].dt.year.astype(
        "uint16"
    )
    df_fileter_after_2017["month"] = df_fileter_after_2017["date"].dt.month.astype(
        "uint8"
    )
    df_fileter_after_2017["day"] = df_fileter_after_2017["date"].dt.day.astype("uint8")

    df_no_flat_type_no_date = df_fileter_after_2017.drop(columns=["flat_type", "date"])

    df_clean = df_no_flat_type_no_date
    # df_clean = cast_types(df_fileter_after_2017, CANONICAL_COLUMNS)

    df_clean.to_parquet("data/interim/clean.parquet", index=False, engine="pyarrow")


if __name__ == "__main__":
    main()
