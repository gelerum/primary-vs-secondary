import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema, Field
from pandera.typing import DataFrame, Series

wnir_cols = [
    "wnir_p_value_100_all",
    "wnir_s_value_100_all",
    "wnir_s_mean_100_all",
    "wnir_s_std_100_all",
    "wnir_s_min_100_all",
    "wnir_s_max_100_all",
    "wnir_s_median_100_all",
    "wnir_s_count_100_all",
    "wnir_p_value_500_all",
    "wnir_s_value_500_all",
    "wnir_s_mean_500_all",
    "wnir_s_std_500_all",
    "wnir_s_min_500_all",
    "wnir_s_max_500_all",
    "wnir_s_median_500_all",
    "wnir_s_count_500_all",
    "wnir_p_value_1000_all",
    "wnir_s_value_1000_all",
    "wnir_s_mean_1000_all",
    "wnir_s_std_1000_all",
    "wnir_s_min_1000_all",
    "wnir_s_max_1000_all",
    "wnir_s_median_1000_all",
    "wnir_s_count_1000_all",
    "wnir_p_value_5000_all",
    "wnir_s_value_5000_all",
    "wnir_s_mean_5000_all",
    "wnir_s_std_5000_all",
    "wnir_s_min_5000_all",
    "wnir_s_max_5000_all",
    "wnir_s_median_5000_all",
    "wnir_s_count_5000_all",
    "wnir_p_value_10000_all",
    "wnir_s_value_10000_all",
    "wnir_s_mean_10000_all",
    "wnir_s_std_10000_all",
    "wnir_s_min_10000_all",
    "wnir_s_max_10000_all",
    "wnir_s_median_10000_all",
    "wnir_s_count_10000_all",
]


class WnirAllSchema(pa.DataFrameModel):
    # 1. Базовые явные колонки
    longitude: Series[pa.Float32] = Field(nullable=False)
    latitude: Series[pa.Float32] = Field(nullable=False)
    area: Series[pa.Float32] = Field(nullable=False)
    room_count: Series[pa.UInt8] = Field(nullable=False)
    floor: Series[pa.Int16] = Field(nullable=False)

    market_type: Series[pa.Category] = Field(
        isin=["primary", "secondary"], nullable=False
    )

    build_year: Series[pa.UInt16] = Field(nullable=False)
    date: Series[pa.DateTime] = Field(nullable=False)
    year: Series[pa.UInt16] = Field(nullable=False)
    month: Series[pa.UInt8] = Field(nullable=False)
    day: Series[pa.UInt8] = Field(nullable=False)
    administrative_district: Series[pa.Category] = Field(nullable=False)
    price_per_square_meter_normalized: Series[pa.Float32] = Field(nullable=False)
    price_normalized: Series[pa.Float32] = Field(nullable=False)

    wnir_features: Series[pa.Float32] = Field(
        alias="^wnir_.*_all$", regex=True, nullable=True
    )

    class Config:
        strict = True

    @pa.dataframe_check(name="strict_nan_logic")
    def check_wnir_nan_logic(cls, df: DataFrame) -> DataFrame:
        is_secondary = df["market_type"] == "secondary"

        wnir_df = df[wnir_cols]
        wnir_check = wnir_df.isna().eq(is_secondary, axis=0)

        result = pd.DataFrame(True, index=df.index, columns=df.columns)

        result[wnir_cols] = wnir_check

        return result


wnir_all_schema = WnirAllSchema.to_schema()
