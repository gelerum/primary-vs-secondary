import pandas as pd
import argparse

import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema

schema = DataFrameSchema(
    {
        "latitude": Column(pa.Float32, nullable=False),
        "longitude": Column(pa.Float32, nullable=False),
        "area": Column(pa.Float32, nullable=False),
        "room_count": Column(pa.UInt8, nullable=False),
        "floor": Column(pa.Int16, nullable=False),
        "build_year": Column(pa.UInt16, nullable=False),
        "balcony": Column(pa.Bool, nullable=False),
        "price": Column(pa.Float32, nullable=False),
        "price_per_square_meter": Column(pa.Float32, nullable=False),
        "date": Column(pa.DateTime, nullable=False),
        "year": Column(pa.UInt16, nullable=False),
        "month": Column(pa.UInt8, nullable=False),
        "day": Column(pa.UInt8, nullable=False),
        "market_type": Column(pa.Category, nullable=False),
        "administrative_district": Column(pa.Category, nullable=False),
    },
    strict=True,  # запрещает лишние колонки
    coerce=False,
)


def validate(df: pd.DataFrame):
    schema.validate(df, lazy=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path", help="Путь к parquet файлу")
    args = parser.parse_args()

    df = pd.read_parquet(args.input_path)
    validate(df)


if __name__ == "__main__":
    main()
