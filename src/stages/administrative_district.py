import pandas as pd

from src.administrative_district.enrich_df import add_administrative_district


def main():
    df = pd.read_parquet("data/interim/geocode.parquet", engine="pyarrow")

    df = add_administrative_district(df, "data/complimentary/ao.shp")

    df["administrative_district"] = df["administrative_district"].fillna(
        "outside_moscow"
    )

    df["administrative_district"] = df["administrative_district"].astype("category")
    df.to_parquet(
        "data/interim/administrative_district.parquet", index=False, engine="pyarrow"
    )


if __name__ == "__main__":
    main()
