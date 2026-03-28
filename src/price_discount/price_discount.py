import pandas as pd


def discount_prices(df, min_obs=30):
    df = df.copy()

    df["date"] = pd.to_datetime(df["date"])
    df["period"] = df["date"].dt.to_period("M").dt.to_timestamp()

    monthly = (
        df.groupby(["market_type", "period"], as_index=False)
        .agg(
            price_sqm_median=("price_per_square_meter", "median"),
            n_obs=("price_per_square_meter", "size"),
        )
        .sort_values(["market_type", "period"])
    )

    monthly = monthly[monthly["n_obs"] >= min_obs].copy()

    monthly["base_value"] = monthly.groupby("market_type")[
        "price_sqm_median"
    ].transform("last")

    monthly["discount_index"] = monthly["price_sqm_median"] / monthly["base_value"]

    df = df.merge(
        monthly[["market_type", "period", "discount_index"]],
        on=["market_type", "period"],
        how="left",
    )

    df["price_per_square_meter_normalized"] = (
        df["price_per_square_meter"] / df["discount_index"]
    )

    df["price_normalized"] = df["price_per_square_meter_normalized"] * df["area"]

    return df
