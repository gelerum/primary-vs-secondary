def discount_prices(df, min_obs=30):
    df = df.copy()

    df["period"] = df["date"].dt.to_period("M").dt.to_timestamp()

    monthly = (
        df.groupby(["market_type", "period"], as_index=False)
        .agg(
            price_sqm_median=("price_per_square_meter", "median"),
            n_obs=("price_per_square_meter", "size"),
        )
        .sort_values(["market_type", "period"])
    )

    # keep only reliable stats
    monthly = monthly[monthly["n_obs"] >= min_obs].copy()

    # market baseline (last available month per market)
    monthly["base_value"] = monthly.groupby("market_type")[
        "price_sqm_median"
    ].transform("last")

    monthly["discount_index"] = monthly["price_sqm_median"] / monthly["base_value"]

    # ⚠️ ML FIX: ensure no missing index after merge
    df = df.merge(
        monthly[["market_type", "period", "discount_index"]],
        on=["market_type", "period"],
        how="left",
    )

    # fallback = neutral scaling (VERY IMPORTANT for ML)
    df["discount_index"] = df["discount_index"].fillna(1.0)

    # normalized features
    df["price_per_square_meter_normalized"] = (
        df["price_per_square_meter"] / df["discount_index"]
    ).astype("float32")

    df["price_normalized"] = (
        df["price_per_square_meter_normalized"] * df["area"]
    ).astype("float32")

    return df.drop(columns=["period", "discount_index"])
