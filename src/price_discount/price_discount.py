def discount_prices(df):
    df["year_month"] = df["date"].dt.to_period("M")

    monthly_stats = df.groupby("year_month", as_index=False).agg(
        mean_price=("price", "mean")
    )
    base_value = monthly_stats["mean_price"].iloc[-1]
    monthly_stats["index"] = monthly_stats["mean_price"] / base_value

    df = df.merge(monthly_stats[["year_month", "index"]], on="year_month", how="left")

    df["price_normalized"] = df["price"] / df["index"]
    df["price_per_square_meter_normalized"] = df["price_normalized"] / df["area"]

    return df
