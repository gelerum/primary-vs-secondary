import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class PriceDiscounter(BaseEstimator, TransformerMixin):
    def __init__(self, min_obs=30):
        self.min_obs = min_obs
        self.discount_map_ = None

    def fit(self, df, y=None):

        df_ = df.copy()
        df_["date"] = pd.to_datetime(df_[["year", "month", "day"]])
        df_["period"] = df_["date"].dt.to_period("M").dt.to_timestamp()

        monthly = (
            df_.groupby(["market_type", "period"], as_index=False)
            .agg(
                price_sqm_median=("price_per_square_meter", "median"),
                n_obs=("price_per_square_meter", "size"),
            )
            .sort_values(["market_type", "period"])
        )

        monthly = monthly[monthly["n_obs"] >= self.min_obs].copy()

        monthly["base_value"] = monthly.groupby("market_type")[
            "price_sqm_median"
        ].transform("last")

        monthly["discount_index"] = monthly["price_sqm_median"] / monthly["base_value"]

        self.discount_map_ = monthly[["market_type", "period", "discount_index"]]

        return self

    def transform(self, df):

        # Проверяем, был ли вызван fit
        if self.discount_map_ is None:
            raise RuntimeError(
                "Этот экземпляр PriceDiscounter еще не был обучен. Вызовите .fit() перед .transform()."
            )

        df_ = df.copy()
        df_["date"] = pd.to_datetime(df_[["year", "month", "day"]])
        df_["period"] = df_["date"].dt.to_period("M").dt.to_timestamp()

        # Присоединяем карту, полученную на train-данных
        df_ = df_.merge(
            self.discount_map_,
            on=["market_type", "period"],
            how="left",
        )

        # Fallback для месяцев/рынков, которых не было в train
        df_["discount_index"] = df_["discount_index"].fillna(1.0)

        # Нормализация
        df_["price_per_square_meter_normalized"] = (
            df_["price_per_square_meter"] / df_["discount_index"]
        ).astype("float32")

        df_["price_normalized"] = (
            df_["price_per_square_meter_normalized"] * df_["area"]
        ).astype("float32")

        return df_.drop(columns=["period", "discount_index"])

    def fit_transform(self, df, y=None):

        self.fit(df, y)
        return self.transform(df)


def main():
    df_train = pd.read_parquet("data/interim/split_train.parquet", engine="pyarrow")
    df_valid = pd.read_parquet("data/interim/split_valid.parquet", engine="pyarrow")
    df_test = pd.read_parquet("data/interim/split_test.parquet", engine="pyarrow")

    price_discounter = PriceDiscounter(min_obs=30)

    train_processed = price_discounter.fit_transform(df_train).drop(
        columns=["price", "price_per_square_meter"]
    )
    valid_processed = price_discounter.transform(df_valid).drop(
        columns=["price", "price_per_square_meter"]
    )
    test_processed = price_discounter.transform(df_test).drop(
        columns=["price", "price_per_square_meter"]
    )

    train_processed.to_parquet(
        "data/interim/price_discount_train.parquet", index=False, engine="pyarrow"
    )
    valid_processed.to_parquet(
        "data/interim/price_discount_valid.parquet", index=False, engine="pyarrow"
    )
    test_processed.to_parquet(
        "data/interim/price_discount_test.parquet", index=False, engine="pyarrow"
    )


if __name__ == "__main__":
    main()
