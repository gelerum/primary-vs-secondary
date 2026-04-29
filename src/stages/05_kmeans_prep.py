import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder


def main():
    df_train = pd.read_parquet("data/interim/04_train.parquet", engine="pyarrow")
    df_valid = pd.read_parquet("data/interim/04_valid.parquet", engine="pyarrow")
    df_test = pd.read_parquet("data/interim/04_test.parquet", engine="pyarrow")

    df_train = df_train.drop(
        columns=["price", "price_per_square_meter", "date", "market_type"]
    )
    df_valid = df_valid.drop(
        columns=["price", "price_per_square_meter", "date", "market_type"]
    )
    df_test = df_test.drop(
        columns=["price", "price_per_square_meter", "date", "market_type"]
    )

    num_cols = df_train.select_dtypes(include="number").columns
    cat_cols = df_train.select_dtypes(include="category").columns

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), num_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
        ]
    )

    X_train = preprocessor.fit_transform(df_train)
    X_valid = preprocessor.transform(df_valid)
    X_test = preprocessor.transform(df_test)

    feature_names = preprocessor.get_feature_names_out()

    df_train_out = pd.DataFrame(X_train, columns=feature_names)
    df_valid_out = pd.DataFrame(X_valid, columns=feature_names)
    df_test_out = pd.DataFrame(X_test, columns=feature_names)

    df_train_out.to_parquet(
        "data/interim/05_train.parquet", index=False, engine="pyarrow"
    )
    df_valid_out.to_parquet(
        "data/interim/05_valid.parquet", index=False, engine="pyarrow"
    )
    df_test_out.to_parquet(
        "data/interim/05_test.parquet", index=False, engine="pyarrow"
    )


if __name__ == "__main__":
    main()
