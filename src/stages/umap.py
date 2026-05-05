from dvc.api import params_show
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# Replace standard umap with cuml
from cuml.manifold import UMAP


def main():
    params = params_show()["umap"]
    n_comp = params["umap_n_components"]

    print("Loading interim datasets...")
    df_train = pd.read_parquet("data/interim/price_discount_train.parquet")
    df_valid = pd.read_parquet("data/interim/price_discount_valid.parquet")
    df_test = pd.read_parquet("data/interim/price_discount_test.parquet")

    date_cols = ["date", "year", "month", "day"]
    price_cols = ["price_per_square_meter_normalized", "price_normalized"]
    cols_to_exclude = date_cols + price_cols + ["market_type"]

    num_cols = (
        df_train.select_dtypes(include="number")
        .columns.drop(cols_to_exclude, errors="ignore")
        .tolist()
    )
    cat_cols = (
        df_train.select_dtypes(include="category")
        .columns.drop(cols_to_exclude, errors="ignore")
        .tolist()
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), num_cols),
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                cat_cols,
            ),
        ]
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "umap",
                UMAP(
                    n_components=n_comp,
                    n_neighbors=30,
                    random_state=42,
                    verbose=True,
                ),
            ),
        ]
    )

    print(f"Fitting GPU UMAP pipeline ({n_comp} components)...")
    X_train = df_train.drop(columns=cols_to_exclude, errors="ignore")
    # cuML handles the conversion from NumPy/Pandas to GPU memory automatically
    umap_train = pipeline.fit_transform(X_train)

    print("Transforming validation and test sets on GPU...")
    umap_valid = pipeline.transform(
        df_valid.drop(columns=cols_to_exclude, errors="ignore")
    )
    umap_test = pipeline.transform(
        df_test.drop(columns=cols_to_exclude, errors="ignore")
    )

    umap_col_names = [f"umap_{i + 1}" for i in range(n_comp)]

    def create_final_df(original_df, umap_array, col_names):
        meta = original_df[cols_to_exclude]
        # umap_array returned by cuML might be a CuPy array;
        # convert to numpy if needed, though pd.DataFrame usually handles it.
        umap_df = pd.DataFrame(umap_array, columns=col_names, index=original_df.index)
        return pd.concat([meta, umap_df], axis=1)

    df_train_final = create_final_df(df_train, umap_train, umap_col_names)
    df_valid_final = create_final_df(df_valid, umap_valid, umap_col_names)
    df_test_final = create_final_df(df_test, umap_test, umap_col_names)

    print("Saving processed datasets to data/interim/...")
    df_train_final.to_parquet("data/interim/umap_train.parquet")
    df_valid_final.to_parquet("data/interim/umap_valid.parquet")
    df_test_final.to_parquet("data/interim/umap_test.parquet")


if __name__ == "__main__":
    main()
