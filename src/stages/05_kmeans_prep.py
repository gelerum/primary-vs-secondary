import pandas as pd
import cupy  # Полезно для проверки, что CUDA доступна

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.decomposition import PCA

# pip install cudf-cu11 cuml-cu11 --extra-index-url=https://pypi.ngc.nvidia.com
# Но лучше устанавливать через conda, как описано выше.

# Изменено: Импортируем UMAP из библиотеки cuml для GPU-ускорения
from cuml.manifold import UMAP

# from umap import UMAP # <-- Старый импорт с CPU-версии


def main():
    # Проверка доступности GPU
    try:
        cupy.cuda.runtime.getDeviceCount()
        print("CUDA GPU найдена. Вычисления UMAP будут на GPU.")
    except cupy.cuda.runtime.CUDARuntimeError:
        print(
            "Ошибка: CUDA GPU не найдена. Убедитесь, что драйверы и CUDA установлены."
        )
        print("Продолжение невозможно без GPU для cuml.UMAP.")
        return

    # Загрузка данных
    df_train = pd.read_parquet("data/interim/04_train.parquet", engine="pyarrow")
    df_valid = pd.read_parquet("data/interim/04_valid.parquet", engine="pyarrow")
    df_test = pd.read_parquet("data/interim/04_test.parquet", engine="pyarrow")

    # Удаление лишних колонок
    drop_cols = ["price", "price_per_square_meter", "date", "market_type"]
    df_train = df_train.drop(columns=drop_cols)
    df_valid = df_valid.drop(columns=drop_cols)
    df_test = df_test.drop(columns=drop_cols)

    # Определение типов колонок
    num_cols = df_train.select_dtypes(include="number").columns
    cat_cols = df_train.select_dtypes(include="category").columns

    # 1. Базовая предобработка
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

    # 2. Создание Pipeline с PCA и UMAP
    # PCA (sklearn) работает на CPU, его результат (Numpy массив) будет
    # автоматически передан в cuml.UMAP, который перенесет его на GPU.
    full_pipeline = Pipeline(
        [
            ("preprocessor", preprocessor),
            ("pca", PCA(n_components=0.95, random_state=42)),
            (
                "umap",
                # Изменено: Используем cuml.UMAP. API очень похож.
                UMAP(n_components=10, n_neighbors=15, min_dist=0.1, random_state=42),
            ),
        ]
    )

    # Обучение и трансформация
    # ВНИМАНИЕ: cuml.UMAP.transform тоже может быть не очень быстрым,
    # но значительно быстрее CPU-версии на больших данных.
    print("Начинаем обучение и трансформацию pipeline...")
    X_train = full_pipeline.fit_transform(df_train)
    print("Трансформация train завершена.")
    X_valid = full_pipeline.transform(df_valid)
    print("Трансформация valid завершена.")
    X_test = full_pipeline.transform(df_test)
    print("Трансформация test завершена.")

    # Названия колонок (например, umap0, umap1)
    feature_names = [f"umap_{i}" for i in range(X_train.shape[1])]

    # Создание результирующих DataFrame
    # Важно: Результат от cuml.UMAP - это CuPy массив (живет на GPU).
    # Чтобы создать из него Pandas DataFrame, его нужно сначала перенести на CPU
    # с помощью метода .get() (который преобразует его в Numpy массив).
    df_train_out = pd.DataFrame(X_train.get(), columns=feature_names)
    df_valid_out = pd.DataFrame(X_valid.get(), columns=feature_names)
    df_test_out = pd.DataFrame(X_test.get(), columns=feature_names)

    # Сохранение
    df_train_out.to_parquet("data/interim/05_train.parquet", index=False)
    df_valid_out.to_parquet("data/interim/05_valid.parquet", index=False)
    df_test_out.to_parquet("data/interim/05_test.parquet", index=False)

    print(f"UMAP (GPU) завершен. Форма выходных данных: {X_train.shape}")


if __name__ == "__main__":
    main()
