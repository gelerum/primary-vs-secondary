import argparse
import os
import joblib
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Apply clustering model to a dataset.")
    parser.add_argument(
        "input", type=str, help="Path to input parquet file (valid or test)"
    )
    parser.add_argument(
        "output", type=str, help="Path to save the parquet file with clusters"
    )

    args = parser.parse_args()

    # 1. Загрузка данных
    print(f"Loading data from {args.input}...")
    df = pd.read_parquet(args.input)

    # 2. Загрузка сохраненных моделей
    # Эти файлы должны быть созданы на этапе ksearch
    preprocessor = joblib.load("models/preprocessor.joblib")
    model = joblib.load("models/kmeans_model.joblib")

    # 3. Препроцессинг
    # ВАЖНО: используем transform(), а не fit_transform(),
    # чтобы использовать параметры (среднее, отклонение и категории) из трейна.
    print("Preprocessing data...")
    X_processed = preprocessor.transform(df)

    # 4. Предсказание кластеров
    print("Predicting clusters...")
    df["cluster"] = model.predict(X_processed)

    # 5. Сохранение результата
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_parquet(args.output)
    print(f"Saved results with clusters to {args.output}")


if __name__ == "__main__":
    main()
