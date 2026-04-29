# main.py

import pandas as pd
from dvc.api import params_show
import gc

# Импортируем новую функцию из нашего модуля
from src.wnir.wnir import process_markets_in_batches


def main():
    """
    Главная функция для оркестрации расчета временного WNIR.
    1. Загружает и сортирует данные по дате.
    2. Разделяет на первичный и вторичный рынки.
    3. Итерируется по радиусам, запуская пакетную обработку для каждого.
    4. Собирает и сохраняет финальный результат.
    """
    print("Starting WNIR stage...")
    params = params_show()["04_wnir"]
    h = params["h"]
    Rs = list(params["R"].values())
    batch_size = params.get(
        "batch_size", 20000
    )  # Размер пакета, можно вынести в params.yaml

    print("Loading and preparing data...")
    # Предполагается, что в данных есть колонка 'date' или 'created_at'
    # Убедитесь, что она имеет формат datetime
    df = pd.read_parquet("data/interim/03_price_discounted.parquet")

    # ВАЖНО: Сортировка по дате - ключевой шаг для корректного расчета
    if "date" not in df.columns:
        raise ValueError(
            "DataFrame must contain a 'date' column for temporal processing."
        )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date", ascending=True).reset_index(drop=True)

    df_primary = df[df["market_type"] == "primary"].copy()
    df_secondary = df[df["market_type"] == "secondary"].copy()

    # Удаляем большой исходный DataFrame для освобождения памяти
    del df
    gc.collect()

    print(f"Primary market size: {len(df_primary)}")
    print(f"Secondary market size: {len(df_secondary)}")
    print(f"Processing with batch size: {batch_size}")

    # Создаем DataFrame для хранения результатов, чтобы не модифицировать исходные
    # и избежать проблем с индексами при объединении
    results_primary_list = []
    results_secondary_list = []

    for r in Rs:
        print(f"\nProcessing R = {r} ...")

        # Запускаем основную функцию пакетной обработки
        res_p, res_s = process_markets_in_batches(
            df_primary, df_secondary, R=r, h=h, batch_size=batch_size
        )

        # Собираем результаты для каждого R
        # Сохраняем только новые колонки, чтобы не дублировать данные
        new_cols_p = [col for col in res_p.columns if col.endswith(f"_{r}")]
        results_primary_list.append(res_p[new_cols_p])

        new_cols_s = [col for col in res_s.columns if col.endswith(f"_{r}")]
        results_secondary_list.append(res_s[new_cols_s])

    print("\nCombining final results...")
    # Объединяем результаты всех R с исходными данными
    if results_primary_list:
        df_primary = df_primary.join(pd.concat(results_primary_list, axis=1))
    if results_secondary_list:
        df_secondary = df_secondary.join(pd.concat(results_secondary_list, axis=1))

    # Финальное объединение и сохранение
    df_wnir = pd.concat([df_primary, df_secondary], ignore_index=False)

    # Вернем исходный порядок, если это необходимо
    df_wnir = df_wnir.sort_index()

    print("Saving data to parquet...")
    df_wnir.to_parquet("data/interim/04_wnir.parquet", index=False)

    print("WNIR stage completed successfully.")


if __name__ == "__main__":
    main()
