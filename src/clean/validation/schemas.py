import pandera as pa
from pandera import Check, Column, DataFrameSchema

# 1. Чтобы не писать 40 колонок руками, составим их список
wnir_cols = [
    "wnir_p_value_100_all",
    "wnir_s_value_100_all",
    "wnir_s_mean_100_all",
    "wnir_s_std_100_all",
    "wnir_s_min_100_all",
    "wnir_s_max_100_all",
    "wnir_s_median_100_all",
    "wnir_s_count_100_all",
    "wnir_p_value_500_all",
    "wnir_s_value_500_all",
    "wnir_s_mean_500_all",
    "wnir_s_std_500_all",
    "wnir_s_min_500_all",
    "wnir_s_max_500_all",
    "wnir_s_median_500_all",
    "wnir_s_count_500_all",
    "wnir_p_value_1000_all",
    "wnir_s_value_1000_all",
    "wnir_s_mean_1000_all",
    "wnir_s_std_1000_all",
    "wnir_s_min_1000_all",
    "wnir_s_max_1000_all",
    "wnir_s_median_1000_all",
    "wnir_s_count_1000_all",
    "wnir_p_value_5000_all",
    "wnir_s_value_5000_all",
    "wnir_s_mean_5000_all",
    "wnir_s_std_5000_all",
    "wnir_s_min_5000_all",
    "wnir_s_max_5000_all",
    "wnir_s_median_5000_all",
    "wnir_s_count_5000_all",
    "wnir_p_value_10000_all",
    "wnir_s_value_10000_all",
    "wnir_s_mean_10000_all",
    "wnir_s_std_10000_all",
    "wnir_s_min_10000_all",
    "wnir_s_max_10000_all",
    "wnir_s_median_10000_all",
    "wnir_s_count_10000_all",
]

# 2. Описываем базовые (не wnir) колонки со строгими типами
columns = {
    "longitude": Column(pa.Float32, nullable=False),
    "latitude": Column(pa.Float32, nullable=False),
    "area": Column(pa.Float32, nullable=False),
    "room_count": Column(pa.UInt8, nullable=False),
    "floor": Column(pa.Int16, nullable=False),
    # Жесткая проверка: только primary или secondary
    "market_type": Column(
        pa.Category, Check.isin(["primary", "secondary"]), nullable=False
    ),
    "build_year": Column(pa.UInt16, nullable=False),
    "date": Column(pa.DateTime, nullable=False),
    "year": Column(pa.UInt16, nullable=False),
    "month": Column(pa.UInt8, nullable=False),
    "day": Column(pa.UInt8, nullable=False),
    "administrative_district": Column(pa.Category, nullable=False),
    "price_per_square_meter_normalized": Column(pa.Float32, nullable=False),
    "price_normalized": Column(pa.Float32, nullable=False),
}

# 3. Добавляем wnir колонки в словарь columns.
# nullable=True ОБЯЗАТЕЛЕН, так как в этих колонках физически будут NaN (для secondary).
# А логику, ГДЕ ИМЕННО они могут быть, мы опишем ниже.
for col in wnir_cols:
    columns[col] = Column(pa.Float32, nullable=True)

# 4. Собираем финальную строгую схему
wnir_all_schema = DataFrameSchema(
    columns=columns,
    # Проверки на уровне всего датафрейма (условие на пересечение колонок)
    checks=[
        Check(
            # Логика: Ячейка является NaN ТОГДА И ТОЛЬКО ТОГДА, когда market_type == "secondary".
            # (Использование c=col нужно для правильного замыкания в цикле Python)
            lambda df, c=col: df[c].isna() == (df["market_type"] == "secondary"),
            name=f"strict_nan_logic_{col}",
            error=f"Ошибка логики: {col} должен быть NaN для secondary и НЕ NaN для primary!",
        )
        for col in wnir_cols
    ],
    # Строгое соответствие: если появится колонка, которой нет в списке выше — скрипт упадет
    strict=True,
)
