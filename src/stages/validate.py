import argparse
import logging
from pathlib import Path

import pandas as pd
import src.validation.schemas as schemas

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True)
    parser.add_argument("--inputs", nargs="+", required=True)
    parser.add_argument("--flag-file", required=True)
    args = parser.parse_args()

    schema_name = f"{args.stage}_schema"
    if not hasattr(schemas, schema_name):
        raise ValueError(f"Схема {schema_name} не найдена в файле schemas.py!")

    schema = getattr(schemas, schema_name)

    # 2. Проверяем каждый переданный файл
    for path in args.inputs:
        logging.info(f"Проверка {path} по схеме {schema_name}...")
        df = pd.read_parquet(path)

        # Строгая валидация (если что-то не так, скрипт выбросит ошибку и DVC остановится)
        schema.validate(df)

    # 3. Если всё прошло успешно, создаем пустой файл для DVC
    Path(args.flag_file).parent.mkdir(parents=True, exist_ok=True)
    Path(args.flag_file).touch()
    logging.info(f"✅ Этап {args.stage} прошел строгую валидацию.")


if __name__ == "__main__":
    main()
