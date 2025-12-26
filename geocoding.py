import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from threading import Thread
import time


YANDEX_URL = "https://geocode-maps.yandex.ru/1.x/"
SAVE_COLUMNS = ["address", "latitude", "longitude"]

BATCH_SIZE = 100  # сколько адресов обрабатывает поток за раз
SAVE_EVERY = 1000  # как часто писать на диск (строк)


def _parse_yandex_response(data: dict):
    try:
        pos = data["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"][
            "Point"
        ]["pos"]
        lon, lat = map(float, pos.split())
        return lat, lon
    except Exception:
        return None, None


def geocode_df_yandex(
    df: pd.DataFrame,
    api_keys: list[str],
    address_column: str = "address",
    city: str = "Москва",
    country: str = "Россия",
    checkpoint_path: str = "yandex_geocode_checkpoint.parquet",
) -> pd.DataFrame:
    """
    МАКСИМАЛЬНО БЫСТРАЯ версия.
    1 поток = 1 API-ключ.
    """

    if address_column not in df.columns:
        raise ValueError(f"В DataFrame должна быть колонка '{address_column}'")

    df = df.copy()
    df[address_column] = df[address_column].astype(str)

    checkpoint_path = Path(checkpoint_path)

    # --- загрузка чекпоинта ---
    if checkpoint_path.exists():
        result = pd.read_parquet(checkpoint_path)
        processed = set(result["address"])
    else:
        result = pd.DataFrame(columns=SAVE_COLUMNS)
        processed = set()

    addresses = [a for a in df[address_column] if a not in processed]
    total = len(addresses)

    print(f"▶ К обработке: {total} адресов")

    # --- делим адреса между ключами ---
    chunks = [addresses[i :: len(api_keys)] for i in range(len(api_keys))]

    queue = Queue()
    stop_token = object()

    # =========================
    # WRITER THREAD
    # =========================

    def writer():
        nonlocal result
        buffer = []
        written = len(result)
        start_time = time.time()

        while True:
            item = queue.get()
            if item is stop_token:
                break

            buffer.extend(item)

            if len(buffer) >= SAVE_EVERY:
                result = pd.concat([result, pd.DataFrame(buffer)], ignore_index=True)
                result.to_parquet(checkpoint_path)
                written += len(buffer)
                buffer.clear()

                elapsed = time.time() - start_time
                rps = written / elapsed if elapsed else 0
                eta = (total - written) / rps / 60 if rps else float("inf")

                print(f"✓ {written}/{total} | {rps:.1f} req/s | ETA ≈ {eta:.1f} мин")

        if buffer:
            result = pd.concat([result, pd.DataFrame(buffer)], ignore_index=True)
            result.to_parquet(checkpoint_path)

    writer_thread = Thread(target=writer, daemon=True)
    writer_thread.start()

    # =========================
    # WORKERS
    # =========================

    def worker(api_key: str, chunk: list[str]):
        session = requests.Session()
        batch = []

        for address in chunk:
            full_address = f"{address}, {city}, {country}"

            params = {
                "apikey": api_key,
                "geocode": full_address,
                "format": "json",
                "lang": "ru_RU",
                "results": 1,
            }

            try:
                r = session.get(YANDEX_URL, params=params, timeout=10)
                lat, lon = _parse_yandex_response(r.json())
            except Exception:
                lat, lon = None, None

            batch.append({"address": address, "latitude": lat, "longitude": lon})

            if len(batch) >= BATCH_SIZE:
                queue.put(batch)
                batch = []

        if batch:
            queue.put(batch)

    # =========================
    # EXECUTION
    # =========================

    with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
        for api_key, chunk in zip(api_keys, chunks):
            executor.submit(worker, api_key, chunk)

    queue.put(stop_token)
    writer_thread.join()

    print("✅ Геокодирование завершено")

    return result
