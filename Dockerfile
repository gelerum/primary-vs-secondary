# Этап сборки (builder)
# Используем образ devel, так как он содержит компиляторы (nvcc) и заголовочные файлы,
# которые могут понадобиться для установки некоторых Python пакетов.
# Выберите версию CUDA, которая вам нужна. Например, 12.1.1
FROM nvidia/cuda:12.1.1-devel-ubuntu22.04 AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Устанавливаем Python 3.14, так как в базовом образе NVIDIA его нет
# Используем PPA deadsnakes для свежих версий Python
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    build-essential \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y python3.14 python3.14-dev python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
# Предполагаем, что requirements-gpu.txt тоже существует
COPY requirements-gpu.txt .

RUN python -m pip install --upgrade pip
# Устанавливаем пакеты в отдельную директорию, чтобы скопировать их на финальный этап
RUN python -m pip install --prefix=/install -r requirements.txt
RUN python -m pip install --prefix=/install -r requirements-gpu.txt


# Финальный этап (final stage)
# Используем образ runtime, он меньше по размеру, так как не содержит инструментов для разработки
FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

# Устанавливаем переменные окружения
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # Переменные для корректной работы с GPU внутри контейнера
    NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility

# Устанавливаем Python 3.14 и другие необходимые утилиты
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y python3.14 libpython3.14-stdlib \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем предустановленные Python пакеты из этапа сборки
COPY --from=builder /install /usr/local

# Копируем код приложения
COPY . .

# Пример команды для запуска
# CMD ["dvc", "repro", "--pull"]