# Перед клонированием
Этот репозиторий использует **Git LFS (Large File Storage)** для датасетов в директории `data`.

Перед тем как клонировать репозиторий установите **Git LFS**.

**Ubuntu**
```
sudo apt install git-lfs
```
**Windows**
Download from: https://git-lfs.github.com/

Теперь можно клонировать
```
git clone git@github.com:gelerum/primary-vs-secondary.git
```

Если датасеты не скачались автоматически:
```
git lfs pull
```

# Перед использованием репозитория
Перед запускам нужно положить следующие файлы в директорию `secrets`, если планируется парсинг геоточек.

```bash
├── secrets
│   └── geocoding
│       └── ya_api_keys.csv
```

# Запуск пайплайна для обработки датасетов
```
pip install -r requirements.txt

python -m src.process_data.py
```