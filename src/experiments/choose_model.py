import gc
import os

import mlflow
import numpy as np
import optuna
import pandas as pd
import torch
from dvc.api import params_show

# Импорт кластеризации на PyTorch (pip install fast-pytorch-kmeans)
from fast_pytorch_kmeans import KMeans as TorchKMeans
from optuna.integration.mlflow import MLflowCallback
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# Импорт твоей функции расчета WNIR
from src.wnir.wnir import calculate_and_impute_wnir

# ==========================================
# 0. ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# ==========================================
TARGET = "price_normalized"

# Признаки (wnir_* колонки будут добавляться динамически в зависимости от пайплайна)
BASE_NUM_FEATURES = [
    "area",
    "room_count",
    "floor",
    "build_year",
    "longitude",
    "latitude",
]
BASE_CAT_FEATURES = ["administrative_district"]

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {DEVICE}")


# ==========================================
# 1. RIDGE РЕГРЕССИЯ (VRAM-optimized, PyTorch)
# ==========================================
class TorchRidge:
    """Оптимизированная L2-регрессия для работы с видеопамятью и большими данными"""

    def __init__(self, alpha=1.0, device="cuda"):
        self.alpha = alpha
        self.device = device
        self.w = None

    def fit(self, X, y):
        # Строго float32 для экономии памяти
        X_t = torch.tensor(X, dtype=torch.float32, device=self.device)
        y_t = torch.tensor(y, dtype=torch.float32, device=self.device).view(-1, 1)

        ones = torch.ones((X_t.shape[0], 1), dtype=torch.float32, device=self.device)
        X_t = torch.cat([ones, X_t], dim=1)

        D = X_t.shape[1]
        I = torch.eye(D, dtype=torch.float32, device=self.device)
        I[0, 0] = 0.0  # Не штрафуем смещение (Intercept)

        A = X_t.T @ X_t + self.alpha * I
        b = X_t.T @ y_t

        self.w = torch.linalg.solve(A, b)

        # Очистка VRAM
        del X_t, y_t, ones, I, A, b
        torch.cuda.empty_cache()

    def predict(self, X):
        X_t = torch.tensor(X, dtype=torch.float32, device=self.device)
        ones = torch.ones((X_t.shape[0], 1), dtype=torch.float32, device=self.device)
        X_t = torch.cat([ones, X_t], dim=1)

        preds = X_t @ self.w

        # Переносим результат на CPU, чтобы не засорять видеокарту массивами предсказаний
        res = preds.cpu().numpy().flatten()
        del X_t, ones, preds
        torch.cuda.empty_cache()
        return res


# ==========================================
# 2. ПОДГОТОВКА ДАННЫХ
# ==========================================
def get_preprocessor():
    """Создает ColumnTransformer для базовых признаков"""
    numeric_transformer = StandardScaler()
    categorical_transformer = OneHotEncoder(
        handle_unknown="ignore", sparse_output=False, dtype=np.float32
    )

    # dtype убран из ColumnTransformer, приведение типов будет делаться через .astype()
    return ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, BASE_NUM_FEATURES),
            ("cat", categorical_transformer, BASE_CAT_FEATURES),
        ]
    )


def load_data():
    """Загрузка данных DVC и приведение к нужному формату"""
    print("Loading prepared DVC data (up to 1.5M rows)...")
    df_train = pd.read_parquet("data/interim/wnir_all_train.parquet")
    df_valid = pd.read_parquet("data/interim/wnir_all_valid.parquet")

    df_train["date"] = pd.to_datetime(df_train["date"])
    df_valid["date"] = pd.to_datetime(df_valid["date"])

    df_train["set_type"] = "train"
    df_valid["set_type"] = "valid"
    return df_train, df_valid


# ==========================================
# 3. ЦЕЛЕВЫЕ ФУНКЦИИ OPTUNA
# ==========================================


def objective_global(trial, df_train, df_valid, preprocessor, exp_type):
    """Целевая функция для глобальных моделей (Без кластеризации)"""
    train_p = df_train[df_train["market_type"] == "primary"].copy()
    valid_p = df_valid[df_valid["market_type"] == "primary"].copy()

    # Принудительно приводим к float32 для экономии памяти
    X_train_base = preprocessor.fit_transform(train_p).astype(np.float32)
    X_valid_base = preprocessor.transform(valid_p).astype(np.float32)

    y_train = train_p[TARGET].values.astype(np.float32)
    y_valid = valid_p[TARGET].values.astype(np.float32)

    metrics = {}

    if exp_type == 1:
        # Эксперимент 1: Базовая модель
        alpha = trial.suggest_float("ridge_alpha", 1e-3, 1e3, log=True)
        model = TorchRidge(alpha=alpha, device=DEVICE)
        model.fit(X_train_base, y_train)
        preds = model.predict(X_valid_base)

    elif exp_type == 2:
        # Эксперимент 2: Предиктор-Корректор
        R = trial.suggest_categorical("R", [100, 500, 1000, 5000, 10000])
        alpha1 = trial.suggest_float("ridge_alpha1", 1e-3, 1e3, log=True)
        alpha2 = trial.suggest_float("ridge_alpha2", 1e-3, 1e3, log=True)

        proxy_col = f"wnir_p_value_{R}_all"
        y_train_proxy = train_p[proxy_col].fillna(0).values.astype(np.float32)
        y_valid_proxy = valid_p[proxy_col].fillna(0).values.astype(np.float32)

        # Шаг 1: Предсказываем Proxy WNIR
        model1 = TorchRidge(alpha=alpha1, device=DEVICE)
        model1.fit(X_train_base, y_train_proxy)
        pred_proxy_tr = model1.predict(X_train_base).reshape(-1, 1)
        pred_proxy_va = model1.predict(X_valid_base).reshape(-1, 1)

        metrics["valid_proxy_rmse"] = mean_squared_error(
            y_valid_proxy, pred_proxy_va, squared=False
        )

        # Шаг 2: Предсказываем Цену
        X_train_step2 = np.hstack([X_train_base, pred_proxy_tr])
        X_valid_step2 = np.hstack([X_valid_base, pred_proxy_va])

        model2 = TorchRidge(alpha=alpha2, device=DEVICE)
        model2.fit(X_train_step2, y_train)
        preds = model2.predict(X_valid_step2)

    # Основные метрики для цены
    metrics["valid_rmse"] = mean_squared_error(y_valid, preds, squared=False)
    metrics["valid_mae"] = mean_absolute_error(y_valid, preds)
    metrics["valid_mape"] = mean_absolute_percentage_error(y_valid, preds)
    metrics["valid_r2"] = r2_score(y_valid, preds)

    # Очистка ОЗУ
    del train_p, valid_p, X_train_base, X_valid_base
    gc.collect()

    return metrics


def objective_cluster(trial, df_train, df_valid, preprocessor, exp_type, wnir_params):
    """Целевая функция для кластерных моделей"""

    # 1. Подготовка фичей для кластеризации (строго без цены и WNIR) + float32
    X_train_all = preprocessor.fit_transform(df_train).astype(np.float32)
    X_valid_all = preprocessor.transform(df_valid).astype(np.float32)

    n_clusters = trial.suggest_int("n_clusters", 3, 20)

    # Кластеризация на видеокарте
    kmeans = TorchKMeans(
        n_clusters=n_clusters, mode="euclidean", verbose=0, max_iter=100
    )
    X_tr_t = torch.tensor(X_train_all, dtype=torch.float32, device=DEVICE)
    X_va_t = torch.tensor(X_valid_all, dtype=torch.float32, device=DEVICE)

    df_train["cluster"] = kmeans.fit_predict(X_tr_t).cpu().numpy()
    df_valid["cluster"] = kmeans.predict(X_va_t).cpu().numpy()

    # Быстрая очистка VRAM от тяжелых тензоров кластеризации
    del X_tr_t, X_va_t, X_train_all, X_valid_all, kmeans
    torch.cuda.empty_cache()

    # 2. Инициализация глобальных векторов для сбора предсказаний
    mask_valid_primary = df_valid["market_type"] == "primary"
    len_valid_p = mask_valid_primary.sum()

    valid_preds = np.zeros(len_valid_p, dtype=np.float32)
    valid_true = df_valid[mask_valid_primary][TARGET].values.astype(np.float32)

    if exp_type in [4, 5.2]:
        valid_proxy_preds = np.zeros(len_valid_p, dtype=np.float32)
        valid_proxy_true = np.zeros(len_valid_p, dtype=np.float32)

    # Запрашиваем параметры регрессии у Optuna (одинаковые гиперпараметры для всех кластеров)
    alpha1 = trial.suggest_float("ridge_alpha1", 1e-3, 1e3, log=True)
    if exp_type in [4, 5.2]:
        alpha2 = trial.suggest_float("ridge_alpha2", 1e-3, 1e3, log=True)
        R = trial.suggest_categorical("R", [100, 500, 1000, 5000, 10000])

    valid_idx_offset = 0

    # 3. Итерация по кластерам
    for c in range(n_clusters):
        c_train_all = df_train[df_train["cluster"] == c].copy()
        c_valid_all = df_valid[df_valid["cluster"] == c].copy()

        # ЭКСПЕРИМЕНТ 5: Внутрикластерный расчет WNIR
        if exp_type in [5.1, 5.2]:
            c_combined = (
                pd.concat([c_train_all, c_valid_all])
                .sort_values("date")
                .reset_index(drop=True)
            )

            new_wnir_cluster = calculate_and_impute_wnir(
                df_group=c_combined,
                Rs=list(wnir_params["R"].values()),
                h=wnir_params["h"],
                batch_size=wnir_params.get("batch_size", 20000),
                suffix="cluster",
                device=DEVICE,
                fill_nearest_threshold=wnir_params["fill_nearest_threshold"],
            )
            c_combined = c_combined.join(new_wnir_cluster)
            c_train_all = c_combined[c_combined["set_type"] == "train"]
            c_valid_all = c_combined[c_combined["set_type"] == "valid"]

            del c_combined, new_wnir_cluster
            gc.collect()
            torch.cuda.empty_cache()

        # Фильтруем данные кластера до Primary (для обучения цены)
        c_train_p = c_train_all[c_train_all["market_type"] == "primary"]
        c_valid_p = c_valid_all[c_valid_all["market_type"] == "primary"]

        idx_start = valid_idx_offset
        idx_end = valid_idx_offset + len(c_valid_p)
        valid_idx_offset = idx_end

        # Обработка пустых или микро-кластеров (защита от краша)
        if len(c_train_p) < 5 or len(c_valid_p) == 0:
            if len(c_valid_p) > 0:
                mean_price = (
                    c_train_p[TARGET].mean()
                    if len(c_train_p) > 0
                    else df_train[TARGET].mean()
                )
                valid_preds[idx_start:idx_end] = mean_price
            continue

        # Приводим к float32 матрицы признаков внутри кластера
        X_tr_base = preprocessor.transform(c_train_p).astype(np.float32)
        X_va_base = preprocessor.transform(c_valid_p).astype(np.float32)
        y_tr = c_train_p[TARGET].values.astype(np.float32)

        # Формирование матриц признаков (Base + WNIR features)
        if exp_type == 3:
            X_tr, X_va = X_tr_base, X_va_base

        elif exp_type == 4:
            wnir_cols = [
                col
                for col in c_train_p.columns
                if col.startswith("wnir_") and col.endswith("_all")
            ]
            X_tr = np.hstack(
                [X_tr_base, c_train_p[wnir_cols].fillna(0).values.astype(np.float32)]
            )
            X_va = np.hstack(
                [X_va_base, c_valid_p[wnir_cols].fillna(0).values.astype(np.float32)]
            )

        elif exp_type in [5.1, 5.2]:
            wnir_all = [
                col
                for col in c_train_p.columns
                if col.startswith("wnir_") and col.endswith("_all")
            ]
            wnir_cluster = [
                col
                for col in c_train_p.columns
                if col.startswith("wnir_s_") and col.endswith("_cluster")
            ]
            all_wnir = wnir_all + wnir_cluster
            X_tr = np.hstack(
                [X_tr_base, c_train_p[all_wnir].fillna(0).values.astype(np.float32)]
            )
            X_va = np.hstack(
                [X_va_base, c_valid_p[all_wnir].fillna(0).values.astype(np.float32)]
            )

        # Обучение
        if exp_type in [3, 5.1]:
            # Прямое предсказание цены
            model = TorchRidge(alpha=alpha1, device=DEVICE)
            model.fit(X_tr, y_tr)
            valid_preds[idx_start:idx_end] = model.predict(X_va)

        elif exp_type in [4, 5.2]:
            # Предиктор-Корректор
            suffix = "all" if exp_type == 4 else "cluster"
            proxy_col = f"wnir_p_value_{R}_{suffix}"

            y_tr_proxy = c_train_p[proxy_col].fillna(0).values.astype(np.float32)
            y_va_proxy = c_valid_p[proxy_col].fillna(0).values.astype(np.float32)

            model1 = TorchRidge(alpha=alpha1, device=DEVICE)
            model1.fit(X_tr, y_tr_proxy)
            pred_proxy_tr = model1.predict(X_tr).reshape(-1, 1)
            pred_proxy_va = model1.predict(X_va).reshape(-1, 1)

            valid_proxy_preds[idx_start:idx_end] = pred_proxy_va.flatten()
            valid_proxy_true[idx_start:idx_end] = y_va_proxy

            X_tr_step2 = np.hstack([X_tr, pred_proxy_tr])
            X_va_step2 = np.hstack([X_va, pred_proxy_va])

            model2 = TorchRidge(alpha=alpha2, device=DEVICE)
            model2.fit(X_tr_step2, y_tr)
            valid_preds[idx_start:idx_end] = model2.predict(X_va_step2)

        # Локальная очистка кластера
        del X_tr, X_va, c_train_p, c_valid_p, c_train_all, c_valid_all
        gc.collect()

    # 4. Сборка финальных глобальных метрик (со всех кластеров)
    metrics = {
        "valid_rmse": mean_squared_error(valid_true, valid_preds, squared=False),
        "valid_mae": mean_absolute_error(valid_true, valid_preds),
        "valid_mape": mean_absolute_percentage_error(valid_true, valid_preds),
        "valid_r2": r2_score(valid_true, valid_preds),
    }

    if exp_type in [4, 5.2]:
        metrics["valid_proxy_rmse"] = mean_squared_error(
            valid_proxy_true, valid_proxy_preds, squared=False
        )

    return metrics


# ==========================================
# 4. ДВИЖОК ЭКСПЕРИМЕНТОВ
# ==========================================
def run_experiment(
    exp_name, exp_type, df_train, df_valid, preprocessor, wnir_params, n_trials=20
):
    print(f"\n{'=' * 60}\nRunning: {exp_name}\n{'=' * 60}")

    study = optuna.create_study(direction="minimize", study_name=exp_name)
    mlflow_cb = MLflowCallback(tracking_uri="mlruns", metric_name="valid_rmse")

    def objective(trial):
        # Nested Run: логирует каждый подбор гиперпараметров внутрь родительского эксперимента
        with mlflow.start_run(nested=True, run_name=f"Trial_{trial.number}"):
            mlflow.log_param("exp_type", exp_type)

            if exp_type in [1, 2]:
                metrics_dict = objective_global(
                    trial, df_train, df_valid, preprocessor, exp_type
                )
            else:
                metrics_dict = objective_cluster(
                    trial, df_train, df_valid, preprocessor, exp_type, wnir_params
                )

            # Логируем ВСЕ метрики (RMSE, MAE, MAPE, R2, Proxy_RMSE)
            mlflow.log_metrics(metrics_dict)

            # Глобальная очистка между триалами
            gc.collect()
            torch.cuda.empty_cache()

            return metrics_dict["valid_rmse"]

    study.optimize(objective, n_trials=n_trials, callbacks=[mlflow_cb])
    print(f"[{exp_name}] Finished! Best RMSE: {study.best_value:.4f}")


# ==========================================
# 5. ТОЧКА ВХОДА
# ==========================================
if __name__ == "__main__":
    # Установка имени проекта в MLflow
    mlflow.set_experiment("Real_Estate_Pricing_Pipelines")

    # Чтение параметров DVC для функции WNIR
    try:
        wnir_params = params_show()["wnir"]
    except Exception as e:
        print(f"Warning: Could not load DVC params ({e}), using defaults.")
        wnir_params = {
            "h": 500,
            "R": {"r1": 100, "r2": 500, "r3": 1000},
            "fill_nearest_threshold": 3000,
            "batch_size": 20000,
        }

    # Подготовка
    df_train, df_valid = load_data()
    preprocessor = get_preprocessor()

    # Описание дерева экспериментов
    experiments = [
        ("Exp_1_Global_Baseline", 1),
        ("Exp_2_Global_Corrector", 2),
        ("Exp_3_Cluster_Baseline", 3),
        ("Exp_4_Cluster_Corrector_GlobalWNIR", 4),
        ("Exp_5.1_Cluster_Direct_MacroMicro", 5.1),
        ("Exp_5.2_Cluster_Corrector_MacroMicro", 5.2),
    ]

    # Запуск
    for exp_name, exp_type in experiments:
        # Parent Run для группировки
        with mlflow.start_run(run_name=exp_name):
            run_experiment(
                exp_name=exp_name,
                exp_type=exp_type,
                df_train=df_train,
                df_valid=df_valid,
                preprocessor=preprocessor,
                wnir_params=wnir_params,
                n_trials=10,  # <--- Измените это число для управления длительностью
            )

    print("\nAll experiments finished. Run 'mlflow ui' to view the dashboard.")
