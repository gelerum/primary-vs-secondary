import gc
import os

import mlflow
import numpy as np
import optuna
import pandas as pd
import torch
from dvc.api import params_show

# Импорт кластеризации на PyTorch
from fast_pytorch_kmeans import KMeans as TorchKMeans
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
TARGET = "price_per_square_meter_normalized"
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
# 1. RIDGE РЕГРЕССИЯ (GPU)
# ==========================================
class TorchRidge:
    def __init__(self, alpha=1.0, device="cuda"):
        self.alpha = alpha
        self.device = device
        self.w = None

    def fit(self, X, y):
        X_t = torch.tensor(X, dtype=torch.float32, device=self.device)
        y_t = torch.tensor(y, dtype=torch.float32, device=self.device).view(-1, 1)

        ones = torch.ones((X_t.shape[0], 1), dtype=torch.float32, device=self.device)
        X_t = torch.cat([ones, X_t], dim=1)

        D = X_t.shape[1]
        I = torch.eye(D, dtype=torch.float32, device=self.device)
        I[0, 0] = 0.0

        A = X_t.T @ X_t + self.alpha * I
        b = X_t.T @ y_t

        self.w = torch.linalg.lstsq(A, b).solution
        self.w = torch.nan_to_num(self.w, nan=0.0, posinf=0.0, neginf=0.0)

        del X_t, y_t, ones, I, A, b
        torch.cuda.empty_cache()

    def predict(self, X):
        X_t = torch.tensor(X, dtype=torch.float32, device=self.device)
        ones = torch.ones((X_t.shape[0], 1), dtype=torch.float32, device=self.device)
        X_t = torch.cat([ones, X_t], dim=1)

        preds = X_t @ self.w
        res = preds.cpu().numpy().flatten()

        del X_t, ones, preds
        torch.cuda.empty_cache()
        return res


# ==========================================
# 2. ПОДГОТОВКА ДАННЫХ
# ==========================================
def get_preprocessor():
    numeric_transformer = StandardScaler()
    categorical_transformer = OneHotEncoder(
        handle_unknown="ignore", drop="first", sparse_output=False
    )
    return ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, BASE_NUM_FEATURES),
            ("cat", categorical_transformer, BASE_CAT_FEATURES),
        ]
    )


def load_data():
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
    """
    БЛОК 1: ГЛОБАЛЬНЫЕ МОДЕЛИ (1.1, 1.2, 1.3, 1.4)
    """
    train_p = df_train[df_train["market_type"] == "primary"].copy()
    valid_p = df_valid[df_valid["market_type"] == "primary"].copy()

    X_train_base = preprocessor.fit_transform(train_p).astype(np.float32)
    X_valid_base = preprocessor.transform(valid_p).astype(np.float32)

    y_train = train_p[TARGET].values.astype(np.float32)
    y_valid = valid_p[TARGET].values.astype(np.float32)

    metrics = {}

    # Запрашиваем параметры
    alpha1 = trial.suggest_float("ridge_alpha1", 1e-3, 1e3, log=True)
    if exp_type in [1.3, 1.4]:
        alpha2 = trial.suggest_float("ridge_alpha2", 1e-3, 1e3, log=True)
        R = trial.suggest_categorical("R", [100, 500, 1000, 5000, 10000])

    # Сборка фичей для Шага 1
    if exp_type in [1.1, 1.3]:
        # БЕЗ wnir_s
        X_tr_step1, X_va_step1 = X_train_base, X_valid_base
    else:  # 1.2, 1.4
        # С wnir_s
        s_cols = [
            c for c in train_p.columns if c.startswith("wnir_s_") and c.endswith("_all")
        ]
        X_tr_s = train_p[s_cols].fillna(0).values.astype(np.float32)
        X_va_s = valid_p[s_cols].fillna(0).values.astype(np.float32)
        X_tr_step1 = np.hstack([X_train_base, X_tr_s])
        X_va_step1 = np.hstack([X_valid_base, X_va_s])

    # Обучение
    if exp_type in [1.1, 1.2]:
        # DIRECT
        model = TorchRidge(alpha=alpha1, device=DEVICE)
        model.fit(X_tr_step1, y_train)
        preds = model.predict(X_va_step1)

    elif exp_type in [1.3, 1.4]:
        # CORRECTOR
        proxy_col = f"wnir_p_value_{R}_all"
        y_train_proxy = train_p[proxy_col].fillna(0).values.astype(np.float32)
        y_valid_proxy = valid_p[proxy_col].fillna(0).values.astype(np.float32)

        model1 = TorchRidge(alpha=alpha1, device=DEVICE)
        model1.fit(X_tr_step1, y_train_proxy)
        pred_proxy_tr = model1.predict(X_tr_step1).reshape(-1, 1)
        pred_proxy_va = model1.predict(X_va_step1).reshape(-1, 1)

        metrics["valid_proxy_rmse"] = float(
            np.sqrt(mean_squared_error(y_valid_proxy, pred_proxy_va))
        )

        X_tr_step2 = np.hstack([X_tr_step1, pred_proxy_tr])
        X_va_step2 = np.hstack([X_va_step1, pred_proxy_va])

        model2 = TorchRidge(alpha=alpha2, device=DEVICE)
        model2.fit(X_tr_step2, y_train)
        preds = model2.predict(X_va_step2)

    metrics["valid_rmse"] = float(np.sqrt(mean_squared_error(y_valid, preds)))
    metrics["valid_mae"] = float(mean_absolute_error(y_valid, preds))
    metrics["valid_mape"] = float(mean_absolute_percentage_error(y_valid, preds))
    metrics["valid_r2"] = float(r2_score(y_valid, preds))

    del train_p, valid_p, X_train_base, X_valid_base, X_tr_step1, X_va_step1
    gc.collect()

    return metrics


def objective_cluster(trial, df_train, df_valid, preprocessor, exp_type, wnir_params):
    """
    БЛОК 2 И 3: Кластерные и Супер-модели (2.1-2.4, 3.1-3.3)
    """
    # 1. Сбрасываем индексы сразу — теперь они строго от 0 до N
    df_train = df_train.reset_index(drop=True).copy()
    df_valid = df_valid.reset_index(drop=True).copy()

    global_mean_price = df_train[df_train["market_type"] == "primary"][TARGET].mean()

    # =============================================
    # 1. Кластеризация
    # =============================================
    X_train_all = preprocessor.fit_transform(df_train).astype(np.float32)
    X_valid_all = preprocessor.transform(df_valid).astype(np.float32)

    n_clusters = trial.suggest_int("n_clusters", 3, 20)

    kmeans = TorchKMeans(
        n_clusters=n_clusters, mode="euclidean", verbose=0, max_iter=100
    )
    X_tr_t = torch.tensor(X_train_all, dtype=torch.float32, device=DEVICE)
    X_va_t = torch.tensor(X_valid_all, dtype=torch.float32, device=DEVICE)

    df_train["cluster"] = kmeans.fit_predict(X_tr_t).cpu().numpy()
    df_valid["cluster"] = kmeans.predict(X_va_t).cpu().numpy()

    del X_tr_t, X_va_t, X_train_all, X_valid_all, kmeans
    torch.cuda.empty_cache()
    gc.collect()

    # =============================================
    # 2. Подготовка для валидации (ЧЕРЕЗ PANDAS SERIES)
    # =============================================
    # Сохраняем индексы "первички"
    valid_p_idx = df_valid[df_valid["market_type"] == "primary"].index

    # Массивы-пустышки с правильными индексами
    valid_preds = pd.Series(index=valid_p_idx, dtype=np.float32)

    if exp_type in [2.3, 2.4, 3.2, 3.3]:
        valid_proxy_preds = pd.Series(index=valid_p_idx, dtype=np.float32)
        valid_proxy_true = pd.Series(index=valid_p_idx, dtype=np.float32)

    alpha1 = trial.suggest_float("ridge_alpha1", 1e-3, 1e3, log=True)
    if exp_type in [2.3, 2.4, 3.2, 3.3]:
        alpha2 = trial.suggest_float("ridge_alpha2", 1e-3, 1e3, log=True)
        R = trial.suggest_categorical("R", [100, 500, 1000, 5000, 10000])

    # =============================================
    # 3. Цикл по кластерам
    # =============================================
    for c in range(n_clusters):
        c_train_all = df_train[df_train["cluster"] == c].copy()
        c_valid_all = df_valid[df_valid["cluster"] == c].copy()

        # --- Пересчёт WNIR на уровне кластера (только для Блока 3) ---
        if exp_type in [3.1, 3.2, 3.3]:
            # ВАЖНО: сохраняем оригинальные индексы перед объединением!
            c_train_all["orig_idx"] = c_train_all.index
            c_valid_all["orig_idx"] = c_valid_all.index

            c_combined = pd.concat([c_train_all, c_valid_all], ignore_index=True)
            c_combined = c_combined.sort_values("date").reset_index(drop=True)

            new_wnir_cluster = calculate_and_impute_wnir(
                df_group=c_combined,
                Rs=list(wnir_params["R"].values()),
                h=wnir_params["h"],
                batch_size=wnir_params.get("batch_size", 20000),
                suffix="cluster",
                device=DEVICE,
                fill_nearest_threshold=wnir_params["fill_nearest_threshold"],
            )

            new_wnir_cluster = new_wnir_cluster.reset_index(drop=True)
            c_combined = pd.concat([c_combined, new_wnir_cluster], axis=1)

            # Возвращаем оригинальные индексы!
            c_train_all = c_combined[c_combined["set_type"] == "train"].set_index(
                "orig_idx"
            )
            c_valid_all = c_combined[c_combined["set_type"] == "valid"].set_index(
                "orig_idx"
            )
            c_train_all.index.name = None
            c_valid_all.index.name = None

            del c_combined, new_wnir_cluster
            gc.collect()

        # =============================================
        # 4. Подготовка данных внутри кластера
        # =============================================
        c_train_p = c_train_all[c_train_all["market_type"] == "primary"]
        c_valid_p = c_valid_all[c_valid_all["market_type"] == "primary"]

        if len(c_train_p) < 5 or len(c_valid_p) == 0:
            if len(c_valid_p) > 0:
                # Запись по глобальному индексу
                valid_preds.loc[c_valid_p.index] = global_mean_price
            continue

        X_tr_base = preprocessor.transform(c_train_p).astype(np.float32)
        X_va_base = preprocessor.transform(c_valid_p).astype(np.float32)
        y_tr = c_train_p[TARGET].values.astype(np.float32)

        # =============================================
        # 5. Формирование фичей
        # =============================================
        if exp_type in [2.1, 2.3]:
            X_tr_step1, X_va_step1 = X_tr_base, X_va_base

        elif exp_type in [2.2, 2.4, 3.2]:
            s_cols = [
                col
                for col in c_train_p.columns
                if col.startswith("wnir_s_") and col.endswith("_all")
            ]
            X_tr_step1 = np.hstack(
                [X_tr_base, c_train_p[s_cols].fillna(0).values.astype(np.float32)]
            )
            X_va_step1 = np.hstack(
                [X_va_base, c_valid_p[s_cols].fillna(0).values.astype(np.float32)]
            )

        elif exp_type in [3.1, 3.3]:
            s_all = [
                col
                for col in c_train_p.columns
                if col.startswith("wnir_s_") and col.endswith("_all")
            ]
            s_cluster = [
                col
                for col in c_train_p.columns
                if col.startswith("wnir_s_") and col.endswith("_cluster")
            ]
            X_tr_step1 = np.hstack(
                [
                    X_tr_base,
                    c_train_p[s_all + s_cluster].fillna(0).values.astype(np.float32),
                ]
            )
            X_va_step1 = np.hstack(
                [
                    X_va_base,
                    c_valid_p[s_all + s_cluster].fillna(0).values.astype(np.float32),
                ]
            )

        # =============================================
        # 6. Обучение моделей
        # =============================================
        if exp_type in [2.1, 2.2, 3.1]:
            model = TorchRidge(alpha=alpha1, device=DEVICE)
            model.fit(X_tr_step1, y_tr)
            cluster_preds = model.predict(X_va_step1)

        else:
            suffix = "all" if exp_type in [2.3, 2.4] else "cluster"
            proxy_col = f"wnir_p_value_{R}_{suffix}"

            y_tr_proxy = c_train_p[proxy_col].fillna(0).values.astype(np.float32)
            y_va_proxy = c_valid_p[proxy_col].fillna(0).values.astype(np.float32)

            model1 = TorchRidge(alpha=alpha1, device=DEVICE)
            model1.fit(X_tr_step1, y_tr_proxy)
            pred_proxy_tr = model1.predict(X_tr_step1).reshape(-1, 1)
            pred_proxy_va = model1.predict(X_va_step1).reshape(-1, 1)

            X_tr_step2 = np.hstack([X_tr_step1, pred_proxy_tr])
            X_va_step2 = np.hstack([X_va_step1, pred_proxy_va])

            model2 = TorchRidge(alpha=alpha2, device=DEVICE)
            model2.fit(X_tr_step2, y_tr)
            cluster_preds = model2.predict(X_va_step2)

            # ЗАПИСЬ ПО ИНДЕКСАМ (Прокси)
            valid_proxy_preds.loc[c_valid_p.index] = pred_proxy_va.flatten()
            valid_proxy_true.loc[c_valid_p.index] = y_va_proxy

        # ЗАПИСЬ ПО ИНДЕКСАМ (Целевой таргет)
        cluster_preds = np.nan_to_num(
            cluster_preds,
            nan=global_mean_price,
            posinf=global_mean_price,
            neginf=global_mean_price,
        )
        valid_preds.loc[c_valid_p.index] = cluster_preds

        del X_tr_step1, X_va_step1, c_train_p, c_valid_p
        if "X_tr_step2" in locals():
            del X_tr_step2, X_va_step2
        gc.collect()

    # =============================================
    # 7. Финальные метрики
    # =============================================
    # Берем true прямо из df_valid по тем же индексам, чтобы порядок совпал 1 к 1
    y_true_final = df_valid.loc[valid_p_idx, TARGET].values.astype(np.float32)
    y_pred_final = valid_preds.values

    metrics = {
        "valid_rmse": float(np.sqrt(mean_squared_error(y_true_final, y_pred_final))),
        "valid_mae": float(mean_absolute_error(y_true_final, y_pred_final)),
        "valid_mape": float(mean_absolute_percentage_error(y_true_final, y_pred_final)),
        "valid_r2": float(r2_score(y_true_final, y_pred_final)),
    }

    if exp_type in [2.3, 2.4, 3.2, 3.3]:
        metrics["valid_proxy_rmse"] = float(
            np.sqrt(
                mean_squared_error(valid_proxy_true.values, valid_proxy_preds.values)
            )
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

    def objective(trial):
        with mlflow.start_run(nested=True, run_name=f"Trial_{trial.number}"):
            mlflow.log_param("exp_type", exp_type)

            if exp_type < 2.0:
                metrics_dict = objective_global(
                    trial, df_train, df_valid, preprocessor, exp_type
                )
            else:
                metrics_dict = objective_cluster(
                    trial, df_train, df_valid, preprocessor, exp_type, wnir_params
                )

            mlflow.log_params(trial.params)
            mlflow.log_metrics(metrics_dict)

            gc.collect()
            torch.cuda.empty_cache()

            return metrics_dict["valid_rmse"]

    study.optimize(objective, n_trials=n_trials)
    print(f"[{exp_name}] Finished! Best RMSE: {study.best_value:.4f}")


# ==========================================
# 5. ТОЧКА ВХОДА
# ==========================================
if __name__ == "__main__":
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("Real_Estate_Pricing_Pipelines")

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

    df_train, df_valid = load_data()
    preprocessor = get_preprocessor()

    # ПОЛНАЯ МАТРИЦА ЭКСПЕРИМЕНТОВ (11 ШТУК)
    experiments = [
        # БЛОК 1: Глобальные (Единый город)
        ("1.1_Global_Direct_NO_wnir", 1.1),
        ("1.2_Global_Direct_WITH_wnir", 1.2),
        ("1.3_Global_Corrector_NO_wnir", 1.3),
        ("1.4_Global_Corrector_WITH_wnir", 1.4),
        # БЛОК 2: Кластерные (Макро-уровень)
        ("2.1_Cluster_Direct_NO_wnir", 2.1),
        ("2.2_Cluster_Direct_WITH_wnir", 2.2),
        ("2.3_Cluster_Corrector_NO_wnir", 2.3),
        ("2.4_Cluster_Corrector_WITH_wnir", 2.4),
        # БЛОК 3: Супер-модели (Макро + Микро уровень)
        ("3.1_Super_Direct_WITH_Micro", 3.1),
        ("3.2_Super_Corrector_NO_Micro", 3.2),
        ("3.3_Super_Corrector_WITH_Micro", 3.3),
    ]

    for exp_name, exp_type in experiments:
        with mlflow.start_run(run_name=exp_name):
            run_experiment(
                exp_name=exp_name,
                exp_type=exp_type,
                df_train=df_train,
                df_valid=df_valid,
                preprocessor=preprocessor,
                wnir_params=wnir_params,
                n_trials=10,  # Измени для управления длительностью
            )

    print("\nAll experiments finished!")
    print(
        "Run 'mlflow ui --backend-store-uri sqlite:///mlflow.db' to view the dashboard."
    )
