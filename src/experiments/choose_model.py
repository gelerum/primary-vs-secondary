import gc
from dataclasses import dataclass
from typing import Literal

import hdbscan
import mlflow
import numpy as np
import optuna
import pandas as pd
import torch
from catboost import CatBoostRegressor
from dvc.api import params_show
from fast_pytorch_kmeans import KMeans as TorchKMeans
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)
from sklearn.preprocessing import OneHotEncoder, StandardScaler

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
# 0.5 КОНФИГ ЭКСПЕРИМЕНТА
# ==========================================
@dataclass(frozen=True)
class ExpConfig:
    """Orthogonal experiment axes — replaces float exp_type literals.

    Feature-inclusion rules:
      - scope='local' always feeds both global (s_*_all) and per-cluster (s_*_cluster)
        WNIR features to the regressor.
      - scope='global'|'clustered' only feeds s_*_all features when use_wnir=True.
    """

    scope: Literal["global", "clustered", "nested"]
    mode: Literal["direct", "two_stage"]
    use_wnir: bool = False  # controls s_*_all features for global/clustered; ignored for local

    @property
    def name(self) -> str:
        base = f"{self.scope}_{self.mode}"
        suffixes = []
        if self.includes_wnir_all:
            suffixes.append("+wnir_all")
        if self.includes_wnir_cluster:
            suffixes.append("+wnir_cluster")
        return base + "".join(suffixes)

    @property
    def includes_wnir_all(self) -> bool:
        return self.use_wnir or self.scope == "nested"

    @property
    def includes_wnir_cluster(self) -> bool:
        return self.scope == "nested"

    @property
    def proxy_suffix(self) -> str:
        # Maps to the WNIR data-column suffix, not the scope name.
        return "cluster" if self.scope == "nested" else "all"

    @property
    def needs_per_cluster_wnir(self) -> bool:
        return self.scope == "nested"

    def tags(self) -> dict:
        return {
            "scope": self.scope,
            "mode": self.mode,
            "includes_wnir_all": str(self.includes_wnir_all),
            "includes_wnir_cluster": str(self.includes_wnir_cluster),
        }


# ==========================================
# 1. МОДЕЛИ (RIDGE И ВРАППЕРЫ)
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

    @property
    def feature_importances_(self):
        return np.abs(self.w[1:].cpu().numpy().flatten())


class TorchLinearRegression:
    """GPU OLS via torch.linalg.lstsq."""

    def __init__(self, device="cuda"):
        self.device = device
        self.w = None

    def fit(self, X, y):
        X_t = torch.tensor(X, dtype=torch.float32, device=self.device)
        y_t = torch.tensor(y, dtype=torch.float32, device=self.device).view(-1, 1)

        ones = torch.ones((X_t.shape[0], 1), dtype=torch.float32, device=self.device)
        X_t = torch.cat([ones, X_t], dim=1)

        self.w = torch.linalg.lstsq(X_t, y_t).solution
        self.w = torch.nan_to_num(self.w, nan=0.0, posinf=0.0, neginf=0.0)

        del X_t, y_t, ones
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

    @property
    def feature_importances_(self):
        return np.abs(self.w[1:].cpu().numpy().flatten())


class TorchElasticNet:
    """GPU ElasticNet via FISTA. l1_ratio=1.0 -> Lasso, l1_ratio=0.0 -> Ridge-like (no closed form, still iterative)."""

    def __init__(
        self, alpha=1.0, l1_ratio=0.5, max_iter=1000, tol=1e-5, device="cuda"
    ):
        self.alpha = float(alpha)
        self.l1_ratio = float(l1_ratio)
        self.max_iter = int(max_iter)
        self.tol = float(tol)
        self.device = device
        self.w = None
        self.intercept = 0.0
        self._x_mean = None

    def fit(self, X, y):
        X_t = torch.tensor(X, dtype=torch.float32, device=self.device)
        y_t = torch.tensor(y, dtype=torch.float32, device=self.device)
        n, d = X_t.shape

        x_mean = X_t.mean(dim=0)
        y_mean = y_t.mean()
        Xc = X_t - x_mean
        yc = y_t - y_mean

        l1_pen = self.alpha * self.l1_ratio
        l2_pen = self.alpha * (1.0 - self.l1_ratio)

        # Spectral norm of (1/n) * Xc^T Xc via power iteration -> Lipschitz constant L
        v = torch.randn(d, dtype=torch.float32, device=self.device)
        v = v / (torch.norm(v) + 1e-12)
        for _ in range(30):
            Av = (Xc.T @ (Xc @ v)) / n
            v = Av / (torch.norm(Av) + 1e-12)
        sigma2 = float((v @ ((Xc.T @ (Xc @ v)) / n)).item())
        L = sigma2 + l2_pen + 1e-6
        step = 1.0 / L

        w = torch.zeros(d, dtype=torch.float32, device=self.device)
        z = w.clone()
        t = 1.0
        thresh = step * l1_pen

        for _ in range(self.max_iter):
            grad = (Xc.T @ (Xc @ z - yc)) / n + l2_pen * z
            w_new = z - step * grad
            if l1_pen > 0:
                w_new = torch.sign(w_new) * torch.clamp(
                    torch.abs(w_new) - thresh, min=0.0
                )

            t_new = 0.5 * (1.0 + (1.0 + 4.0 * t * t) ** 0.5)
            z = w_new + ((t - 1.0) / t_new) * (w_new - w)

            denom = torch.norm(w) + 1e-12
            diff = torch.norm(w_new - w) / denom
            w = w_new
            t = t_new

            if float(diff.item()) < self.tol:
                break

        w = torch.nan_to_num(w, nan=0.0, posinf=0.0, neginf=0.0)
        self.w = w
        self.intercept = float((y_mean - (x_mean * w).sum()).item())
        self._x_mean = x_mean

        del X_t, y_t, Xc, yc, v
        torch.cuda.empty_cache()

    def predict(self, X):
        X_t = torch.tensor(X, dtype=torch.float32, device=self.device)
        preds = X_t @ self.w + self.intercept
        res = preds.cpu().numpy().flatten()
        res = np.nan_to_num(res, nan=0.0, posinf=0.0, neginf=0.0)
        del X_t, preds
        torch.cuda.empty_cache()
        return res

    @property
    def feature_importances_(self):
        return np.abs(self.w.cpu().numpy().flatten())


class TorchLasso(TorchElasticNet):
    def __init__(self, alpha=1.0, max_iter=1000, tol=1e-5, device="cuda"):
        super().__init__(
            alpha=alpha, l1_ratio=1.0, max_iter=max_iter, tol=tol, device=device
        )


def get_model(trial, model_type, prefix="1"):
    if model_type == "ridge":
        alpha = trial.suggest_float(f"ridge_alpha{prefix}", 1e-3, 1e3, log=True)
        return TorchRidge(alpha=alpha, device=DEVICE)
    elif model_type == "lasso":
        alpha = trial.suggest_float(f"lasso_alpha{prefix}", 1e-4, 1e2, log=True)
        return TorchLasso(alpha=alpha, device=DEVICE)
    elif model_type == "elastic_net":
        alpha = trial.suggest_float(f"en_alpha{prefix}", 1e-4, 1e2, log=True)
        l1_ratio = trial.suggest_float(f"en_l1_ratio{prefix}", 0.05, 0.95)
        return TorchElasticNet(alpha=alpha, l1_ratio=l1_ratio, device=DEVICE)
    elif model_type == "ols":
        return TorchLinearRegression(device=DEVICE)
    elif model_type == "catboost":
        params = {
            "iterations": trial.suggest_int(f"cb_iters{prefix}", 100, 500),
            "depth": trial.suggest_int(f"cb_depth{prefix}", 4, 10),
            "learning_rate": trial.suggest_float(f"cb_lr{prefix}", 1e-3, 0.3, log=True),
            "verbose": 0,
            "task_type": "GPU" if torch.cuda.is_available() else "CPU",
            "random_seed": 42,
        }
        return CatBoostRegressor(**params)


# ==========================================
# 2. ПОДГОТОВКА ДАННЫХ И ХЕЛПЕРЫ
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
    df_test = pd.read_parquet("data/interim/wnir_all_test.parquet")
    df_train["date"] = pd.to_datetime(df_train["date"])
    df_valid["date"] = pd.to_datetime(df_valid["date"])
    df_test["date"] = pd.to_datetime(df_test["date"])
    df_train["set_type"] = "train"
    df_valid["set_type"] = "valid"
    df_test["set_type"] = "test"
    return df_train, df_valid, df_test


def get_base_feature_names(preprocessor):
    cat_cols = preprocessor.named_transformers_["cat"].get_feature_names_out(
        BASE_CAT_FEATURES
    )
    return BASE_NUM_FEATURES + list(cat_cols)


def log_fi(importances, feature_names, filename):
    df = pd.DataFrame({"feature": feature_names, "importance": importances})
    df = df.sort_values("importance", ascending=False)
    mlflow.log_text(df.to_csv(index=False), filename)


def fit_transform_block(X_tr, X_va):
    """Fit StandardScaler on X_tr, transform both. Fresh scaler per call — no leakage."""
    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_tr).astype(np.float32)
    X_va_s = scaler.transform(X_va).astype(np.float32)
    return X_tr_s, X_va_s


# ==========================================
# 3. ЦЕЛЕВЫЕ ФУНКЦИИ OPTUNA
# ==========================================
def objective_global(
    trial, df_train, df_valid, preprocessor, cfg: ExpConfig, wnir_params, model_type,
    phase="valid",
):
    torch.manual_seed(42)
    np.random.seed(42)

    train_p = df_train[df_train["market_type"] == "primary"].copy()
    valid_p = df_valid[df_valid["market_type"] == "primary"].copy()

    X_train_base = preprocessor.fit_transform(train_p).astype(np.float32)
    X_valid_base = preprocessor.transform(valid_p).astype(np.float32)

    base_feat_names = get_base_feature_names(preprocessor)

    y_train = train_p[TARGET].values.astype(np.float32)
    y_valid = valid_p[TARGET].values.astype(np.float32)

    metrics = {}

    if cfg.mode == "two_stage":
        R = trial.suggest_categorical("R", list(wnir_params["R"].values()))

    if cfg.includes_wnir_all:
        s_cols = [
            c for c in train_p.columns if c.startswith("wnir_s_") and c.endswith("_all")
        ]
        X_tr_s = train_p[s_cols].fillna(0).values.astype(np.float32)
        X_va_s = valid_p[s_cols].fillna(0).values.astype(np.float32)
        X_tr_s, X_va_s = fit_transform_block(X_tr_s, X_va_s)
        X_tr_step1 = np.hstack([X_train_base, X_tr_s])
        X_va_step1 = np.hstack([X_valid_base, X_va_s])
        feat_names_step1 = base_feat_names + s_cols
    else:
        X_tr_step1, X_va_step1 = X_train_base, X_valid_base
        feat_names_step1 = base_feat_names.copy()

    if cfg.mode == "direct":
        model = get_model(trial, model_type, prefix="1")
        model.fit(X_tr_step1, y_train)
        preds = model.predict(X_va_step1)
        log_fi(model.feature_importances_, feat_names_step1, f"{phase}_fi_main.csv")

    else:  # corrector
        proxy_col = f"wnir_p_value_{R}_{cfg.proxy_suffix}"
        y_train_proxy = train_p[proxy_col].fillna(0).values.astype(np.float32)
        y_valid_proxy = valid_p[proxy_col].fillna(0).values.astype(np.float32)

        y_proxy_scaler = StandardScaler()
        y_tr_proxy_scaled = y_proxy_scaler.fit_transform(
            y_train_proxy.reshape(-1, 1)
        ).flatten()

        model1 = get_model(trial, model_type, prefix="1")
        model1.fit(X_tr_step1, y_tr_proxy_scaled)
        pred_proxy_tr_scaled = model1.predict(X_tr_step1).reshape(-1, 1)
        pred_proxy_va_scaled = model1.predict(X_va_step1).reshape(-1, 1)

        log_fi(model1.feature_importances_, feat_names_step1, f"{phase}_fi_proxy.csv")

        # Inverse-transform only for raw-scale metric (interpretable RMSE).
        pred_proxy_va_raw = y_proxy_scaler.inverse_transform(
            pred_proxy_va_scaled
        ).flatten()
        metrics["valid_proxy_rmse"] = float(
            np.sqrt(mean_squared_error(y_valid_proxy, pred_proxy_va_raw))
        )

        X_tr_step2 = np.hstack([X_tr_step1, pred_proxy_tr_scaled])
        X_va_step2 = np.hstack([X_va_step1, pred_proxy_va_scaled])
        feat_names_step2 = feat_names_step1 + ["proxy_prediction"]

        model2 = get_model(trial, model_type, prefix="2")
        model2.fit(X_tr_step2, y_train)
        preds = model2.predict(X_va_step2)

        log_fi(model2.feature_importances_, feat_names_step2, f"{phase}_fi_main.csv")

    metrics["valid_rmse"] = float(np.sqrt(mean_squared_error(y_valid, preds)))
    metrics["valid_mae"] = float(mean_absolute_error(y_valid, preds))
    metrics["valid_mape"] = float(mean_absolute_percentage_error(y_valid, preds))
    metrics["valid_r2"] = float(r2_score(y_valid, preds))

    del train_p, valid_p, X_train_base, X_valid_base, X_tr_step1, X_va_step1
    gc.collect()

    return metrics


def objective_cluster(
    trial,
    df_train,
    df_valid,
    preprocessor,
    cfg: ExpConfig,
    wnir_params,
    model_type,
    cluster_algo,
    phase="valid",
):
    torch.manual_seed(42)
    np.random.seed(42)

    df_train = df_train.reset_index(drop=True).copy()
    df_valid = df_valid.reset_index(drop=True).copy()

    global_mean_price = df_train[df_train["market_type"] == "primary"][TARGET].mean()

    # 1. Кластеризация — отдельный preprocessor на primary+secondary
    cluster_preprocessor = get_preprocessor()
    X_train_all = cluster_preprocessor.fit_transform(df_train).astype(np.float32)
    X_valid_all = cluster_preprocessor.transform(df_valid).astype(np.float32)

    # Regression preprocessor — фит на primary, единая шкала с objective_global
    preprocessor.fit(df_train[df_train["market_type"] == "primary"])
    base_feat_names = get_base_feature_names(preprocessor)

    if cluster_algo == "kmeans":
        n_clusters = trial.suggest_int("n_clusters", 3, 20)
        kmeans = TorchKMeans(
            n_clusters=n_clusters, mode="euclidean", verbose=0, max_iter=100
        )
        X_tr_t = torch.tensor(X_train_all, dtype=torch.float32, device=DEVICE)
        X_va_t = torch.tensor(X_valid_all, dtype=torch.float32, device=DEVICE)

        df_train["cluster"] = kmeans.fit_predict(X_tr_t).cpu().numpy()
        df_valid["cluster"] = kmeans.predict(X_va_t).cpu().numpy()

        del X_tr_t, X_va_t, kmeans
        torch.cuda.empty_cache()

    elif cluster_algo == "hdbscan":
        min_cluster_size = trial.suggest_int("hdb_min_cluster_size", 15, 300)
        min_samples = trial.suggest_int("hdb_min_samples", 5, 50)

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            prediction_data=True,
            core_dist_n_jobs=-1,
        )
        df_train["cluster"] = clusterer.fit_predict(X_train_all)

        labels, _ = hdbscan.approximate_predict(clusterer, X_valid_all)
        df_valid["cluster"] = labels

        del clusterer

    del X_train_all, X_valid_all
    gc.collect()

    unique_clusters = sorted(df_train["cluster"].unique())
    # Log actual cluster count — for hdbscan it's the only place this is recorded
    # (params only carry min_cluster_size/min_samples). -1 is HDBSCAN noise.
    mlflow.log_metric(
        "n_clusters_actual", len([c for c in unique_clusters if c != -1])
    )

    # 2. Подготовка массивов для валидации
    valid_p_idx = df_valid[df_valid["market_type"] == "primary"].index
    valid_preds = pd.Series(index=valid_p_idx, dtype=np.float32)

    if cfg.mode == "two_stage":
        valid_proxy_preds = pd.Series(index=valid_p_idx, dtype=np.float32)
        valid_proxy_true = pd.Series(index=valid_p_idx, dtype=np.float32)
        R = trial.suggest_categorical("R", list(wnir_params["R"].values()))

    fi_accum_step1 = 0
    fi_accum_step2 = 0
    total_samples = 0
    feat_names_step1 = None
    feat_names_step2 = None

    # 3. Цикл по уникальным кластерам
    for cluster_idx, c in enumerate(unique_clusters):
        c_train_all = df_train[df_train["cluster"] == c].copy()
        c_valid_all = df_valid[df_valid["cluster"] == c].copy()

        if cfg.needs_per_cluster_wnir:
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

            c_train_all = c_combined[c_combined["set_type"] == "train"].set_index(
                "orig_idx"
            )
            c_valid_all = c_combined[c_combined["set_type"] == "valid"].set_index(
                "orig_idx"
            )
            c_train_all.index.name = None
            c_valid_all.index.name = None
            del c_combined, new_wnir_cluster

        c_train_p = c_train_all[c_train_all["market_type"] == "primary"]
        c_valid_p = c_valid_all[c_valid_all["market_type"] == "primary"]
        n_p = len(c_train_p)

        # ==========================================
        # ИСПРАВЛЕНИЕ: обработка мелких кластеров и proxy-NaN
        # ==========================================
        if n_p < 5 or len(c_valid_p) == 0:
            if len(c_valid_p) > 0:
                valid_preds.loc[c_valid_p.index] = np.float32(global_mean_price)

                if cfg.mode == "two_stage":
                    proxy_col = f"wnir_p_value_{R}_{cfg.proxy_suffix}"

                    y_va_proxy_fallback = (
                        c_valid_p[proxy_col].fillna(0).values.astype(np.float32)
                    )
                    valid_proxy_true.loc[c_valid_p.index] = y_va_proxy_fallback

                    fallback_pred = (
                        np.nanmean(y_va_proxy_fallback)
                        if len(y_va_proxy_fallback) > 0
                        else 0.0
                    )
                    if np.isnan(fallback_pred):
                        fallback_pred = 0.0
                    valid_proxy_preds.loc[c_valid_p.index] = np.float32(fallback_pred)
            continue
        # ==========================================

        X_tr_base = preprocessor.transform(c_train_p).astype(np.float32)
        X_va_base = preprocessor.transform(c_valid_p).astype(np.float32)
        y_tr = c_train_p[TARGET].values.astype(np.float32)

        # 4. Формирование фичей
        extra_cols = []
        if cfg.includes_wnir_all:
            extra_cols.extend(
                col
                for col in c_train_p.columns
                if col.startswith("wnir_s_") and col.endswith("_all")
            )
        if cfg.includes_wnir_cluster:
            extra_cols.extend(
                col
                for col in c_train_p.columns
                if col.startswith("wnir_s_") and col.endswith("_cluster")
            )

        if extra_cols:
            X_tr_extra = c_train_p[extra_cols].fillna(0).values.astype(np.float32)
            X_va_extra = c_valid_p[extra_cols].fillna(0).values.astype(np.float32)
            X_tr_extra, X_va_extra = fit_transform_block(X_tr_extra, X_va_extra)
            X_tr_step1 = np.hstack([X_tr_base, X_tr_extra])
            X_va_step1 = np.hstack([X_va_base, X_va_extra])
            if feat_names_step1 is None:
                feat_names_step1 = base_feat_names + extra_cols
        else:
            X_tr_step1, X_va_step1 = X_tr_base, X_va_base
            if feat_names_step1 is None:
                feat_names_step1 = base_feat_names.copy()

        # 5. Обучение моделей
        if cfg.mode == "direct":
            # Проверяем, не одинаковые ли все значения таргета
            if np.all(y_tr == y_tr[0]):
                cluster_preds = np.full(len(X_va_step1), y_tr[0], dtype=np.float32)
                fi = np.zeros(X_tr_step1.shape[1], dtype=np.float32)
            else:
                model = get_model(trial, model_type, prefix="1")
                model.fit(X_tr_step1, y_tr)
                cluster_preds = model.predict(X_va_step1)
                fi = model.feature_importances_

            fi_accum_step1 += fi * n_p
            total_samples += n_p

        else:
            proxy_col = f"wnir_p_value_{R}_{cfg.proxy_suffix}"

            y_tr_proxy = c_train_p[proxy_col].fillna(0).values.astype(np.float32)
            y_va_proxy = c_valid_p[proxy_col].fillna(0).values.astype(np.float32)

            # --- МОДЕЛЬ 1 (PROXY) ---
            if np.all(y_tr_proxy == y_tr_proxy[0]):
                pred_proxy_tr = np.full(
                    (len(X_tr_step1), 1), y_tr_proxy[0], dtype=np.float32
                )
                pred_proxy_va = np.full(
                    (len(X_va_step1), 1), y_tr_proxy[0], dtype=np.float32
                )
                pred_proxy_tr_scaled = np.zeros(
                    (len(X_tr_step1), 1), dtype=np.float32
                )
                pred_proxy_va_scaled = np.zeros(
                    (len(X_va_step1), 1), dtype=np.float32
                )
                fi1 = np.zeros(X_tr_step1.shape[1], dtype=np.float32)
            else:
                y_proxy_scaler = StandardScaler()
                y_tr_proxy_scaled = y_proxy_scaler.fit_transform(
                    y_tr_proxy.reshape(-1, 1)
                ).flatten()

                model1 = get_model(trial, model_type, prefix="1")
                model1.fit(X_tr_step1, y_tr_proxy_scaled)
                pred_proxy_tr_scaled = model1.predict(X_tr_step1).reshape(-1, 1)
                pred_proxy_va_scaled = model1.predict(X_va_step1).reshape(-1, 1)

                # Защита от inf (Ridge на плохо обусловленной X^T X)
                pred_proxy_tr_scaled = np.nan_to_num(
                    pred_proxy_tr_scaled, nan=0.0, posinf=0.0, neginf=0.0
                )
                pred_proxy_va_scaled = np.nan_to_num(
                    pred_proxy_va_scaled, nan=0.0, posinf=0.0, neginf=0.0
                )

                # Unscale для valid_proxy_rmse (хранится на сырой шкале proxy-таргета)
                pred_proxy_tr = y_proxy_scaler.inverse_transform(
                    pred_proxy_tr_scaled
                ).astype(np.float32)
                pred_proxy_va = y_proxy_scaler.inverse_transform(
                    pred_proxy_va_scaled
                ).astype(np.float32)

                fi1 = model1.feature_importances_

            X_tr_step2 = np.hstack([X_tr_step1, pred_proxy_tr_scaled])
            X_va_step2 = np.hstack([X_va_step1, pred_proxy_va_scaled])
            if feat_names_step2 is None:
                feat_names_step2 = feat_names_step1 + ["proxy_prediction"]

            # --- МОДЕЛЬ 2 (ОСНОВНАЯ) ---
            if np.all(y_tr == y_tr[0]):
                cluster_preds = np.full(len(X_va_step2), y_tr[0], dtype=np.float32)
                fi2 = np.zeros(X_tr_step2.shape[1], dtype=np.float32)
            else:
                model2 = get_model(trial, model_type, prefix="2")
                model2.fit(X_tr_step2, y_tr)
                cluster_preds = model2.predict(X_va_step2)
                fi2 = model2.feature_importances_

            # ==========================================
            # ИСПРАВЛЕНИЕ: строгое приведение типов (убирает FutureWarning)
            # ==========================================
            valid_proxy_preds.loc[c_valid_p.index] = pred_proxy_va.flatten().astype(
                np.float32
            )
            valid_proxy_true.loc[c_valid_p.index] = y_va_proxy.astype(np.float32)
            # ==========================================

            fi_accum_step1 += fi1 * n_p
            fi_accum_step2 += fi2 * n_p
            total_samples += n_p

        cluster_preds = np.nan_to_num(
            cluster_preds,
            nan=global_mean_price,
            posinf=global_mean_price,
            neginf=global_mean_price,
        )

        # ==========================================
        # ИСПРАВЛЕНИЕ: строгое приведение типов
        # ==========================================
        valid_preds.loc[c_valid_p.index] = cluster_preds.astype(np.float32)
        # ==========================================

        # Pruner: running RMSE на уже обработанных valid-точках.
        # Только во время HPO (phase == "valid"), не на финальном test-replay.
        if phase == "valid":
            processed = valid_preds.dropna()
            if len(processed) > 0:
                y_true_so_far = df_valid.loc[processed.index, TARGET].values.astype(
                    np.float32
                )
                y_pred_so_far = processed.values.astype(np.float32)
                running_rmse = float(
                    np.sqrt(mean_squared_error(y_true_so_far, y_pred_so_far))
                )
                trial.report(running_rmse, step=cluster_idx)
                if trial.should_prune():
                    raise optuna.TrialPruned()

    # 6. Усреднение и логирование FI
    if total_samples > 0:
        if cfg.mode == "direct":
            avg_fi_main = fi_accum_step1 / total_samples
            log_fi(avg_fi_main, feat_names_step1, f"{phase}_fi_main.csv")
        else:
            avg_fi_proxy = fi_accum_step1 / total_samples
            avg_fi_main = fi_accum_step2 / total_samples
            log_fi(avg_fi_proxy, feat_names_step1, f"{phase}_fi_proxy.csv")
            log_fi(avg_fi_main, feat_names_step2, f"{phase}_fi_main.csv")

    # 7. Финальные метрики (со страховкой от NaN)
    y_true_final = df_valid.loc[valid_p_idx, TARGET].values.astype(np.float32)
    y_pred_final = valid_preds.fillna(global_mean_price).values.astype(np.float32)

    # ИСПРАВЛЕНИЕ: Глобальная очистка от inf перед расчетом метрик
    y_true_final = np.nan_to_num(
        y_true_final,
        nan=global_mean_price,
        posinf=global_mean_price,
        neginf=global_mean_price,
    )
    y_pred_final = np.nan_to_num(
        y_pred_final,
        nan=global_mean_price,
        posinf=global_mean_price,
        neginf=global_mean_price,
    )

    metrics = {
        "valid_rmse": float(np.sqrt(mean_squared_error(y_true_final, y_pred_final))),
        "valid_mae": float(mean_absolute_error(y_true_final, y_pred_final)),
        "valid_mape": float(mean_absolute_percentage_error(y_true_final, y_pred_final)),
        "valid_r2": float(r2_score(y_true_final, y_pred_final)),
    }

    if cfg.mode == "two_stage":
        vp_true = valid_proxy_true.fillna(0.0).values.astype(np.float32)
        vp_preds = valid_proxy_preds.fillna(0.0).values.astype(np.float32)

        # ИСПРАВЛЕНИЕ: Очистка от inf для прокси-таргета
        vp_true = np.nan_to_num(vp_true, nan=0.0, posinf=0.0, neginf=0.0)
        vp_preds = np.nan_to_num(vp_preds, nan=0.0, posinf=0.0, neginf=0.0)

        metrics["valid_proxy_rmse"] = float(
            np.sqrt(mean_squared_error(vp_true, vp_preds))
        )

    return metrics


# ==========================================
# 4. ДВИЖОК ЭКСПЕРИМЕНТОВ
# ==========================================
def evaluate_on_test(
    study,
    cfg: ExpConfig,
    model_type,
    df_train,
    df_valid,
    df_test,
    preprocessor,
    wnir_params,
    cluster_algo,
):
    """Retrain with best params on train+valid, evaluate on test, log test_* metrics.

    Returns (test_metrics, best_params, best_valid_rmse) or (None, None, None) on failure.
    """
    try:
        best_params = study.best_params
        best_trial_number = study.best_trial.number
        best_valid_rmse = study.best_value
    except ValueError:
        print(f"[{study.study_name}] No best trial available, skipping test evaluation.")
        return None, None, None

    # Combine train+valid as the new training set; test plays the role of "valid".
    # set_type is reassigned so the super-scope split-by-set_type logic still works.
    df_train_full = pd.concat([df_train, df_valid], ignore_index=True).copy()
    df_train_full["set_type"] = "train"
    df_test_eval = df_test.copy()
    df_test_eval["set_type"] = "valid"

    fixed_trial = optuna.trial.FixedTrial(best_params)

    with mlflow.start_run(nested=True, run_name="Final_Test"):
        mlflow.set_tags({**cfg.tags(), "model_type": model_type,
                         "cluster_algo": cluster_algo, "phase": "test"})
        mlflow.log_param("best_trial_number", best_trial_number)
        mlflow.log_params(best_params)
        mlflow.log_metric("best_valid_rmse", float(best_valid_rmse))

        if cfg.scope == "global":
            metrics_dict = objective_global(
                fixed_trial, df_train_full, df_test_eval, preprocessor,
                cfg, wnir_params, model_type, phase="test",
            )
        else:
            metrics_dict = objective_cluster(
                fixed_trial, df_train_full, df_test_eval, preprocessor,
                cfg, wnir_params, model_type, cluster_algo, phase="test",
            )

        test_metrics = {k.replace("valid_", "test_"): v for k, v in metrics_dict.items()}
        mlflow.log_metrics(test_metrics)

        gc.collect()
        torch.cuda.empty_cache()

    print(
        f"[{study.study_name}] Test metrics: "
        + ", ".join(f"{k}={v:.4f}" for k, v in test_metrics.items())
    )
    return test_metrics, best_params, float(best_valid_rmse)


def run_experiment(
    cfg: ExpConfig,
    model_type,
    df_train,
    df_valid,
    df_test,
    preprocessor,
    wnir_params,
    cluster_algo="none",
    n_trials=20,
):
    print(
        f"\n{'=' * 60}\nRunning: {cfg.name} | Model: {model_type.upper()} "
        f"| Clustering: {cluster_algo.upper()}\n{'=' * 60}"
    )

    # Tag parent run with orthogonal axes for filter/group in MLflow UI
    mlflow.set_tags({**cfg.tags(), "model_type": model_type, "cluster_algo": cluster_algo})

    study_name = f"{cfg.name}__{model_type}__{cluster_algo}"
    optuna_db_path = "sqlite:///optuna.db"

    study = optuna.create_study(
        direction="minimize",
        study_name=study_name,
        storage=optuna_db_path,
        load_if_exists=True,
        sampler=optuna.samplers.TPESampler(seed=42),
        pruner=optuna.pruners.MedianPruner(n_startup_trials=2, n_warmup_steps=2),
    )

    completed_trials = len(
        [t for t in study.trials if t.state.name in ["COMPLETE", "PRUNED"]]
    )
    trials_to_run = max(0, n_trials - completed_trials)

    if trials_to_run == 0:
        print(f"[{study_name}] Already completed {n_trials} trials. Skipping HPO...")
    else:
        print(
            f"[{study_name}] Found {completed_trials} completed trials. Running {trials_to_run} more..."
        )

        def objective(trial):
            with mlflow.start_run(nested=True, run_name=f"Trial_{trial.number}"):
                mlflow.set_tags({**cfg.tags(), "model_type": model_type,
                                 "cluster_algo": cluster_algo, "phase": "valid"})
                try:
                    if cfg.scope == "global":
                        metrics_dict = objective_global(
                            trial, df_train, df_valid, preprocessor, cfg,
                            wnir_params, model_type,
                        )
                    else:
                        metrics_dict = objective_cluster(
                            trial, df_train, df_valid, preprocessor, cfg,
                            wnir_params, model_type, cluster_algo,
                        )

                    mlflow.log_params(trial.params)
                    mlflow.log_metrics(metrics_dict)

                    return metrics_dict["valid_rmse"]
                except optuna.TrialPruned:
                    mlflow.set_tag("status", "pruned")
                    raise
                finally:
                    gc.collect()
                    torch.cuda.empty_cache()

        study.optimize(objective, n_trials=trials_to_run)
        try:
            print(f"[{study_name}] HPO finished! Best valid RMSE: {study.best_value:.4f}")
        except ValueError:
            print(f"[{study_name}] HPO finished — no completed trials (all pruned).")

    test_metrics, best_params, best_valid_rmse = evaluate_on_test(
        study=study,
        cfg=cfg,
        model_type=model_type,
        df_train=df_train,
        df_valid=df_valid,
        df_test=df_test,
        preprocessor=preprocessor,
        wnir_params=wnir_params,
        cluster_algo=cluster_algo,
    )

    # Hoist summary metrics to the parent run so experiments are comparable
    # in the MLflow UI without expanding nested runs.
    if test_metrics is not None:
        mlflow.log_metric("best_valid_rmse", best_valid_rmse)
        mlflow.log_metrics(test_metrics)
        mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})


# ==========================================
# 5. АДАПТИВНЫЙ РАЗМЕР HPO
# ==========================================
MODEL_PARAMS_PER_STAGE = {
    "ols": 0, "ridge": 1, "lasso": 1, "elastic_net": 2, "catboost": 3
}
CLUSTER_PARAMS = {"none": 0, "kmeans": 1, "hdbscan": 2}


def n_trials_for(cfg: ExpConfig, model_type: str, cluster_algo: str) -> int:
    """Minimal trial budget: enough to probe each param dimension once or twice."""
    if model_type == "catboost":
        return 3
    stages = 2 if cfg.mode == "two_stage" else 1
    r_param = 1 if cfg.mode == "two_stage" else 0
    n_params = (
        stages * MODEL_PARAMS_PER_STAGE[model_type]
        + r_param
        + CLUSTER_PARAMS[cluster_algo]
    )
    if n_params == 0:
        return 1
    if n_params <= 2:
        return 3
    if n_params <= 4:
        return 5
    return 8


# ==========================================
# 6. ТОЧКА ВХОДА
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

    df_train, df_valid, df_test = load_data()
    preprocessor = get_preprocessor()

    experiments = [
        ExpConfig("global", "direct"),
        ExpConfig("global", "direct", use_wnir=True),
        ExpConfig("global", "two_stage"),
        ExpConfig("global", "two_stage", use_wnir=True),
        ExpConfig("clustered", "direct"),
        ExpConfig("clustered", "direct", use_wnir=True),
        ExpConfig("clustered", "two_stage"),
        ExpConfig("clustered", "two_stage", use_wnir=True),
        ExpConfig("nested", "direct"),
        ExpConfig("nested", "two_stage"),
    ]

    model_types = ["ridge", "lasso", "elastic_net", "ols", "catboost"]

    for cfg in experiments:
        cluster_algos = ["none"] if cfg.scope == "global" else ["kmeans", "hdbscan"]

        for model_type in model_types:
            for cluster_algo in cluster_algos:
                run_name_parts = [cfg.name, model_type]
                if cluster_algo != "none":
                    run_name_parts.append(cluster_algo)
                run_name = "__".join(run_name_parts)

                n_trials = n_trials_for(cfg, model_type, cluster_algo)
                with mlflow.start_run(run_name=run_name):
                    run_experiment(
                        cfg=cfg,
                        model_type=model_type,
                        df_train=df_train,
                        df_valid=df_valid,
                        df_test=df_test,
                        preprocessor=preprocessor,
                        wnir_params=wnir_params,
                        cluster_algo=cluster_algo,
                        n_trials=n_trials,
                    )

    print("\nAll experiments finished!")
    print(
        "Run 'mlflow ui --backend-store-uri sqlite:///mlflow.db' to view the dashboard."
    )
