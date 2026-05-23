"""Dump all Final_Test runs from MLflow into a flat CSV with one row per run.

Columns: config axes, model, cluster algo & sizes, all test_* metrics,
best_valid_rmse, run_id, start_time. Output: model_analysis/test_metrics.csv.

Run after a fresh experiment sweep:
    python -m src.experiments.choose_model
    python model_analysis/build_test_metrics_table.py
"""

import sys
from pathlib import Path

import mlflow
import pandas as pd

MLFLOW_DB = "sqlite:///mlflow.db"
EXPERIMENT_NAME = "Real_Estate_Pricing_Pipelines"
OUT_PATH = Path(__file__).parent / "test_metrics.csv"

CONFIG_ORDER = [
    "global_direct",
    "global_direct+wnir_all",
    "global_two_stage",
    "global_two_stage+wnir_all",
    "clustered_direct",
    "clustered_direct+wnir_all",
    "clustered_two_stage",
    "clustered_two_stage+wnir_all",
    "nested_direct",
    "nested_two_stage",
]
MODEL_ORDER = ["ridge", "lasso", "elastic_net", "ols", "catboost"]
CLUSTER_ORDER = ["none", "kmeans", "hdbscan"]

OUTPUT_COLS = [
    "cfg_name",
    "scope",
    "mode",
    "includes_wnir_all",
    "includes_wnir_cluster",
    "model_type",
    "cluster_algo",
    "n_clusters",
    "hdb_min_cluster_size",
    "hdb_min_samples",
    "R",
    "test_rmse",
    "test_mae",
    "test_mape",
    "test_r2",
    "test_proxy_rmse",
    "best_valid_rmse",
    "start_time",
    "run_id",
]


def build_cfg_name(row) -> str:
    base = f"{row['tags.scope']}_{row['tags.mode']}"
    suffix = ""
    if row.get("tags.includes_wnir_all") == "True":
        suffix += "+wnir_all"
    if row.get("tags.includes_wnir_cluster") == "True":
        suffix += "+wnir_cluster"
    return base + suffix


def main() -> int:
    mlflow.set_tracking_uri(MLFLOW_DB)
    client = mlflow.tracking.MlflowClient()
    exp = client.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        sys.exit(
            f"ERROR: MLflow experiment '{EXPERIMENT_NAME}' not found at {MLFLOW_DB}. "
            "Run `python -m src.experiments.choose_model` first."
        )

    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string="tags.phase = 'test'",
        output_format="pandas",
    )
    if runs.empty:
        sys.exit("ERROR: no Final_Test runs (tags.phase='test') found.")

    runs["cfg_name"] = runs.apply(build_cfg_name, axis=1)

    # If a (cfg, model, cluster_algo) was rerun, keep the most recent.
    runs = runs.sort_values("start_time", ascending=False)
    runs = runs.drop_duplicates(
        subset=["cfg_name", "tags.model_type", "tags.cluster_algo"], keep="first"
    )

    out = pd.DataFrame()
    out["cfg_name"] = runs["cfg_name"]
    out["scope"] = runs["tags.scope"]
    out["mode"] = runs["tags.mode"]
    out["includes_wnir_all"] = runs.get("tags.includes_wnir_all")
    out["includes_wnir_cluster"] = runs.get("tags.includes_wnir_cluster")
    out["model_type"] = runs["tags.model_type"]
    out["cluster_algo"] = runs["tags.cluster_algo"]

    # Cluster sizing params live under params.* (best HPO trial logged on Final_Test).
    out["n_clusters"] = pd.to_numeric(runs.get("params.n_clusters"), errors="coerce")
    out["hdb_min_cluster_size"] = pd.to_numeric(
        runs.get("params.hdb_min_cluster_size"), errors="coerce"
    )
    out["hdb_min_samples"] = pd.to_numeric(
        runs.get("params.hdb_min_samples"), errors="coerce"
    )
    out["R"] = pd.to_numeric(runs.get("params.R"), errors="coerce")

    for m in ("test_rmse", "test_mae", "test_mape", "test_r2", "test_proxy_rmse"):
        out[m] = pd.to_numeric(runs.get(f"metrics.{m}"), errors="coerce")
    out["best_valid_rmse"] = pd.to_numeric(
        runs.get("metrics.best_valid_rmse"), errors="coerce"
    )

    out["start_time"] = runs["start_time"]
    out["run_id"] = runs["run_id"]

    # Stable, presentation-aligned ordering.
    cfg_rank = {c: i for i, c in enumerate(CONFIG_ORDER)}
    model_rank = {m: i for i, m in enumerate(MODEL_ORDER)}
    cluster_rank = {c: i for i, c in enumerate(CLUSTER_ORDER)}
    out["_cfg"] = out["cfg_name"].map(cfg_rank).fillna(99)
    out["_model"] = out["model_type"].map(model_rank).fillna(99)
    out["_cluster"] = out["cluster_algo"].map(cluster_rank).fillna(99)
    out = out.sort_values(["_cfg", "_model", "_cluster"]).drop(
        columns=["_cfg", "_model", "_cluster"]
    )

    out = out[OUTPUT_COLS]
    out.to_csv(OUT_PATH, index=False)

    print(f"Wrote {OUT_PATH} ({len(out)} runs).")
    print(
        f"  configs:      {out['cfg_name'].nunique()} / {len(CONFIG_ORDER)}\n"
        f"  models:       {out['model_type'].nunique()} / {len(MODEL_ORDER)}\n"
        f"  cluster algos:{sorted(out['cluster_algo'].dropna().unique().tolist())}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
