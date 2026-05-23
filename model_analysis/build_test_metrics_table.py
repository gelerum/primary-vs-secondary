"""Build presentation/Defense-Deck-style pivot CSV (10 configs × 5 models) from MLflow.

Reads Final_Test runs (tagged phase=test) from sqlite:///mlflow.db, picks the
winning cluster_algo per (config, model) for clustered/nested scopes by min
test_rmse, formats each cell exactly like the deck:
    {rmse/1000:.1f}k\n{mae/1000:.1f}k · {mape*100:.1f}% · {r2:.2f}[ · prx ...][ · km|hdb]
Writes model_analysis/test_metrics_pivot.csv.

Run after a fresh experiment sweep:
    python -m src.experiments.choose_model
    python model_analysis/build_test_metrics_table.py
"""

import csv
import math
import sys
from pathlib import Path

import mlflow
import pandas as pd

MLFLOW_DB = "sqlite:///mlflow.db"
EXPERIMENT_NAME = "Real_Estate_Pricing_Pipelines"
OUT_PATH = Path(__file__).parent / "test_metrics_pivot.csv"

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
CLUSTER_BADGE = {"kmeans": "km", "hdbscan": "hdb"}
DASH = "—"


def build_cfg_name(row) -> str:
    base = f"{row['tags.scope']}_{row['tags.mode']}"
    suffix = ""
    if row.get("tags.includes_wnir_all") == "True":
        suffix += "+wnir_all"
    if row.get("tags.includes_wnir_cluster") == "True":
        suffix += "+wnir_cluster"
    return base + suffix


def fmt_k(v) -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return DASH
    return f"{v / 1000:.1f}k"


def fmt_mape(v) -> str:
    # MAPE > 1000% almost certainly means the model diverged — show a dash so the
    # cell stays readable.
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return DASH
    if v > 10:
        return DASH
    return f"{v * 100:.1f}%"


def fmt_r2(v) -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return DASH
    if v < 0:
        return DASH
    return f"{v:.2f}"


def format_cell(row) -> str:
    rmse = row.get("metrics.test_rmse")
    if rmse is None or (isinstance(rmse, float) and (math.isnan(rmse) or math.isinf(rmse))):
        return DASH

    big = fmt_k(rmse)

    meta_parts = [
        fmt_k(row.get("metrics.test_mae")),
        fmt_mape(row.get("metrics.test_mape")),
        fmt_r2(row.get("metrics.test_r2")),
    ]
    if row["tags.mode"] == "two_stage":
        meta_parts.append(f"prx {fmt_k(row.get('metrics.test_proxy_rmse'))}")
    if row["tags.scope"] in ("clustered", "nested"):
        badge = CLUSTER_BADGE.get(row.get("tags.cluster_algo"), "?")
        meta_parts.append(badge)

    return f"{big}\n{' · '.join(meta_parts)}"


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
        sys.exit(
            "ERROR: no Final_Test runs (tags.phase='test') found. "
            "Run `python -m src.experiments.choose_model` first."
        )

    runs["cfg_name"] = runs.apply(build_cfg_name, axis=1)

    # Per (cfg, model, cluster_algo): keep the most recent Final_Test run.
    runs = runs.sort_values("start_time", ascending=False)
    runs = runs.drop_duplicates(
        subset=["cfg_name", "tags.model_type", "tags.cluster_algo"], keep="first"
    )

    # Per (cfg, model): for clustered/nested, pick winning cluster_algo by min test_rmse.
    runs = runs.sort_values("metrics.test_rmse", ascending=True, na_position="last")
    winners = runs.drop_duplicates(
        subset=["cfg_name", "tags.model_type"], keep="first"
    )

    pivot = pd.DataFrame(index=CONFIG_ORDER, columns=MODEL_ORDER, data=DASH)
    filled = 0
    for _, row in winners.iterrows():
        cfg = row["cfg_name"]
        model = row["tags.model_type"]
        if cfg not in CONFIG_ORDER or model not in MODEL_ORDER:
            continue
        pivot.loc[cfg, model] = format_cell(row)
        filled += 1
    pivot.index.name = "configuration"

    pivot.to_csv(OUT_PATH, quoting=csv.QUOTE_ALL)

    total = len(CONFIG_ORDER) * len(MODEL_ORDER)
    print(f"Wrote {OUT_PATH} ({filled}/{total} cells filled).")

    missing = [
        (cfg, m)
        for cfg in CONFIG_ORDER
        for m in MODEL_ORDER
        if pivot.loc[cfg, m] == DASH
    ]
    if missing:
        print(f"Missing ({len(missing)}):")
        for cfg, m in missing:
            print(f"  {cfg} × {m}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
