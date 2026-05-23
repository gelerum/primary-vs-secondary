"""Render a 10×5 heatmap of test metrics (configs × models), coloured by test_rmse.

Each cell shows RMSE / MAE / MAPE / R². For clustered/nested configs we pick the
cluster_algo with the lowest test_rmse — same rule the deck table uses.

Input:  model_analysis/test_metrics.csv  (produced by build_test_metrics_table.py)
Output: model_analysis/charts/test_metrics_heatmap.png
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

CSV_PATH = Path(__file__).parent / "test_metrics.csv"
OUT_DIR = Path(__file__).parent / "charts"
OUT_PATH = OUT_DIR / "test_metrics_heatmap.png"

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


def fmt_cell(row) -> str:
    def k(v):
        return f"{v / 1000:.1f}k" if pd.notna(v) else "—"

    def pct(v):
        return f"{v * 100:.1f}%" if pd.notna(v) and v <= 10 else "—"

    def r2(v):
        return f"{v:.2f}" if pd.notna(v) and v >= 0 else "—"

    lines = [
        f"RMSE {k(row['test_rmse'])}",
        f"MAE  {k(row['test_mae'])}",
        f"MAPE {pct(row['test_mape'])}",
        f"R²   {r2(row['test_r2'])}",
    ]

    algo = row.get("cluster_algo")
    badge = {"kmeans": "km", "hdbscan": "hdb"}.get(algo)
    if badge:
        n = row.get("n_clusters")
        lines.append(f"{badge} n={int(n)}" if pd.notna(n) else badge)

    return "\n".join(lines)


def main() -> int:
    if not CSV_PATH.exists():
        sys.exit(
            f"ERROR: {CSV_PATH} not found. "
            "Run `python model_analysis/build_test_metrics_table.py` first."
        )

    df = pd.read_csv(CSV_PATH)
    # ExpConfig.name appends +wnir_all+wnir_cluster to nested scopes, but the
    # deck (and CONFIG_ORDER) uses the bare nested_* labels — normalise here so
    # older CSVs still match.
    df.loc[df["scope"] == "nested", "cfg_name"] = (
        df.loc[df["scope"] == "nested", "scope"]
        + "_"
        + df.loc[df["scope"] == "nested", "mode"]
    )
    df = df[df["cfg_name"].isin(CONFIG_ORDER) & df["model_type"].isin(MODEL_ORDER)]

    # Winner per (cfg, model): lowest test_rmse across cluster_algos (km vs hdb).
    df = df.sort_values("test_rmse", na_position="last")
    df = df.drop_duplicates(subset=["cfg_name", "model_type"], keep="first")

    rmse_pivot = (
        df.pivot(index="cfg_name", columns="model_type", values="test_rmse")
        .reindex(index=CONFIG_ORDER, columns=MODEL_ORDER)
    )

    annot = pd.DataFrame(index=CONFIG_ORDER, columns=MODEL_ORDER, data="—")
    for _, row in df.iterrows():
        annot.loc[row["cfg_name"], row["model_type"]] = fmt_cell(row)

    OUT_DIR.mkdir(exist_ok=True)
    sns.set_theme(style="white")
    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(
        rmse_pivot,
        annot=annot.values,
        fmt="",
        cmap="RdYlGn_r",
        robust=True,  # 2/98 percentile — outliers don't crush the scale
        linewidths=1.2,
        linecolor="white",
        cbar_kws={"label": "test RMSE (lower = better)", "shrink": 0.7},
        annot_kws={"fontsize": 9, "fontfamily": "monospace"},
        ax=ax,
    )
    ax.set_xlabel("model", fontsize=11, fontweight="bold")
    ax.set_ylabel("configuration", fontsize=11, fontweight="bold")
    ax.set_title(
        "Test metrics by configuration × model\n"
        "(colour = test RMSE; for clustered/nested — winner of km vs hdb)",
        fontsize=12, fontweight="bold", pad=14,
    )
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position("top")
    plt.setp(ax.get_xticklabels(), rotation=0, fontsize=10)
    plt.setp(ax.get_yticklabels(), rotation=0, fontsize=9.5)

    plt.tight_layout()
    plt.savefig(OUT_PATH, bbox_inches="tight", dpi=140)
    plt.close()
    print(f"Saved {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
