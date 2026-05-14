"""
ONE master decision chart: best run per (exp_type × model × cluster_algo),
ranked, with every metric, cluster size, and chosen params visible.
Winner highlighted.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from pathlib import Path

OUT = Path(__file__).parent / "charts"
OUT.mkdir(exist_ok=True)

ALL_METRICS = ["valid_r2", "valid_mae", "valid_rmse", "valid_mape", "valid_proxy_rmse"]

# Short, code-style names per exp_type
EXP_NAME = {
    "1.1": "global_direct",
    "1.2": "global_direct_wnir_s",
    "1.3": "global_predictor_wnir_p",
    "1.4": "global_predictor_wnir_p_wnir_s",
    "2.1": "cluster_direct",
    "2.2": "cluster_direct_wnir_s",
    "2.3": "cluster_predictor_wnir_p",
    "2.4": "cluster_predictor_wnir_p_wnir_s",
    "3.1": "cluster_direct_wnir_s_wnir_s_cluster",
    "3.2": "cluster_predictor_wnir_p_cluster_wnir_s",
    "3.3": "cluster_predictor_wnir_p_cluster_wnir_s_wnir_s_cluster",
}

# ── Load ─────────────────────────────────────────────────────────────────────
df = pd.read_csv(Path(__file__).parent / "runs.csv")
df = df[df["Status"] == "FINISHED"].copy()
for c in ALL_METRICS:
    # MLflow exports some numeric columns as strings prefixed with ' to keep
    # spreadsheet apps from interpreting them as formulas — strip it.
    if df[c].dtype == object:
        df[c] = df[c].astype(str).str.lstrip("'")
    df[c] = pd.to_numeric(df[c], errors="coerce")
    df.loc[np.isinf(df[c]), c] = np.nan
    # Treat near-float64-max values as effectively infinite (some MLflow runs
    # wrote ~1.8e308 instead of inf). They blow up cell widths otherwise.
    df.loc[df[c].abs() > 1e30, c] = np.nan
# Keep every run that produced at least an MAE — many ridge runs blew up
# numerically (R² → NaN), but we still want them on the chart.
df = df.dropna(subset=["valid_mae"])
df["exp_type"] = df["exp_type"].astype(str)
df["model_type"] = df["model_type"].str.strip()

# Flag truly broken runs (predictions exploded by 20×+) — they get a FAIL
# marker. Merely bad ones (negative R², MAE 2–3× the winner) are kept with
# real numbers so the chart preserves the magnitude of the failure.
df["degenerate"] = df["valid_mae"] > 1e6

# ── Best run per (exp_type × model_type × cluster_algo) ─────────────────────
#   • prefer best R² when at least one trial in the group is non-degenerate
#   • otherwise (all degenerate) pick the row with the smallest MAE so we
#     at least surface the least-broken attempt
def pick_best(g):
    good = g[~g["degenerate"]]
    if len(good):
        return good.loc[good["valid_r2"].idxmax()]
    return g.loc[g["valid_mae"].idxmin()]

best = (
    df.groupby(["exp_type", "model_type", "cluster_algo"], group_keys=False)
      .apply(pick_best)
      .reset_index(drop=True)
)

def cluster_size(row):
    if row["cluster_algo"] == "hdbscan":
        v = row["hdb_min_cluster_size"]
        return f"cs={int(v)}" if pd.notna(v) else "cs=?"
    if row["cluster_algo"] == "kmeans":
        v = row["n_clusters"]
        return f"k={int(v)}" if pd.notna(v) else "k=?"
    return "-"

def _num(v):
    """Compact, fixed-ish-width formatter for ridge alphas (log-spaced)."""
    if v == 0:           return "0"
    a = abs(v)
    if a >= 100:         return f"{v:.0f}"
    if a >= 10:          return f"{v:.1f}"
    if a >= 1:           return f"{v:.2f}"
    if a >= 0.01:        return f"{v:.3f}"
    return f"{v:.1e}"

def param_str(row):
    if row["model_type"] == "catboost":
        bits = []
        if pd.notna(row.get("cb_depth1")): bits.append(f"d={int(row['cb_depth1'])}")
        if pd.notna(row.get("cb_iters1")): bits.append(f"it={int(row['cb_iters1'])}")
        if pd.notna(row.get("cb_lr1")):    bits.append(f"lr={row['cb_lr1']:.3f}")
        return " ".join(bits)
    bits = []
    if pd.notna(row.get("ridge_alpha1")): bits.append(f"a1={_num(row['ridge_alpha1'])}")
    if pd.notna(row.get("ridge_alpha2")): bits.append(f"a2={_num(row['ridge_alpha2'])}")
    return " ".join(bits)

best["cs_info"]  = best.apply(cluster_size, axis=1)
best["params"]   = best.apply(param_str, axis=1)
best["exp_name"] = best["exp_type"].map(EXP_NAME).fillna(best["exp_type"])

# Sort by RMSE (lower = better). Degenerate rows (FAIL) end up at the
# bottom because their MAE is huge and RMSE is correspondingly large.
best = best.sort_values(
    ["degenerate", "valid_rmse"], ascending=[True, True],
).reset_index(drop=True)

# ── Label: everything in one line ───────────────────────────────────────────
exp_w = best["exp_name"].str.len().max()
params_w = best["params"].str.len().max()
best["row_label"] = (
    best["exp_name"].str.ljust(exp_w)
    + " | " + best["model_type"].str.ljust(8)
    + " | " + best["cluster_algo"].str.ljust(7)
    + " | " + best["cs_info"].str.ljust(8)
    + " | " + best["params"].str.ljust(params_w)
)

# ── Per-metric rank (1 = best) for color coding ─────────────────────────────
# RMSE first (the sort key), then R², MAE, MAPE, proxy_rmse.
METRIC_ORDER = [
    ("valid_rmse",       False),
    ("valid_r2",         True),
    ("valid_mae",        False),
    ("valid_mape",       False),
    ("valid_proxy_rmse", False),
]
# Degenerate rows are excluded from ranking (kept NaN, drawn dark gray later)
ranks = pd.DataFrame(index=best.index)
mask_good = ~best["degenerate"]
for m, higher_better in METRIC_ORDER:
    series = best.loc[mask_good, m]
    ranks[m] = series.rank(ascending=not higher_better, method="min")
ranks = ranks[[m for m, _ in METRIC_ORDER]]   # enforce column order

n = len(best)
n_good = int(mask_good.sum())
color = (ranks - 1) / max(n_good - 1, 1)
# Hide proxy_rmse colors when value missing
color["valid_proxy_rmse"] = color["valid_proxy_rmse"].where(
    best["valid_proxy_rmse"].notna(), np.nan
)

# ── Annotation strings ──────────────────────────────────────────────────────
def fmt(v, kind):
    if pd.isna(v):          return "—"
    if abs(v) >= 1e12:      return f"{v:.1e}"     # safety: never overflow cells
    if kind == "r2":
        if abs(v) >= 1e6:   return f"{v:.1e}"
        if abs(v) >= 1000:  return f"{v:,.0f}"
        if abs(v) >= 10:    return f"{v:.1f}"
        return f"{v:.4f}"
    if kind == "mape":
        if abs(v) >= 1e4:   return f"{v:.1e}"
        if abs(v) >= 10:    return f"{v:.1f}"
        return f"{v:.4f}"
    if abs(v) >= 1e9:       return f"{v:.1e}"
    return f"{v:,.0f}"

def fmt_row(row, col, kind):
    if row["degenerate"]:
        return "FAIL"
    return fmt(row[col], kind)

annot = pd.DataFrame({
    "valid_rmse":       best.apply(lambda r: fmt_row(r, "valid_rmse", "int"), axis=1),
    "valid_r2":         best.apply(lambda r: fmt_row(r, "valid_r2", "r2"), axis=1),
    "valid_mae":        best.apply(lambda r: fmt_row(r, "valid_mae", "int"), axis=1),
    "valid_mape":       best.apply(lambda r: fmt_row(r, "valid_mape", "mape"), axis=1),
    "valid_proxy_rmse": best.apply(lambda r: fmt_row(r, "valid_proxy_rmse", "int"), axis=1),
}, index=best.index)[[m for m, _ in METRIC_ORDER]]

# ── Plot ────────────────────────────────────────────────────────────────────
sns.set_theme(style="white")
fig, ax = plt.subplots(figsize=(20, max(8, 0.42 * n + 2.5)), dpi=140)

sns.heatmap(
    color, annot=annot, fmt="", cmap="RdYlGn_r",
    cbar=False, linewidths=1.4, linecolor="white",
    annot_kws={"fontsize": 9.5, "fontweight": "medium"},
    ax=ax, vmin=0, vmax=1,
)

# Dark overlay for FAIL rows (every cell)
for i in range(n):
    if best["degenerate"].iloc[i]:
        for col in range(5):
            ax.add_patch(plt.Rectangle((col, i), 1, 1,
                                       facecolor="#3a3a3a", edgecolor="white",
                                       linewidth=1.4, zorder=2))
            ax.text(col + 0.5, i + 0.5, "FAIL", ha="center", va="center",
                    fontsize=9, color="#ffcccc", fontweight="bold", zorder=3)

# Gray-out NaN cells in proxy_rmse (only for non-degenerate rows)
for i in range(n):
    if not best["degenerate"].iloc[i] and pd.isna(best["valid_proxy_rmse"].iloc[i]):
        ax.add_patch(plt.Rectangle((4, i), 1, 1,
                                   facecolor="#ececec", edgecolor="white",
                                   linewidth=1.4, zorder=2))
        ax.text(4.5, i + 0.5, "—", ha="center", va="center",
                fontsize=10, color="#999", zorder=3)

# Y-tick labels = full info row
ax.set_yticklabels(best["row_label"], rotation=0, fontsize=9,
                   family="monospace")

# X-tick labels = metric names with arrows (matches METRIC_ORDER)
ax.set_xticklabels([
    "RMSE  ↓", "R²  ↑", "MAE  ↓", "MAPE  ↓", "Proxy RMSE  ↓"
], rotation=0, fontsize=11, fontweight="bold")
ax.tick_params(top=True, labeltop=True, bottom=False, labelbottom=False)

# Highlight the winner (row 0)
for col in range(5):
    ax.add_patch(plt.Rectangle((col, 0), 1, 1,
                               fill=False, edgecolor="#1a1a1a",
                               linewidth=3, zorder=5))
# Trophy marker
ax.text(-0.35, 0.5, "★", ha="center", va="center",
        fontsize=22, color="#d4a017", zorder=6,
        transform=ax.get_yaxis_transform())

# Title
ax.set_title(
    "Model Selection Leaderboard  —  best run per (exp_type × model × cluster), ranked by RMSE\n"
    "Green = best on that metric, red = worst.  Star = overall winner.",
    fontsize=12, fontweight="bold", pad=14, loc="left",
)
ax.set_xlabel("")
ax.set_ylabel("")

plt.tight_layout()
plt.savefig(OUT / "00_DECISION.png", bbox_inches="tight")
plt.close()
print("Saved 00_DECISION.png")

# ── Console summary ─────────────────────────────────────────────────────────
print("\n=== WINNER ===")
w = best.iloc[0]
print(f"  exp          : {w['exp_name']}  (exp_type {w['exp_type']})")
print(f"  model_type   : {w['model_type']}")
print(f"  cluster_algo : {w['cluster_algo']} ({w['cs_info']})")
print(f"  params       : {w['params']}")
print(f"  valid_r2     : {w['valid_r2']:.4f}")
print(f"  valid_mae    : {w['valid_mae']:,.0f}")
print(f"  valid_rmse   : {w['valid_rmse']:,.0f}")
print(f"  valid_mape   : {w['valid_mape']:.4f}")
print(f"  valid_proxy_rmse : {w['valid_proxy_rmse'] if pd.notna(w['valid_proxy_rmse']) else 'N/A'}")
