"""
Model selection analysis: primary vs secondary price prediction.
Generates comparison tables and charts across all experiment types.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from pathlib import Path
from matplotlib.patches import Patch

# ── Config ───────────────────────────────────────────────────────────────────
OUT = Path(__file__).parent / "charts"
OUT.mkdir(exist_ok=True)

PALETTE = {"catboost": "#E07B39", "ridge": "#4A90D9"}
ALL_METRICS = ["valid_r2", "valid_mae", "valid_rmse", "valid_mape", "valid_proxy_rmse"]
METRIC_LABELS = {
    "valid_r2":         "R²  (↑ better)",
    "valid_mae":        "MAE  (↓ better)",
    "valid_rmse":       "RMSE  (↓ better)",
    "valid_mape":       "MAPE  (↓ better)",
    "valid_proxy_rmse": "Proxy RMSE  (↓ better)",
}
# proxy_rmse is only available for these exp_types
PROXY_EXPS = {"1.3", "1.4", "2.3", "2.4", "3.2", "3.3"}
EXP_ORDER = ["1.1", "1.2", "1.3", "1.4", "2.1", "2.2", "2.3", "2.4", "3.1", "3.2", "3.3"]

sns.set_theme(style="whitegrid", font_scale=1.05)
plt.rcParams.update({"figure.dpi": 140, "axes.spines.top": False,
                     "axes.spines.right": False})

# ── Load & clean ─────────────────────────────────────────────────────────────
df = pd.read_csv(Path(__file__).parent / "runs.csv")
df = df[df["Status"] == "FINISHED"].copy()

for c in ALL_METRICS:
    df[c] = pd.to_numeric(df[c], errors="coerce")
    df.loc[np.isinf(df[c]), c] = np.nan

# Keep rows with the four core metrics; proxy_rmse is optional
df = df.dropna(subset=["valid_r2", "valid_mae", "valid_rmse", "valid_mape"])
df["exp_type"] = df["exp_type"].astype(str)
df["model_type"] = df["model_type"].str.strip()

# Subset that also has proxy_rmse
df_proxy = df.dropna(subset=["valid_proxy_rmse"])

# ── Summary table ─────────────────────────────────────────────────────────────
def summary_table(data):
    grp = data.groupby(["model_type", "exp_type"])
    rows = []
    for (mt, et), g in grp:
        row = {
            "Model":          mt,
            "Exp":            et,
            "n":              len(g),
            "R² best":        round(g["valid_r2"].max(),  4),
            "R² mean":        round(g["valid_r2"].mean(), 4),
            "MAE best":       round(g["valid_mae"].min(),  0),
            "MAE mean":       round(g["valid_mae"].mean(), 0),
            "RMSE best":      round(g["valid_rmse"].min(), 0),
            "MAPE best":      round(g["valid_mape"].min(), 4),
        }
        if g["valid_proxy_rmse"].notna().any():
            row["ProxyRMSE best"] = round(g["valid_proxy_rmse"].min(), 0)
            row["ProxyRMSE mean"] = round(g["valid_proxy_rmse"].mean(), 0)
        rows.append(row)
    tbl = pd.DataFrame(rows).sort_values(["Model", "Exp"])
    return tbl

tbl = summary_table(df)
print("\n=== Summary by model × exp_type ===")
print(tbl.to_string(index=False))

# Top 10 overall
top10 = (df.nlargest(10, "valid_r2")
           [["Name", "model_type", "exp_type", "cluster_algo",
             "valid_r2", "valid_mae", "valid_rmse", "valid_mape", "valid_proxy_rmse"]]
           .rename(columns={"model_type": "Model", "exp_type": "Exp",
                             "cluster_algo": "Cluster"}))
print("\n=== Top-10 runs by R² ===")
print(top10.to_string(index=False))


# ═══════════════════════════════════════════════════════════════════════════
#  FIGURE 1 – Both model types overview (2×3 box+strip for all 5 metrics)
# ═══════════════════════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 3, figsize=(18, 11))
fig.suptitle("All Runs — CatBoost vs Ridge (valid set)", fontsize=15, fontweight="bold", y=1.01)

plot_specs = [
    ("valid_r2",         df),
    ("valid_mae",        df),
    ("valid_rmse",       df),
    ("valid_mape",       df),
    ("valid_proxy_rmse", df_proxy),
]
for ax, (metric, data) in zip(axes.flatten(), plot_specs):
    sns.boxplot(
        data=data, x="model_type", y=metric, hue="model_type",
        palette=PALETTE, width=0.45, linewidth=1.2, legend=False,
        flierprops=dict(marker="o", markersize=3, alpha=0.4), ax=ax,
    )
    sns.stripplot(
        data=data, x="model_type", y=metric, hue="model_type",
        palette=PALETTE, alpha=0.25, size=3.5, jitter=True, legend=False, ax=ax,
    )
    label = METRIC_LABELS[metric]
    if metric == "valid_proxy_rmse":
        label += f"\n(only exp_types {', '.join(sorted(PROXY_EXPS))})"
    ax.set_title(label, fontsize=10)
    ax.set_xlabel("")
    ax.set_ylabel("")

# Hide the unused 6th cell
axes[1, 2].set_visible(False)

plt.tight_layout()
plt.savefig(OUT / "01_both_boxplots.png", bbox_inches="tight")
plt.close()
print("Saved 01_both_boxplots.png")


# ═══════════════════════════════════════════════════════════════════════════
#  FIGURE 2 – Both models: all metrics by exp_type (strip + median diamond)
# ═══════════════════════════════════════════════════════════════════════════
metrics_fig2 = [
    ("valid_r2",         df),
    ("valid_mae",        df),
    ("valid_rmse",       df),
    ("valid_mape",       df),
    ("valid_proxy_rmse", df_proxy),
]
fig, axes = plt.subplots(1, 5, figsize=(28, 6))
fig.suptitle("Both Models — All Metrics by Experiment Type", fontsize=14, fontweight="bold")

for ax, (metric, data) in zip(axes, metrics_fig2):
    ord_ = [e for e in EXP_ORDER if e in data["exp_type"].unique()]
    sns.stripplot(
        data=data, x="exp_type", y=metric, hue="model_type",
        palette=PALETTE, order=ord_, alpha=0.4, size=4.5,
        jitter=0.15, dodge=True, ax=ax,
    )
    med = data.groupby(["exp_type", "model_type"])[metric].median().reset_index()
    for mt, color in PALETTE.items():
        m = med[(med["model_type"] == mt) & (med["exp_type"].isin(ord_))]
        xs = [ord_.index(e) + (-0.2 if mt == "catboost" else 0.2) for e in m["exp_type"]]
        ax.scatter(xs, m[metric], color=color, marker="D", s=60, zorder=5,
                   edgecolors="white", linewidths=0.8)

    ax.set_title(METRIC_LABELS[metric], fontsize=10)
    ax.set_xlabel("Exp type")
    ax.set_ylabel("")
    ax.tick_params(axis="x", labelsize=8)
    ax.get_legend().set_title("Model")
    ax.get_legend().get_title().set_fontsize(8)

plt.tight_layout()
plt.savefig(OUT / "02_both_by_exptype.png", bbox_inches="tight")
plt.close()
print("Saved 02_both_by_exptype.png")


# ═══════════════════════════════════════════════════════════════════════════
#  FIGURE 3 – Summary heatmaps (mean for all 5 metrics)
# ═══════════════════════════════════════════════════════════════════════════
heatmap_specs = [
    ("valid_r2",         df,       ".3f", "YlGn"),
    ("valid_mae",        df,       ".0f", "YlOrRd_r"),
    ("valid_rmse",       df,       ".0f", "YlOrRd_r"),
    ("valid_mape",       df,       ".3f", "YlOrRd_r"),
    ("valid_proxy_rmse", df_proxy, ".0f", "PuBu_r"),
]
fig, axes = plt.subplots(1, 5, figsize=(30, 4))
fig.suptitle("Mean metric by model × experiment type", fontsize=13, fontweight="bold")

for ax, (metric, data, fmt, cmap) in zip(axes, heatmap_specs):
    pivot = data.pivot_table(index="model_type", columns="exp_type",
                             values=metric, aggfunc="mean")
    pivot = pivot.reindex(columns=[c for c in EXP_ORDER if c in pivot.columns])
    sns.heatmap(pivot, annot=True, fmt=fmt, cmap=cmap, linewidths=0.5,
                ax=ax, cbar_kws={"shrink": 0.6})
    ax.set_title(METRIC_LABELS[metric], fontsize=9)
    ax.set_xlabel("Exp type")
    ax.set_ylabel("")
    ax.tick_params(axis="x", labelsize=8)

plt.tight_layout()
plt.savefig(OUT / "03_both_heatmap.png", bbox_inches="tight")
plt.close()
print("Saved 03_both_heatmap.png")


# ═══════════════════════════════════════════════════════════════════════════
#  FIGURE 4 – CatBoost deep-dive (all 5 metrics violin + scatter)
# ═══════════════════════════════════════════════════════════════════════════
cb = df[df["model_type"] == "catboost"].copy()
cb_proxy = df_proxy[df_proxy["model_type"] == "catboost"].copy()
order_cb = [e for e in EXP_ORDER if e in cb["exp_type"].unique()]

fig = plt.figure(figsize=(22, 14))
fig.suptitle("CatBoost — Deep Dive", fontsize=15, fontweight="bold")
gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.50, wspace=0.38)

# Row 0: violin distributions for all 5 metrics (span full width × 2 rows split into 3+2)
violin_metrics = [
    ("valid_r2",         cb,       "R²"),
    ("valid_mae",        cb,       "MAE"),
    ("valid_rmse",       cb,       "RMSE"),
    ("valid_mape",       cb,       "MAPE"),
    ("valid_proxy_rmse", cb_proxy, "Proxy RMSE"),
]
violin_positions = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)]

for (r, c), (metric, data, ylabel) in zip(violin_positions, violin_metrics):
    ax = fig.add_subplot(gs[r, c])
    ord_v = [e for e in EXP_ORDER if e in data["exp_type"].unique()]
    sns.violinplot(data=data, x="exp_type", y=metric, order=ord_v,
                   color=PALETTE["catboost"], inner="box", ax=ax, linewidth=1.0)
    sns.stripplot(data=data, x="exp_type", y=metric, order=ord_v,
                  color="white", alpha=0.5, size=2.5, jitter=True, ax=ax)
    ax.set_title(f"{ylabel} by exp_type", fontsize=10)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel, fontsize=9)
    ax.tick_params(axis="x", labelsize=8)
    best_val = data[metric].max() if metric == "valid_r2" else data[metric].min()
    ax.axhline(best_val, ls="--", color="gray", lw=0.8,
               label=f"best={best_val:,.3f}" if metric in ("valid_r2","valid_mape")
                     else f"best={best_val:,.0f}")
    ax.legend(fontsize=7)

# Row 1, col 2: best R² bar per exp_type
ax_bar = fig.add_subplot(gs[1, 2])
best_cb_idx = cb.groupby("exp_type")["valid_r2"].idxmax()
best_cb = cb.loc[best_cb_idx].set_index("exp_type").reindex(order_cb)
bars = ax_bar.bar(best_cb.index, best_cb["valid_r2"],
                  color=PALETTE["catboost"], edgecolor="white", linewidth=0.7)
for bar, v in zip(bars, best_cb["valid_r2"]):
    ax_bar.text(bar.get_x() + bar.get_width()/2, v + 0.003, f"{v:.3f}",
                ha="center", va="bottom", fontsize=7)
ax_bar.set_ylim(0.5, 0.92)
ax_bar.set_title("Best R² per exp_type", fontsize=10)
ax_bar.set_xlabel("Exp type")
ax_bar.set_ylabel("R²")
ax_bar.tick_params(axis="x", labelsize=8)

# Row 2: R² vs MAE scatter + cluster algo box
ax_scatter = fig.add_subplot(gs[2, :2])
exp_palette = sns.color_palette("tab10", n_colors=len(order_cb))
exp_color_map = dict(zip(order_cb, exp_palette))
for et in order_cb:
    sub = cb[cb["exp_type"] == et]
    ax_scatter.scatter(sub["valid_mae"], sub["valid_r2"],
                       color=exp_color_map[et], label=f"exp {et}",
                       alpha=0.6, s=40, edgecolors="white", linewidths=0.4)
ax_scatter.set_xlabel("MAE (↓ better)")
ax_scatter.set_ylabel("R² (↑ better)")
ax_scatter.set_title("CatBoost: R² vs MAE trade-off by exp_type", fontsize=10)
ax_scatter.legend(title="Exp type", fontsize=7, ncol=2)

ax_clust = fig.add_subplot(gs[2, 2])
cb_clust = cb[cb["cluster_algo"].isin(["hdbscan", "kmeans"])]
sns.boxplot(data=cb_clust, x="cluster_algo", y="valid_r2", hue="cluster_algo",
            palette={"hdbscan": "#6C8EBF", "kmeans": "#82B366"},
            width=0.5, ax=ax_clust, linewidth=1.2, legend=False)
sns.stripplot(data=cb_clust, x="cluster_algo", y="valid_r2", hue="cluster_algo",
              palette={"hdbscan": "#6C8EBF", "kmeans": "#82B366"},
              alpha=0.3, size=3.5, jitter=True, legend=False, ax=ax_clust)
ax_clust.set_title("Clustering algo effect on R²", fontsize=10)
ax_clust.set_xlabel("")
ax_clust.set_ylabel("R²")

plt.savefig(OUT / "04_catboost_deep.png", bbox_inches="tight")
plt.close()
print("Saved 04_catboost_deep.png")


# ═══════════════════════════════════════════════════════════════════════════
#  FIGURE 5 – Ridge deep-dive (all available metrics)
# ═══════════════════════════════════════════════════════════════════════════
rd = df[df["model_type"] == "ridge"].copy()
rd_proxy = df_proxy[df_proxy["model_type"] == "ridge"].copy()
order_rd = [e for e in EXP_ORDER if e in rd["exp_type"].unique()]

fig = plt.figure(figsize=(22, 10))
fig.suptitle("Ridge Linear Regression — Deep Dive", fontsize=15, fontweight="bold")
gs5 = gridspec.GridSpec(2, 4, figure=fig, hspace=0.45, wspace=0.38)

# Row 0: violin for all 5 metrics
ridge_violin_specs = [
    ("valid_r2",         rd,       "R²"),
    ("valid_mae",        rd,       "MAE"),
    ("valid_rmse",       rd,       "RMSE"),
    ("valid_mape",       rd,       "MAPE"),
]
for col, (metric, data, ylabel) in enumerate(ridge_violin_specs):
    ax = fig.add_subplot(gs5[0, col])
    ord_v = [e for e in EXP_ORDER if e in data["exp_type"].unique()]
    sns.violinplot(data=data, x="exp_type", y=metric, order=ord_v,
                   color=PALETTE["ridge"], inner="box", ax=ax, linewidth=1.0)
    sns.stripplot(data=data, x="exp_type", y=metric, order=ord_v,
                  color="white", alpha=0.6, size=3, jitter=True, ax=ax)
    ax.set_title(f"{ylabel} by exp_type", fontsize=10)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel, fontsize=9)

# Row 1 col 0: proxy_rmse violin (only where available)
ax_pv = fig.add_subplot(gs5[1, 0])
if len(rd_proxy) > 0:
    ord_rp = [e for e in EXP_ORDER if e in rd_proxy["exp_type"].unique()]
    sns.violinplot(data=rd_proxy, x="exp_type", y="valid_proxy_rmse", order=ord_rp,
                   color=PALETTE["ridge"], inner="box", ax=ax_pv, linewidth=1.0)
    sns.stripplot(data=rd_proxy, x="exp_type", y="valid_proxy_rmse", order=ord_rp,
                  color="white", alpha=0.6, size=3, jitter=True, ax=ax_pv)
    ax_pv.set_title("Proxy RMSE by exp_type", fontsize=10)
    ax_pv.set_xlabel("")
    ax_pv.set_ylabel("Proxy RMSE", fontsize=9)

# Row 1 cols 1-2: alpha scatterplots
rd_log = rd.copy()
rd_log["log_alpha1"] = np.log10(rd_log["ridge_alpha1"].clip(lower=1e-6))
rd_log["log_alpha2"] = np.log10(rd_log["ridge_alpha2"].clip(lower=1e-6))

for col, (xcol, xlabel) in enumerate([("log_alpha1", "log₁₀(alpha₁)"),
                                        ("log_alpha2", "log₁₀(alpha₂)")], start=1):
    ax = fig.add_subplot(gs5[1, col])
    sc = ax.scatter(rd_log[xcol], rd_log["valid_r2"],
                    c=rd_log["valid_r2"], cmap="Blues", s=45,
                    edgecolors="gray", linewidths=0.3, alpha=0.85)
    plt.colorbar(sc, ax=ax, label="R²")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("R²")
    ax.set_title(f"Ridge: {xlabel} vs R²", fontsize=10)

# Row 1 col 3: R² vs MAE scatter colored by exp_type
ax_sc = fig.add_subplot(gs5[1, 3])
exp_palette_r = sns.color_palette("tab10", n_colors=len(order_rd))
exp_color_map_r = dict(zip(order_rd, exp_palette_r))
for et in order_rd:
    sub = rd[rd["exp_type"] == et]
    ax_sc.scatter(sub["valid_mae"], sub["valid_r2"],
                  color=exp_color_map_r[et], label=f"exp {et}",
                  alpha=0.65, s=40, edgecolors="white", linewidths=0.4)
ax_sc.set_xlabel("MAE (↓ better)")
ax_sc.set_ylabel("R² (↑ better)")
ax_sc.set_title("Ridge: R² vs MAE by exp_type", fontsize=10)
ax_sc.legend(title="Exp", fontsize=7)

plt.savefig(OUT / "05_ridge_deep.png", bbox_inches="tight")
plt.close()
print("Saved 05_ridge_deep.png")


# ═══════════════════════════════════════════════════════════════════════════
#  FIGURE 6 – Best run per (exp_type × model_type × cluster_algo)
#  All exp_types, all model/cluster combos. Best = highest R² within group.
#  Proxy RMSE shown where available; gray placeholder where not logged.
# ═══════════════════════════════════════════════════════════════════════════

def cluster_size_str(row):
    algo = row["cluster_algo"]
    if algo == "hdbscan":
        v = row["hdb_min_cluster_size"]
        return f"cs={int(v)}" if pd.notna(v) else "cs=?"
    if algo == "kmeans":
        v = row["n_clusters"]
        return f"k={int(v)}" if pd.notna(v) else "k=?"
    return "no clust"

def param_str(row):
    if row["model_type"] == "catboost":
        parts = []
        if pd.notna(row.get("cb_depth1")):  parts.append(f"d={int(row['cb_depth1'])}")
        if pd.notna(row.get("cb_iters1")):  parts.append(f"it={int(row['cb_iters1'])}")
        if pd.notna(row.get("cb_lr1")):     parts.append(f"lr={row['cb_lr1']:.3f}")
        return " ".join(parts)
    else:
        parts = []
        if pd.notna(row.get("ridge_alpha1")): parts.append(f"α₁={row['ridge_alpha1']:.1f}")
        if pd.notna(row.get("ridge_alpha2")): parts.append(f"α₂={row['ridge_alpha2']:.1f}")
        return " ".join(parts)

# Best run per (exp_type × model_type × cluster_algo)
best_idx = df.groupby(["exp_type", "model_type", "cluster_algo"])["valid_r2"].idxmax()
best = df.loc[best_idx].copy()

best["clust_size"]   = best.apply(cluster_size_str, axis=1)
best["params"]       = best.apply(param_str, axis=1)

# Y-axis label: "model | cluster_algo | clust_size"
best["row_label"] = (
    best["model_type"] + "  |  "
    + best["cluster_algo"] + "  |  "
    + best["clust_size"]
)

# Sort: exp_type order, then model_type, then cluster_algo
exp_order_map = {e: i for i, e in enumerate(EXP_ORDER)}
best["_exp_ord"] = best["exp_type"].map(exp_order_map).fillna(99)
best = best.sort_values(["_exp_ord", "model_type", "cluster_algo"]).reset_index(drop=True)

print("\n=== Best run per (exp_type × model_type × cluster_algo) ===")
print(best[["exp_type", "row_label", "params",
            "valid_r2", "valid_mae", "valid_rmse",
            "valid_mape", "valid_proxy_rmse"]].to_string(index=False))

# ── Build figure ─────────────────────────────────────────────────────────
N = len(best)
ROW_H = 0.38          # inches per row
FIG_H  = max(8, N * ROW_H + 2.5)

bar_specs = [
    ("valid_r2",         ".3f",  "YlGn",    False),
    ("valid_mae",        ",.0f", "YlOrRd",  True),
    ("valid_rmse",       ",.0f", "YlOrRd",  True),
    ("valid_mape",       ".3f",  "YlOrRd",  True),
    ("valid_proxy_rmse", ",.0f", "PuBu",    True),
]

fig, axes = plt.subplots(1, 5, figsize=(32, FIG_H))
fig.suptitle(
    "Best run per exp_type × model × cluster  —  all metrics\n"
    "(proxy RMSE only where logged; params on R² bar)",
    fontsize=13, fontweight="bold", y=1.01,
)

yticks = np.arange(N)
HATCH = {"hdbscan": "///", "kmeans": "...", "none": ""}
bar_colors = [PALETTE.get(mt, "#999") for mt in best["model_type"]]

for ax, (metric, fmt, _, lower_better) in zip(axes, bar_specs):
    vals = best[metric]
    has_val = vals.notna()

    # Gray placeholder for missing proxy_rmse
    if metric == "valid_proxy_rmse":
        placeholder = vals.max(skipna=True) * 0.05 if has_val.any() else 1
        ax.barh(yticks[~has_val],
                [placeholder] * (~has_val).sum(),
                color="#e8e8e8", edgecolor="white", height=0.62)
        for i in yticks[~has_val]:
            ax.text(placeholder / 2, i, "—", ha="center", va="center",
                    fontsize=8, color="#aaa")

    # Real bars
    bars = ax.barh(
        yticks[has_val], vals[has_val],
        color=[bar_colors[i] for i in yticks[has_val]],
        edgecolor="white", linewidth=0.5, height=0.62,
    )
    # Hatch by cluster_algo
    for bar, i in zip(bars, yticks[has_val]):
        bar.set_hatch(HATCH.get(best.at[i, "cluster_algo"], ""))

    # Value + params labels (only on R² panel)
    x_max = vals.max(skipna=True) if has_val.any() else 1
    for i in yticks[has_val]:
        v = vals.iloc[i]
        label = format(v, fmt)
        if metric == "valid_r2":
            label += f"  [{best.at[i, 'params']}]"
        ax.text(v + x_max * 0.012, i, label,
                va="center", fontsize=7.8)

    ax.set_title(METRIC_LABELS[metric], fontsize=10, pad=6)
    ax.set_xlabel("")
    ax.set_xlim(right=(x_max * 1.35 if metric == "valid_r2" else x_max * 1.22))
    ax.invert_xaxis() if lower_better else None
    ax.set_xlabel("← lower is better" if lower_better else "higher is better →",
                  fontsize=8, color="#666")

# ── Shared Y-axis labels (row labels + exp_type group headers) ─────────
ax0 = axes[0]
ax0.set_yticks(yticks)
ax0.set_yticklabels(best["row_label"], fontsize=8.5)

# Exp_type group separators and headers on first panel
prev_exp = None
for i, row in best.iterrows():
    et = row["exp_type"]
    if et != prev_exp:
        # horizontal separator above the group
        if i > 0:
            for ax in axes:
                ax.axhline(i - 0.5, color="#ccc", linewidth=0.8, linestyle="--")
        # exp_type label left of the first panel
        ax0.text(-ax0.get_xlim()[0] * 0.02, i,
                 f" exp {et}", va="center", ha="right",
                 fontsize=9, fontweight="bold", color="#444",
                 transform=ax0.get_yaxis_transform())
        prev_exp = et

# Hide ytick labels on panels 1-4 (they share the axis from panel 0)
for ax in axes[1:]:
    ax.set_yticks(yticks)
    ax.set_yticklabels([])

# ── Legend ────────────────────────────────────────────────────────────────
model_patches = [Patch(facecolor=c, label=m) for m, c in PALETTE.items()]
hatch_patches = [
    Patch(facecolor="white", edgecolor="#555", hatch="///", label="hdbscan"),
    Patch(facecolor="white", edgecolor="#555", hatch="...", label="kmeans"),
    Patch(facecolor="white", edgecolor="#555", hatch="",    label="none"),
]
axes[-1].legend(handles=model_patches + hatch_patches,
                title="model  /  cluster", fontsize=8,
                loc="lower right", framealpha=0.85)

plt.tight_layout()
plt.savefig(OUT / "06_best_per_group.png", bbox_inches="tight")
plt.close()
print("Saved 06_best_per_group.png")


# ═══════════════════════════════════════════════════════════════════════════
#  FIGURE 7 – CatBoost hyperparameters heatmap (R² mean)
# ═══════════════════════════════════════════════════════════════════════════
cb_hp = cb.copy()
cb_hp["depth_combo"] = cb_hp["cb_depth1"].astype(str) + "+" + cb_hp["cb_depth2"].astype(str)
cb_hp["iters1_bin"] = pd.cut(cb_hp["cb_iters1"], bins=4,
                              labels=["low", "med-lo", "med-hi", "high"])
pivot_hp = cb_hp.pivot_table(index="depth_combo", columns="iters1_bin",
                              values="valid_r2", aggfunc="mean",
                              observed=False)
if pivot_hp.shape[0] > 1 and pivot_hp.shape[1] > 1:
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(pivot_hp, annot=True, fmt=".3f", cmap="YlGn",
                linewidths=0.5, ax=ax, cbar_kws={"shrink": 0.7})
    ax.set_title("CatBoost: mean R² by depth combo × iters₁ bucket", fontsize=12)
    ax.set_xlabel("iters₁ bin")
    ax.set_ylabel("depth₁ + depth₂")
    plt.tight_layout()
    plt.savefig(OUT / "07_catboost_hp_heatmap.png", bbox_inches="tight")
    plt.close()
    print("Saved 07_catboost_hp_heatmap.png")
else:
    print("Skipped 07 — not enough depth combos")


# ═══════════════════════════════════════════════════════════════════════════
#  PRINT: best params per model type
# ═══════════════════════════════════════════════════════════════════════════
print("\n\n=== Best CatBoost run ===")
best_cb_row = cb.loc[cb["valid_r2"].idxmax()]
print(best_cb_row[["Name","exp_type","cluster_algo",
                    "cb_depth1","cb_depth2","cb_iters1","cb_iters2",
                    "cb_lr1","cb_lr2",
                    "valid_r2","valid_mae","valid_rmse","valid_mape","valid_proxy_rmse"]].to_string())

print("\n=== Best Ridge run ===")
rd_all = df[df["model_type"] == "ridge"]
best_rd_row = rd_all.loc[rd_all["valid_r2"].idxmax()]
print(best_rd_row[["Name","exp_type","cluster_algo",
                   "ridge_alpha1","ridge_alpha2",
                   "valid_r2","valid_mae","valid_rmse","valid_mape","valid_proxy_rmse"]].to_string())

print(f"\nAll charts saved to: {OUT}")
