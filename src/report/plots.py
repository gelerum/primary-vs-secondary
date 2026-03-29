import folium
from folium.plugins import HeatMap
from plotly.subplots import make_subplots
import numpy as np
from plotly import graph_objects as go
import branca.colormap as cm


def plot_pdf_ecdf(df, column, title=None, bin_size=0.1):
    x = np.sort(df[column].dropna().values)

    fig = make_subplots(2, 1, shared_xaxes=True, subplot_titles=["PDF", "ECDF"])

    fig.add_trace(
        go.Histogram(x=x, histnorm="density", xbins=dict(size=bin_size), name="PDF"),
        row=1,
        col=1,
    )

    y = np.arange(1, len(x) + 1) / len(x)
    fig.add_trace(
        go.Scatter(x=x, y=y, mode="lines", name="ECDF"),
        row=2,
        col=1,
    )

    fig.update_layout(
        title=dict(
            text=title,
            xanchor="center",
            x=0.5,
        ),
        template="plotly_white",
        showlegend=False,
    )

    return fig


def plot_heatmap(
    df,
    x_col: str,
    y_col: str,
    z_col: str,
    title: str,
    x_min: float,
    x_max: float,
    y_min: float,
    y_max: float,
    radius: int,
):
    df = df[df["market_type"] == "primary"]

    # Копируем и очищаем данные
    data = df[[y_col, x_col, z_col]].copy()
    data = data.dropna()
    data = data[np.isfinite(data[z_col])]

    if len(data) == 0:
        raise ValueError("Нет валидных данных после очистки NaN/Inf")

    # Границы карты
    m = folium.Map(
        location=[(y_min + y_max) / 2, (x_min + x_max) / 2], zoom_start=10, tiles=None
    )
    folium.TileLayer("CartoDB positron", opacity=0.35).add_to(m)

    # === Viridis ===
    viridis_colors = [
        "#440154",
        "#482677",
        "#414487",
        "#355f8d",
        "#2a788e",
        "#21918c",
        "#22a884",
        "#44bf70",
        "#7ad151",
        "#bddf26",
    ]

    # Нормализация значений z
    z_min = data[z_col].min()
    z_max = data[z_col].max()

    data["weight"] = (data[z_col] - z_min) / (z_max - z_min)
    data["weight"] = data["weight"].clip(0, 1)

    heat_data = data[[x_col, y_col, "weight"]].values.tolist()

    # Gradient
    gradient = {
        i / (len(viridis_colors) - 1): color for i, color in enumerate(viridis_colors)
    }

    HeatMap(
        heat_data,
        radius=radius,
        blur=15,
        min_opacity=0.3,
        max_zoom=15,
        gradient=gradient,
    ).add_to(m)

    # Легенда
    colormap = cm.LinearColormap(
        colors=viridis_colors, vmin=z_min, vmax=z_max, caption=title
    )
    colormap = colormap.to_step(n=8)
    colormap.add_to(m)

    # Ограничиваем область просмотра
    m.fit_bounds([[y_min, x_min], [y_max, x_max]])

    return m
