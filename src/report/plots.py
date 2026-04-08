import folium
from folium.plugins import HeatMap
from plotly.subplots import make_subplots
import numpy as np
from plotly import graph_objects as go
import branca.colormap as cm


def plot_pdf_ecdf(df, column, title, bin_size):
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


import pandas as pd


def plot_pdf_ecdf_by_district(
    df: pd.DataFrame,
    column: str,
    district_col,
    bin_size,
    title,
):

    height_per_district = 600
    # Get unique districts, preserving NaN, and sort nicely
    districts = df[district_col].unique()
    # Sort non-null districts and put Missing at the end
    sorted_districts = sorted([d for d in districts if pd.notna(d)])
    districts_list = sorted_districts + [np.nan]

    n_districts = len(districts_list)

    # Create subplot figure: 2 rows per district
    fig = make_subplots(
        rows=2 * n_districts,
        cols=1,
        subplot_titles=[
            f"{('Missing' if pd.isna(dist) else dist)} - PDF"
            if i % 2 == 0
            else f"{('Missing' if pd.isna(dist) else dist)} - ECDF"
            for dist in districts_list
            for i in range(2)
        ],
        vertical_spacing=0.035,
        shared_xaxes=False,
    )

    row = 1
    for district in districts_list:
        district_label = "Missing" if pd.isna(district) else str(district)

        # Filter data
        if pd.isna(district):
            subset = df[df[district_col].isna()]
        else:
            subset = df[df[district_col] == district]

        data = subset[column].dropna()

        if len(data) < 3:
            fig.add_annotation(
                text="Not enough data points (< 3)",
                xref="x domain",
                yref="y domain",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=14, color="gray"),
                row=row,
                col=1,
            )
            row += 2
            continue

        x = np.sort(data.values)

        # === PDF (Histogram) ===
        fig.add_trace(
            go.Histogram(
                x=x,
                histnorm="density",
                xbins=dict(size=bin_size),
                name=f"PDF - {district_label}",
                marker_color="#1f77b4",
            ),
            row=row,
            col=1,
        )

        # === ECDF ===
        y = np.arange(1, len(x) + 1) / len(x)
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines",
                name=f"ECDF - {district_label}",
                line=dict(color="#d62728", width=2.5),
            ),
            row=row + 1,
            col=1,
        )

        row += 2

    # Update layout
    main_title = title or f"Distribution of '{column}' by Administrative District"

    fig.update_layout(
        title=dict(text=main_title, x=0.5, xanchor="center", font=dict(size=22)),
        template="plotly_white",
        showlegend=False,
        height=max(700, n_districts * height_per_district),
        width=950,
        margin=dict(t=80, b=40, l=80, r=40),
    )

    fig.update_xaxes(
        title_text=column, row=2 * n_districts, col=1
    )  # only bottom x-axis label

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
