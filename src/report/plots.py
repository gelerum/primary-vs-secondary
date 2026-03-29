import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from plotly import graph_objects as go


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


def plot_map(df, x_col, y_col, z_col, title, x_min, x_max, y_min, y_max):
    df = df[df["market_type"] == "primary"].dropna(subset=[x_col, y_col, z_col])

    # Центр области
    center_lat = (y_min + y_max) / 2
    center_lon = (x_min + x_max) / 2

    # Примерный zoom (подбери под свои данные, обычно 5–12)
    # Можно посчитать автоматически, но проще подобрать
    zoom = 8

    fig = px.scatter_map(
        df,
        lat=y_col,  # внимание: в px.scatter_map это lat / lon, а не x_col/y_col!
        lon=x_col,
        color=z_col,
        color_continuous_scale="Viridis",
        title=title,
        zoom=zoom,  # ← задаём здесь
        center={"lat": center_lat, "lon": center_lon},  # ← и центр
        height=800,
    )

    fig.update_layout(
        map_style="carto-positron",  # новый параметр (был mapbox_style)
        margin=dict(l=0, r=0, t=0, b=0),
        uirevision=True,
        map=dict(bounds=dict(west=x_min, east=x_max, south=y_min, north=y_max)),
    )

    return fig
