import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
from typing import Optional, Tuple, Dict

# Загружаем данные
df_raw = pd.read_parquet("data/processed/housing.parquet")
df = df_raw.copy()


def plot_real_estate_maps(
    df: pd.DataFrame,
    output_basename: str = "moscow",
    bbox: Optional[Tuple[float, float, float, float]] = None,
    figsize: Tuple[int, int] = (12, 12),
    dpi: int = 300,
    colors: Dict[str, str] = None,
    alpha: Dict[str, float] = None,
    markersize: Dict[str, float] = None,
    basemap_source=ctx.providers.OpenStreetMap.Mapnik,
    market_type_col: str = "market_type",
    lon_col: str = "longitude",
    lat_col: str = "latitude",
) -> None:
    """
    Создает карты недвижимости и выводит точное количество точек каждого типа на карте.
    """

    if bbox is None:
        bbox = (37.35, 37.85, 55.55, 55.95)  # Москва по умолчанию

    if colors is None:
        colors = {"primary": "red", "secondary": "blue"}

    if alpha is None:
        alpha = {"primary": 0.7, "secondary": 0.7}

    if markersize is None:
        markersize = {"primary": 2, "secondary": 2}

    lon_min, lon_max, lat_min, lat_max = bbox

    # Фильтрация по bounding box
    mask = (
        (df[lon_col] >= lon_min)
        & (df[lon_col] <= lon_max)
        & (df[lat_col] >= lat_min)
        & (df[lat_col] <= lat_max)
    )
    filtered_df = df[mask].copy()

    # Разделение по типу рынка
    primary_df = filtered_df[filtered_df[market_type_col] == "primary"].copy()
    secondary_df = filtered_df[filtered_df[market_type_col] == "secondary"].copy()

    # Подсчет точек и процентов
    total_points = len(filtered_df)
    primary_count = len(primary_df)
    secondary_count = len(secondary_df)
    primary_pct = primary_count / total_points * 100 if total_points > 0 else 0
    secondary_pct = secondary_count / total_points * 100 if total_points > 0 else 0

    print(f"Всего точек в bbox: {total_points}")
    print(f"Первичный рынок: {primary_count} ({primary_pct:.1f}%)")
    print(f"Вторичный рынок: {secondary_count} ({secondary_pct:.1f}%)")

    # Создание GeoDataFrame
    def create_gdf(dataframe):
        if len(dataframe) == 0:
            return None
        gdf = gpd.GeoDataFrame(
            dataframe,
            geometry=gpd.points_from_xy(dataframe[lon_col], dataframe[lat_col]),
            crs="EPSG:4326",
        )
        return gdf.to_crs(epsg=3857)

    gdf_primary = create_gdf(primary_df)
    gdf_secondary = create_gdf(secondary_df)

    # Функция для построения и сохранения карты с подписью количества точек
    def save_map(gdf_list, title, filename_suffix):
        fig, ax = plt.subplots(figsize=figsize)

        # Отображаем каждый GeoDataFrame
        for gdf_info in gdf_list:
            gdf_plot = gdf_info["gdf"]
            if gdf_plot is not None and len(gdf_plot) > 0:
                gdf_plot.plot(
                    ax=ax,
                    markersize=markersize.get(gdf_info["type"], 2),
                    color=colors.get(gdf_info["type"], "gray"),
                    alpha=alpha.get(gdf_info["type"], 0.7),
                    label=f"{gdf_info['label']} ({len(gdf_plot)})",
                )

        # Подложка
        ctx.add_basemap(ax, source=basemap_source, alpha=0.4)

        # Настройка внешнего вида
        ax.set_axis_off()
        if title:
            ax.set_title(title, fontsize=16, pad=20)
        ax.legend(loc="upper right", framealpha=0.8)

        plt.tight_layout()
        filename = f"{output_basename}_{filename_suffix}.png"
        plt.savefig(filename, dpi=dpi, bbox_inches="tight")
        plt.close()
        print(f"Сохранено: {filename}")

    # 1. Общая карта
    gdf_list_all = [
        {"gdf": gdf_secondary, "type": "secondary", "label": "Secondary market"},
        {"gdf": gdf_primary, "type": "primary", "label": "Primary market"},
    ]
    save_map(gdf_list_all, f"Market distribution (Total: {total_points})", "all")

    # 2. Только первичный рынок
    if gdf_primary is not None and len(gdf_primary) > 0:
        save_map(
            [{"gdf": gdf_primary, "type": "primary", "label": "Primary market"}],
            "Market distribution",
            "primary",
        )

    # 3. Только вторичный рынок
    if gdf_secondary is not None and len(gdf_secondary) > 0:
        save_map(
            [{"gdf": gdf_secondary, "type": "secondary", "label": "Secondary market"}],
            "Market distribution",
            "secondary",
        )


# Обертка для быстрого вызова
def plot_simple_maps(df: pd.DataFrame, output_name: str = "moscow"):
    plot_real_estate_maps(
        df=df,
        output_basename=output_name,
        colors={"primary": "red", "secondary": "blue"},
        alpha={"primary": 0.7, "secondary": 0.7},
        markersize={"primary": 2, "secondary": 2},
    )


# Вызов
plot_simple_maps(df, "data/plots/market_distribution_map")
