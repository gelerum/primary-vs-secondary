import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
from typing import Optional, Tuple, Dict

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
    Создает три карты недвижимости: общую, только первичку и только вторичку.

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame с данными о недвижимости
    output_basename : str
        Базовое имя для выходных файлов (к нему добавляются суффиксы)
    bbox : tuple, optional
        Границы области в формате (lon_min, lon_max, lat_min, lat_max)
        По умолчанию: Москва (37.35, 37.85, 55.55, 55.95)
    figsize : tuple
        Размер фигуры в дюймах
    dpi : int
        Разрешение для сохранения изображений
    colors : dict
        Цвета для типов рынка, например: {'primary': 'red', 'secondary': 'blue'}
    alpha : dict
        Прозрачность для типов рынка, например: {'primary': 0.5, 'secondary': 0.5}
    markersize : dict
        Размер маркеров для типов рынка
    basemap_source
        Источник подложки карты из contextily.providers
    market_type_col : str
        Название колонки с типом рынка
    lon_col : str
        Название колонки с долготой
    lat_col : str
        Название колонки с широтой
    """

    # Параметры по умолчанию
    if bbox is None:
        bbox = (37.35, 37.85, 55.55, 55.95)  # Москва по умолчанию

    if colors is None:
        colors = {"primary": "red", "secondary": "blue"}

    if alpha is None:
        alpha = {"primary": 0.5, "secondary": 0.5}

    if markersize is None:
        markersize = {"primary": 1, "secondary": 1}

    lon_min, lon_max, lat_min, lat_max = bbox

    # Фильтрация по bounding box
    mask = (
        (df[lon_col] >= lon_min)
        & (df[lon_col] <= lon_max)
        & (df[lat_col] >= lat_min)
        & (df[lat_col] <= lat_max)
    )
    filtered_df = df[mask].copy()

    # Разделение на первичку и вторичку
    primary_df = filtered_df[filtered_df[market_type_col] == "primary"].copy()
    secondary_df = filtered_df[filtered_df[market_type_col] == "secondary"].copy()

    # Создание GeoDataFrame
    def create_gdf(dataframe, lon_col=lon_col, lat_col=lat_col):
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

    # Функция для создания и сохранения карты
    def save_map(gdf_list, title, filename_suffix):
        fig, ax = plt.subplots(figsize=figsize)

        # Отображение каждого GeoDataFrame
        for gdf_info in gdf_list:
            if gdf_info["gdf"] is not None and len(gdf_info["gdf"]) > 0:
                gdf_info["gdf"].plot(
                    ax=ax,
                    markersize=markersize.get(gdf_info["type"], 1),
                    color=colors.get(gdf_info["type"], "gray"),
                    alpha=alpha.get(gdf_info["type"], 0.5),
                    label=gdf_info.get("label", gdf_info["type"]),
                )

        # Добавление подложки
        ctx.add_basemap(ax, source=basemap_source)

        # Настройка внешнего вида
        ax.set_axis_off()
        if title:
            ax.set_title(title, fontsize=16, pad=20)
        if len(gdf_list) > 1:
            ax.legend(loc="upper right", framealpha=0.8)

        plt.tight_layout()

        # Сохранение
        filename = f"{output_basename}_{filename_suffix}.png"
        plt.savefig(filename, dpi=dpi, bbox_inches="tight")
        plt.close()
        print(f"Сохранено: {filename}")

    # 1. Общая карта (первичка + вторичка)
    gdf_list_all = [
        {"gdf": gdf_secondary, "type": "secondary", "label": "Вторичный рынок"},
        {"gdf": gdf_primary, "type": "primary", "label": "Первичный рынок"},
    ]
    save_map(gdf_list_all, "Распределение недвижиммости", "all")

    # 2. Карта только первички
    if gdf_primary is not None and len(gdf_primary) > 0:
        save_map(
            [{"gdf": gdf_primary, "type": "primary"}],
            "Распределение недвижиммости (первичный рынок)",
            "primary",
        )

    # 3. Карта только вторички
    if gdf_secondary is not None and len(gdf_secondary) > 0:
        save_map(
            [{"gdf": gdf_secondary, "type": "secondary"}],
            "Распределение недвижиммости (вторичный рынок)",
            "secondary",
        )


# Пример использования функции:
def plot_moscow_real_estate(df: pd.DataFrame, output_prefix: str = "moscow"):
    """
    Упрощенная обертка для визуализации недвижимости Москвы

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame с данными о недвижимости
    output_prefix : str
        Префикс для выходных файлов
    """
    # Параметры для Москвы
    moscow_bbox = (37.35, 37.85, 55.55, 55.95)

    # Настройки визуализации
    colors = {
        "primary": "#FF6B6B",  # Красный для первички
        "secondary": "#4ECDC4",  # Бирюзовый для вторички
    }

    alpha = {"primary": 0.7, "secondary": 0.5}

    markersize = {"primary": 2, "secondary": 1}

    # Вызов основной функции
    plot_real_estate_maps(
        df=df,
        output_basename=output_prefix,
        bbox=moscow_bbox,
        colors=colors,
        alpha=alpha,
        markersize=markersize,
    )


# Минималистичная версия для быстрого использования:
def plot_simple_maps(df: pd.DataFrame, output_name: str = "moscow"):
    """
    Простая версия для быстрой визуализации

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame с данными
    output_name : str
        Имя для выходных файлов
    """
    plot_real_estate_maps(
        df=df,
        output_basename=output_name,
        colors={"primary": "red", "secondary": "blue"},
        alpha={"primary": 0.7, "secondary": 0.5},
        markersize={"primary": 2, "secondary": 1},
    )


plot_simple_maps(df, "data/plots/market_points_map")
