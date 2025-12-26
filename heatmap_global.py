import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import os

# =====================
# 1. Папка для картинок
# =====================
os.makedirs("images", exist_ok=True)

# =====================
# 2. Читаем parquet
# =====================
cols = [
    "longitude",
    "latitude",
    "market_type",
    "price_per_square_meter"
]

df = pq.read_table(
    "housing(2).parquet",
    columns=cols
).to_pandas()

df = df.dropna(subset=cols)

# =====================
# 3. Ограничение по Москве
# =====================
lon_min, lon_max = 37.35, 37.85
lat_min, lat_max = 55.55, 55.95

df = df[
    (df.longitude >= lon_min) & (df.longitude <= lon_max) &
    (df.latitude >= lat_min) & (df.latitude <= lat_max)
]

# =====================
# 4. Делим на рынки
# =====================
# Делим на рынки
primary_df = df[df["market_type"] == "primary"].copy()
secondary_df = df[df["market_type"] == "secondary"].copy()

# 🔹 ВЫВОД КОЛИЧЕСТВА ТОЧЕК
print(f"PRIMARY: {len(primary_df)} точек")
print(f"SECONDARY: {len(secondary_df)} точек")


# =====================
# 5. GeoDataFrame
# =====================
def to_gdf(df):
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    ).to_crs(epsg=3857)

gdf_primary = to_gdf(primary_df)
gdf_secondary = to_gdf(secondary_df)

# =====================
# 6. Общая шкала цветов
# =====================
vmin = df["price_per_square_meter"].quantile(0.05)
vmax = df["price_per_square_meter"].quantile(0.95)

# =====================
# 7. Функция отрисовки
# =====================
def draw_heatmap(gdf, title, filename, cmap):
    fig, ax = plt.subplots(figsize=(12, 12))

    gdf.plot(
        ax=ax,
        column="price_per_square_meter",
        cmap=cmap,
        markersize=8,
        alpha=0.75,
        vmin=vmin,
        vmax=vmax,
        legend=True
    )

    # 1️⃣ Добавляем карту
    ctx.add_basemap(
        ax,
        source=ctx.providers.OpenStreetMap.Mapnik
    )

    # 2️⃣ ДЕЛАЕМ КАРТУ ПРОЗРАЧНЕЕ
    for im in ax.get_images():
        im.set_alpha(0.4)

    ax.set_title(title)
    ax.set_axis_off()

    plt.tight_layout()
    plt.savefig(filename, dpi=300)
    plt.close(fig)

    print(f"Сохранено: {filename}")

# =====================
# 8. ДВА HEATMAP'А
# =====================
draw_heatmap(
    gdf_primary,
    "Primary market — price per m²",
    "images/heatmap_primary_map_1.png",
    cmap="Reds"
)

draw_heatmap(
    gdf_secondary,
    "Secondary market — price per m²",
    "images/heatmap_secondary_map_1.png",
    cmap="Blues"
)
