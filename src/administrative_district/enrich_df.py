import geopandas as gpd


def add_administrative_district(df, shp_path):
    # 1. Create a GeoDataFrame from the input DataFrame
    points_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326",
    )

    districts_gdf = gpd.read_file(shp_path)

    districts_gdf = districts_gdf.to_crs(points_gdf.crs)

    joined_gdf = gpd.sjoin(
        points_gdf, districts_gdf[["NAME", "geometry"]], how="left", predicate="within"
    )

    joined_gdf = joined_gdf.drop(columns=["index_right", "geometry"])
    joined_gdf = joined_gdf.rename(columns={"NAME": "administrative_district"})

    return joined_gdf
