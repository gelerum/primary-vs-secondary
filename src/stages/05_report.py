import pandas as pd

from src.report.plots import plot_map, plot_pdf_ecdf
from dvc.api import params_show


def main():
    params = params_show()["02_geocode"]

    df = pd.read_parquet("data/interim/04_mean_knn.parquet")

    fig = plot_pdf_ecdf(
        df,
        column="primary_to_secondary_price_ratio",
        title="Primary to Secondary Price Ratio",
    )
    fig.write_image(
        "reports/pdf_ecdf_primary_to_secondary_price_ratio.png", scale=2, format="png"
    )

    fig = plot_map(
        df,
        "latitude",
        "longitude",
        "primary_to_secondary_price_ratio",
        "Primary to Secondary Price Ratio heatmap",
        params["geo"]["longitude"]["min"],
        params["geo"]["longitude"]["max"],
        params["geo"]["latitude"]["min"],
        params["geo"]["latitude"]["max"],
    )
    fig.write_image("reports/map.png", scale=2, format="png")


if __name__ == "__main__":
    main()
