import pandas as pd

from src.report.plots import plot_heatmap, plot_pdf_ecdf, plot_pdf_ecdf_by_district
from dvc.api import params_show


def main():
    params = params_show()["02_geocode"]

    df = pd.read_parquet("data/interim/05_administrative_district.parquet")

    fig = plot_pdf_ecdf(
        df,
        column="primary_to_secondary_wnir_ratio",
        title="Primary to Secondary Price Ratio",
        bin_size=0.1,
    )
    fig.write_image(
        "reports/pdf_ecdf_primary_to_secondary_price_ratio.png", scale=2, format="png"
    )

    fig = plot_pdf_ecdf_by_district(
        df=df,
        column="primary_to_secondary_wnir_ratio",
        district_col="administrative_district",
        bin_size=0.1,
        title="Primary to Secondary Price Ratio by administrative district",
    )
    fig.write_image(
        "reports/pdf_ecdf_primary_to_secondary_price_ratio_by_administrative_district.png",
        scale=2,
        format="png",
    )

    m = plot_heatmap(
        df,
        "latitude",
        "longitude",
        "primary_to_secondary_wnir_ratio",
        "Primary to Secondary Price Ratio heatmap",
        params["geo"]["longitude"]["min"],
        params["geo"]["longitude"]["max"],
        params["geo"]["latitude"]["min"],
        params["geo"]["latitude"]["max"],
        15,
    )

    m.save("reports/heatmap.html")


if __name__ == "__main__":
    main()
