import pandas as pd


def _read_msk_houses_deals():
    df = pd.read_csv("data/raw/msk_houses_deals_ds.csv")
    df = df.loc[df["flatType"].ne("flatType") & df["address"].ne("address")].copy()
    return df


def read_dfs():
    df1 = pd.read_csv("data/raw/Dataset_SCO_KVM_MONS_GRC_IZD_DMA_MTK_20250805.csv")
    df2 = pd.read_csv("data/raw/Etagi_secondary_classified_dataset_20250805.csv")
    df3 = pd.read_csv("data/raw/Etagi_secondary_dataset_20250805.csv")
    df4 = pd.read_parquet("data/raw/msk_united_geo_market_deals.parquet")
    df5 = _read_msk_houses_deals()

    return [df1, df2, df3, df4, df5]
