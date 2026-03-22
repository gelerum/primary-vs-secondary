import pandas as pd


def read_dfs():
    df1 = pd.read_csv("data/raw/Dataset_SCO_KVM_MONS_GRC_IZD_DMA_MTK_20250805.csv")
    df2 = pd.read_csv("data/raw/Etagi_secondary_classified_dataset_20250805.csv")
    df3 = pd.read_csv("data/raw/Etagi_secondary_dataset_20250805.csv")
    df4 = pd.read_parquet("data/raw/msk_united_geo_market_deals.parquet")

    return [df1, df2, df3, df4]
