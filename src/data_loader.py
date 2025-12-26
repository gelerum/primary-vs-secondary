import pandas as pd
from pathlib import Path


def read_dfs():
    RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

    df1_file = next(RAW_DIR.glob("*Dataset_SCO_KVM_MONS_GRC_IZD_DMA*.csv"))
    df2_file = next(RAW_DIR.glob("*Etagi_secondary_classified*.csv"))
    df3_file = next(RAW_DIR.glob("*Etagi_secondary_dataset*.csv"))
    df4_file = next(RAW_DIR.glob("*msk_united_geo_market_deals*.parquet"))

    df1 = pd.read_csv(df1_file)
    df2 = pd.read_csv(df2_file)
    df3 = pd.read_csv(df3_file)
    df4 = pd.read_parquet(df4_file)

    return [df1, df2, df3, df4]
