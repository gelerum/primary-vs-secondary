import pandas as pd


def concat_dfs(dfs):
    return pd.concat(dfs, ignore_index=True)
