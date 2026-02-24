import pandas as pd

df = pd.read_parquet("housing_residential_processed.parquet")
df.to_csv("housing_residential_processed.csv", index=False)
