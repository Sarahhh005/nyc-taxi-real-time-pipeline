import pandas as pd

df = pd.read_parquet(
    "data/yellow_tripdata_2025-01.parquet"
)

print("Shape:")
print(df.shape)

print("\nColumns:")
print(df.columns)

print("\nFirst 5 rows:")
print(df.head())
