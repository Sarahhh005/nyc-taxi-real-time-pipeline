import pandas as pd

df = pd.read_parquet('/opt/workspace/data/2025_full/yellow_tripdata_2025-01.parquet')

print('=== Shape ===')
print(df.shape)

print('\n=== Columns ===')
print(df.columns.tolist())

print('\n=== Basic Statistics ===')
print(df[['trip_distance', 'fare_amount', 'total_amount', 'passenger_count']].describe().to_string())

print('\n=== Top Pickup Locations (PULocationID) ===')
print(df['PULocationID'].value_counts().head(5).to_string())

print('\n=== Time Range ===')
print('Min pickup:', df['tpep_pickup_datetime'].min())
print('Max pickup:', df['tpep_pickup_datetime'].max())
