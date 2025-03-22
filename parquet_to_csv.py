import pandas as pd
import os

df = pd.read_parquet('prod.parquet')
df.to_csv('prod.csv', index=False)