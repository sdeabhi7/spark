import pandas as pd
import os

df = pd.read_csv('prod.csv')
df.to_parquet('prod.parquet', engine='pyarrow')