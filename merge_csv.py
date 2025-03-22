import pandas as pd

csv_files = ['prod_1.csv', 'prod_2.csv'] 
df_list = []

for file in csv_files:
    df = pd.read_csv(file)
    df_list.append(df)

merged_df = pd.concat(df_list, ignore_index=True)
merged_df.to_csv('prod.csv', index=False)