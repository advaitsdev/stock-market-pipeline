import pandas as pd
import os

raw_files = sorted(os.listdir("data/raw"))
latest = f"data/raw/{raw_files[-1]}"
df = pd.read_csv(latest)
print("Columns:", df.columns.tolist())
print(df.head(2))