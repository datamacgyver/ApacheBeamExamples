import pandas as pd

df = pd.read_csv("transformers.csv")
df.to_parquet("transformers.parquet", index=False)
df.to_json("transformers.json", orient="records")
