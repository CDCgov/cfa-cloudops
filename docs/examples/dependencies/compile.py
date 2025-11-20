import os

import pandas as pd

df = pd.DataFrame(columns=["state", "population", "affected"])

for file in os.listdir("data"):
    if file.endswith("_affected.csv"):
        in_df = pd.read_csv(os.path.join("data", file))
        df = pd.concat([df, in_df], axis=0, ignore_index=True)

print(df["affected"].sum())
