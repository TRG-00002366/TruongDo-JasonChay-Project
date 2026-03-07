import pandas as pd

df = pd.read_csv("US_Accidents_March23.csv")
df = df.sample(frac=0.01, random_state=1)
df.to_csv("sampled_accidents.csv", index=False)