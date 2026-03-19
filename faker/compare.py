import pandas as pd
import numpy as np

df = pd.read_csv("data/sampled_accidents.csv")
synthetic_df = pd.read_csv("generated_accidents.csv")



# COMPARE NUMERICAL MEAN AND STD

summary = []

for col in df.columns:

    if df[col].dtype in [np.float64, np.int64]:

        summary.append({
            "column": col,
            "real_mean": df[col].mean(),
            "synthetic_mean": synthetic_df[col].mean(),
            "real_std": df[col].std(),
            "synthetic_std": synthetic_df[col].std()
        })

comparison_df = pd.DataFrame(summary)

print()
print("="*100)
print("COMPARE MEAN AND STD FOR NUMERICAL COLUMNS")
print("="*100)
print(comparison_df)



# COMPARE NULL COUNT DISTRIBUTIONS
real_null = df.isnull().mean()
synthetic_null = synthetic_df.isnull().mean()

null_comparison = pd.DataFrame({
    "real_null_rate": real_null,
    "synthetic_null_rate": synthetic_null,
    "difference": synthetic_null - real_null
})

print()
print("="*100)
print("COMPARE NULL DISTRIBUTIONS")
print("="*100)
print(null_comparison)



# COMPARE BOOL DISTRIBUTIONS
bool_cols = [
"Amenity","Bump","Crossing","Give_Way","Junction","No_Exit",
"Railway","Roundabout","Station","Stop","Traffic_Calming",
"Traffic_Signal","Turning_Loop"
]

real_bool = df[bool_cols].mean()
synthetic_bool = synthetic_df[bool_cols].mean()

bool_compare = pd.DataFrame({
    "real_true_rate": real_bool,
    "synthetic_true_rate": synthetic_bool
})
print()
print("="*100)
print("COMPARE BOOLEAN DISTRIBUTIONS")
print("="*100)
print(bool_compare)



# COMPARE COORDS
print()
print("="*100)
print("COMPARE LOCATION DISTRIBUTIONS")
print("="*100)

df[["Start_Lat","Start_Lng"]].describe()
synthetic_df[["Start_Lat","Start_Lng"]].describe()

# COMPARE CATEGORICALS FOR A COLUMN: Weather_Condition
col = "Weather_Condition"

real_dist = df[col].value_counts(normalize=True)
synthetic_dist = synthetic_df[col].value_counts(normalize=True)