import pandas as pd
from pathlib import Path

parquet_path = Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_1987_2024.parquet")
out_path     = Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_wide_subset.parquet")

keep_vars = ["UNITID","year","tuition01","net_student_tuition","applcn","admssn","enrlt"]

available = pd.read_parquet(parquet_path, columns=None).columns
cols = [c for c in keep_vars if c in available]
if not cols:
    raise SystemExit("None of the requested columns found")

df = pd.read_parquet(parquet_path, columns=cols)
df["UNITID"] = df["UNITID"].astype("Int64")
df["year"]   = df["year"].astype("int16")

value_vars = [c for c in cols if c not in ("UNITID","year")]
wide = df.pivot_table(index="UNITID", columns="year", values=value_vars)
wide.columns = [f"{var}_{yr}" for var, yr in wide.columns]
wide = wide.reset_index()

wide.to_parquet(out_path, compression="snappy", index=False)
print(f"Wrote {out_path} with {wide.shape[0]} rows and {wide.shape[1]} columns")
