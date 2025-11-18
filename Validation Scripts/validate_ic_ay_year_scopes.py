from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
XWALK_PATH = DATA_ROOT / "Paneled Datasets" / "Crosswalks" / "Filled" / "ic_ay_crosswalk_all.csv"
OUT_DIR = DATA_ROOT / "Parquets" / "Validation"

YEAR_RANGE_REGEX = re.compile(r"(20\d{2})\s*[-–]\s*(\d{2})")


def extract_label_year_range(label: str) -> Tuple[Optional[int], Optional[int]]:
    if not label:
        return (None, None)
    match = YEAR_RANGE_REGEX.search(label)
    if not match:
        return (None, None)
    start_full = int(match.group(1))
    end_suffix = int(match.group(2))
    if end_suffix < 100:
        century = start_full // 100 * 100
        end_full = century + end_suffix
        if end_full < start_full:
            end_full += 100
    else:
        end_full = end_suffix
    return (start_full, end_full)


def load_crosswalk(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"IC_AY crosswalk not found: {path}")
    df = pd.read_csv(path, dtype=str).fillna("")
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"survey", "source_var", "concept_key", "year_start", "year_end", "label"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Crosswalk missing columns: {sorted(missing)}")
    df["concept_key"] = df["concept_key"].astype(str).str.strip()
    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    df["year_start"] = pd.to_numeric(df["year_start"], errors="coerce").astype("Int64")
    df["year_end"] = pd.to_numeric(df["year_end"], errors="coerce").astype("Int64")
    df["label"] = df["label"].astype(str)

    return df


def add_label_year_info(df: pd.DataFrame) -> pd.DataFrame:
    label_year_start = []
    label_year_end = []
    for label in df["label"]:
        ls, le = extract_label_year_range(label)
        label_year_start.append(ls)
        label_year_end.append(le)
    df = df.copy()
    df["label_year_start"] = label_year_start
    df["label_year_end"] = label_year_end
    return df


def compute_concept_coverage(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for concept_key, sub in df.groupby("concept_key", dropna=False):
        ys = sub["year_start"].dropna().astype(int)
        ye = sub["year_end"].dropna().astype(int)
        if ys.empty or ye.empty:
            min_year = None
            max_year = None
        else:
            min_year = ys.min()
            max_year = ye.max()
        lstart = sub["label_year_start"].dropna()
        lend = sub["label_year_end"].dropna()
        label_year_min = int(lstart.min()) if not lstart.empty else None
        label_year_max = int(lend.max()) if not lend.empty else None

        rows.append(
            {
                "concept_key": concept_key,
                "n_rows": len(sub),
                "min_year": min_year,
                "max_year": max_year,
                "label_year_min": label_year_min,
                "label_year_max": label_year_max,
            }
        )
    cov = pd.DataFrame(rows)
    return cov.sort_values(["concept_key"]).reset_index(drop=True)


def compute_coverage_gaps(df: pd.DataFrame, cov: pd.DataFrame) -> pd.DataFrame:
    years = pd.concat([df["year_start"], df["year_end"]]).dropna().astype(int)
    if years.empty:
        global_min = global_max = None
        full_set: set[int] = set()
    else:
        global_min = int(years.min())
        global_max = int(years.max())
        full_set = set(range(global_min, global_max + 1))

    gap_rows = []
    for _, row in cov.iterrows():
        concept = row["concept_key"]
        sub = df[df["concept_key"] == concept]
        covered_years: set[int] = set()
        for _, r in sub.iterrows():
            ys = r["year_start"]
            ye = r["year_end"]
            if pd.isna(ys) or pd.isna(ye):
                continue
            for year in range(int(ys), int(ye) + 1):
                covered_years.add(year)
        if not covered_years:
            missing_years = sorted(full_set)
            coverage_min = row["min_year"]
            coverage_max = row["max_year"]
        else:
            coverage_min = min(covered_years)
            coverage_max = max(covered_years)
            expected = set(range(coverage_min, coverage_max + 1))
            missing_years = sorted(expected - covered_years)
        gap_rows.append(
            {
                "concept_key": concept,
                "coverage_min": coverage_min,
                "coverage_max": coverage_max,
                "missing_years_between_min_max": ",".join(str(y) for y in missing_years),
            }
        )
    gaps = pd.DataFrame(gap_rows)
    return gaps.sort_values(["concept_key"]).reset_index(drop=True)


def compute_row_mismatch(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        ys = r["year_start"]
        ye = r["year_end"]
        lys = r["label_year_start"]
        lye = r["label_year_end"]
        mismatch = False
        reason = ""
        if lys is not None and not pd.isna(lys) and ys is not pd.NA and not pd.isna(ys):
            if lys < ys:
                mismatch = True
                reason = "label_year_start < year_start"
        if lye is not None and not pd.isna(lye) and ye is not pd.NA and not pd.isna(ye):
            if lye < ys:
                mismatch = True
                reason = "label_year_end < year_start" if not reason else f"{reason}; label_year_end < year_start"
            elif lye > ye + 5:
                mismatch = True
                reason = "label_year_end >> year_end" if not reason else f"{reason}; label_year_end >> year_end"
        rows.append(
            {
                "survey": r["survey"],
                "source_var": r["source_var"],
                "concept_key": r["concept_key"],
                "year_start": ys,
                "year_end": ye,
                "label": r["label"],
                "label_year_start": lys,
                "label_year_end": lye,
                "year_scope_mismatch": mismatch,
                "mismatch_reason": reason,
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate IC_AY crosswalk year scopes and label-year hints.")
    parser.add_argument("--crosswalk", type=Path, default=XWALK_PATH)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    df = load_crosswalk(args.crosswalk)
    df = add_label_year_info(df)

    cov = compute_concept_coverage(df)
    gaps = compute_coverage_gaps(df, cov)
    row_mismatch = compute_row_mismatch(df)

    cov_path = args.out_dir / "ic_ay_concept_year_coverage.csv"
    gaps_path = args.out_dir / "ic_ay_concept_year_gaps.csv"
    rows_path = args.out_dir / "ic_ay_row_label_year_mismatch.csv"

    cov.to_csv(cov_path, index=False)
    gaps.to_csv(gaps_path, index=False)
    row_mismatch.to_csv(rows_path, index=False)

    print(f"Wrote concept coverage summary to {cov_path}")
    print(f"Wrote concept coverage gaps to {gaps_path}")
    print(f"Wrote row-level label/year mismatch report to {rows_path}")

    bad = row_mismatch[row_mismatch["year_scope_mismatch"]]
    print("\n=== Rows with label/year scope mismatch ===")
    print(f"Count: {bad.shape[0]}")
    if not bad.empty:
        display_cols = [
            "survey",
            "source_var",
            "concept_key",
            "year_start",
            "year_end",
            "label_year_start",
            "label_year_end",
            "mismatch_reason",
        ]
        print(bad[display_cols].head(40).to_string(index=False))


if __name__ == "__main__":
    main()
