"""Stabilize IPEDS HD/IC variables into a master panel."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd


DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
DEFAULT_FILLED_CROSSWALK_DIR = DEFAULT_CROSSWALK_DIR / "Filled"
DEFAULT_CROSSWALK_PATH = DEFAULT_FILLED_CROSSWALK_DIR / "hd_crosswalk.csv"
DEFAULT_WIDE_DIR = DATA_ROOT / "Parquets" / "Unify" / "HDICwide"
DEFAULT_OUTPUT_PATH = DEFAULT_WIDE_DIR / "hd_master_panel.parquet"
DEFAULT_RAW_PANEL_PATH = DATA_ROOT / "Parquets" / "panel_long_hd_ic.parquet"

SURVEY_SYNONYMS = {
    "INSTITUTIONALCHARACTERISTICS": "HD",
    "INSTITUTIONALCHARACTERISTICSIC": "IC",
    "INSTITUTIONALCHARACTERISTICSIC_A": "IC",
}

EVER_TRUE_COLS = ["STABLE_HBCU", "STABLE_TRIBAL"]
GAP_FILL_COLS = [
    "STABLE_CONTROL",
    "STABLE_SECTOR",
    "STABLE_STFIPS",
    "STABLE_INSTITUTION_NAME",
]
CARNEGIE_COLS = [
    "CARNEGIE_2005",
    "CARNEGIE_2010",
    "CARNEGIE_2015",
    "CARNEGIE_2018",
    "CARNEGIE_2021",
]


def _normalize_survey_label(label: str) -> str:
    cleaned = label.strip().upper().replace(" ", "")
    return SURVEY_SYNONYMS.get(cleaned, cleaned)


def _first_non_empty(series: pd.Series):
    for val in series:
        if pd.notna(val) and str(val).strip():
            return val
    return series.iloc[0] if not series.empty else pd.NA


def _prepare_crosswalk_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    required = {"concept_key", "survey", "source_var", "year_start", "year_end"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Crosswalk missing required columns: {sorted(missing)}")

    df["survey"] = df["survey"].astype(str).map(_normalize_survey_label)
    df["source_var"] = df["source_var"].astype(str).str.upper()
    df["concept_key"] = df["concept_key"].astype(str)
    df["year_start"] = pd.to_numeric(df["year_start"], errors="coerce").astype("Int64")
    df["year_end"] = pd.to_numeric(df["year_end"], errors="coerce").astype("Int64")
    if df[["year_start", "year_end"]].isna().any().any():
        raise ValueError("Crosswalk contains non-numeric year ranges.")
    implausible = (df["year_start"] < 1900) | (df["year_end"] > 2100)
    if implausible.any():
        bad_rows = df.loc[implausible, ["concept_key", "survey", "source_var", "year_start", "year_end"]]
        raise ValueError(
            "Crosswalk contains implausible year ranges (outside 1900–2100):\n"
            f"{bad_rows.to_string(index=False)}"
        )
    bad_range = df["year_start"] > df["year_end"]
    if bad_range.any():
        bad_rows = df.loc[bad_range, ["concept_key", "survey", "source_var", "year_start", "year_end"]]
        raise ValueError(
            "Crosswalk has year_start > year_end for these rows:\n"
            f"{bad_rows.to_string(index=False)}"
        )
    agg = {"year_start": "min", "year_end": "max"}
    if "concept_key" in df.columns:
        agg["concept_key"] = "first"
    if "varlab" in df.columns:
        agg["varlab"] = _first_non_empty
    if "notes" in df.columns:
        agg["notes"] = _first_non_empty
    df = (
        df.groupby(["survey", "source_var"], as_index=False)
        .agg(agg)
        .sort_values(["survey", "source_var", "year_start"])
        .reset_index(drop=True)
    )
    return df


def _read_crosswalk(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Crosswalk file not found: {path}")
    df = pd.read_csv(path)
    return _prepare_crosswalk_df(df)


def _expand_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    records: List[dict] = []
    for _, row in df.iterrows():
        for year in range(int(row["year_start"]), int(row["year_end"]) + 1):
            records.append(
                {
                    "concept_key": row["concept_key"],
                    "survey": row["survey"],
                    "year": year,
                    "varname": row["source_var"],
                }
            )
    if not records:
        raise ValueError("Crosswalk expansion produced no rows.")
    expanded = pd.DataFrame.from_records(records)
    dup_mask = expanded.duplicated(subset=["survey", "year", "varname"], keep=False)
    if dup_mask.any():
        dup_rows = expanded.loc[dup_mask, ["concept_key", "survey", "year", "varname"]]
        raise ValueError(
            "Crosswalk expansion produced non-unique (survey, year, varname) combinations. "
            "Each raw variable-year-survey must map to at most one concept_key:\n"
            f"{dup_rows.head().to_string(index=False)}"
        )
    return expanded


def _prepare_raw_panel_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    if "varname" not in df.columns and "source_var" in df.columns:
        df["varname"] = df["source_var"]
    required = {"unitid", "year", "survey", "varname", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input panel missing required columns: {sorted(missing)}")

    df["unitid"] = pd.to_numeric(df["unitid"], errors="raise")
    df["year"] = pd.to_numeric(df["year"], errors="raise")
    if df["unitid"].isna().any() or df["year"].isna().any():
        raise ValueError("Input panel has missing unitid or year values.")
    df["survey"] = df["survey"].astype(str).str.upper()
    df["varname"] = df["varname"].astype(str).str.upper()
    return df


def _read_raw_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input panel not found: {path}")
    df = pd.read_parquet(path)
    return _prepare_raw_panel_df(df)


def _pivot_wide(merged: pd.DataFrame) -> pd.DataFrame:
    wide = (
        merged.pivot_table(
            index=["unitid", "year"],
            columns="concept_key",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    if isinstance(wide.columns, pd.MultiIndex):
        wide.columns = ["_".join(filter(None, map(str, col))).rstrip("_") for col in wide.columns]
    else:
        wide.columns = wide.columns.astype(str)

    return wide.reset_index(drop=True)


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    skip_tokens = ("name", "instnm", "abbr", "stfips")
    for col in df.columns:
        if col in {"unitid", "year"}:
            continue
        lower = col.lower()
        if any(token in lower for token in skip_tokens):
            continue
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().any() and converted.isna().sum() == df[col].isna().sum():
            df[col] = converted
    return df


def _normalize_binary_flag(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    numeric = pd.to_numeric(df[col], errors="coerce")
    mask_yes = numeric == 1
    mask_no = numeric.isin({0, 2})
    mask_missing = numeric.isin({-3, -2, -1})
    unexpected = numeric[~(mask_yes | mask_no | mask_missing | numeric.isna())].unique()
    if len(unexpected):
        raise ValueError(
            f"{col} contains unexpected codes {sorted(map(float, unexpected))}. "
            "Expected only {1, 0, 2, NaN} (1=yes, 0/2=no)."
        )
    normalized = pd.Series(np.nan, index=df.index, dtype="float64")
    normalized[mask_yes] = 1.0
    normalized[mask_no] = 0.0
    normalized[mask_missing] = np.nan
    df[col] = normalized


def _propagate_ever_true(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return

    def transform(series: pd.Series) -> float:
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.isna().all():
            return np.nan
        return float(numeric.fillna(0).max())

    df[col] = df.groupby("unitid")[col].transform(transform)


def _propagate_gap_fill(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    df[col] = df.groupby("unitid")[col].ffill().bfill()


def _propagate_latest_name(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return

    def latest(series: pd.Series):
        non_null = series.dropna()
        if non_null.empty:
            return np.nan
        return non_null.iloc[-1]

    df[col] = df.groupby("unitid")[col].transform(latest)


def _propagate_carnegie(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    df[col] = df.groupby("unitid")[col].ffill().bfill()


def _derive_parent_child_status(df: pd.DataFrame) -> pd.Series:
    status = pd.Series(pd.NA, index=df.index, dtype="Int64")
    child_mask = pd.Series(False, index=df.index)
    if "CAMPUSID" in df.columns:
        campus_series = df["CAMPUSID"]
        campus_str = campus_series.astype(str).str.strip()
        child_mask |= campus_series.notna() & campus_str.ne("") & campus_str.ne("nan")
    if "PCACT" in df.columns:
        child_mask |= df["PCACT"].notna()
    status[child_mask] = 3
    status.loc[status.isna()] = 1
    return status


def stabilize_hd(input_path: Path, crosswalk_path: Path, output_path: Path) -> pd.DataFrame:
    crosswalk = _read_crosswalk(crosswalk_path)
    expanded = _expand_crosswalk(crosswalk)

    raw = _read_raw_panel(input_path)
    surveys = expanded["survey"].unique().tolist()
    raw = raw[raw["survey"].isin(surveys)]

    merged = raw.merge(expanded, on=["year", "survey", "varname"], how="inner")
    if merged.empty:
        raise ValueError("Merged HD/IC data is empty. Check crosswalk and input panel.")
    merged = merged.sort_values(["unitid", "year"]).drop_duplicates(
        subset=["unitid", "year", "concept_key"], keep="first"
    )
    dup_mask = merged.duplicated(subset=["unitid", "year", "concept_key"], keep=False)
    if dup_mask.any():
        dup_rows = merged.loc[dup_mask, ["unitid", "year", "concept_key", "survey", "varname"]]
        raise ValueError(
            "Found duplicate (unitid, year, concept_key) rows before pivot. "
            "Check for raw panel duplicates or crosswalk overlaps:\n"
            f"{dup_rows.head(10).to_string(index=False)}"
        )

    base_pairs = raw[["unitid", "year"]].drop_duplicates()
    wide_mapped = _pivot_wide(merged)
    if wide_mapped.duplicated(subset=["unitid", "year"]).any():
        dup = (
            wide_mapped.loc[
                wide_mapped.duplicated(subset=["unitid", "year"], keep=False), ["unitid", "year"]
            ]
            .drop_duplicates()
            .head()
        )
        raise ValueError(
            "Post-pivot panel has duplicate (unitid, year) rows, which should be impossible.\n"
            f"Example duplicates:\n{dup.to_string(index=False)}"
        )
    wide = base_pairs.merge(wide_mapped, on=["unitid", "year"], how="left")
    wide = wide.sort_values(["unitid", "year"]).reset_index(drop=True)
    wide = _coerce_types(wide)

    for col in EVER_TRUE_COLS:
        _normalize_binary_flag(wide, col)

    for col in EVER_TRUE_COLS:
        _propagate_ever_true(wide, col)
        if col in wide.columns and not wide[col].isna().all():
            numeric = pd.to_numeric(wide[col], errors="coerce")
            wide[col] = pd.Series(pd.array(numeric.round(), dtype="Int64"), index=wide.index)

    for col in GAP_FILL_COLS:
        _propagate_gap_fill(wide, col)

    _propagate_latest_name(wide, "STABLE_INSTITUTION_NAME")

    for col in CARNEGIE_COLS:
        _propagate_carnegie(wide, col)
        if col in wide.columns:
            non_null = wide[col].notna().sum()
            total = len(wide)
            print(f"{col}: {non_null:,} non-missing values out of {total:,} rows")

    wide["unitid"] = pd.to_numeric(wide["unitid"], errors="raise").astype("int64")
    wide["year"] = pd.to_numeric(wide["year"], errors="raise").astype("int64")
    wide["STABLE_PRNTCHLD_STATUS"] = _derive_parent_child_status(wide)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(output_path, index=False)

    print(f"Wrote HD master panel to {output_path}")
    print(f"Shape: {wide.shape[0]:,} rows x {wide.shape[1]:,} columns")
    return wide


def _run_smoke_test() -> None:
    # Binary guard should reject unexpected codes.
    try:
        df = pd.DataFrame({"STABLE_HBCU": [0, 1, 2, 3]})
        _normalize_binary_flag(df, "STABLE_HBCU")
    except ValueError:
        print("Binary flag normalization guard triggered as expected.")
    else:
        raise AssertionError("Expected _normalize_binary_flag to fail on invalid codes.")

    crosswalk = pd.DataFrame(
        {
            "concept_key": [
                "STABLE_INSTITUTION_NAME",
                "STABLE_CONTROL",
                "STABLE_SECTOR",
                "STABLE_HBCU",
                "STABLE_TRIBAL",
                "STABLE_STFIPS",
                "CARNEGIE_2015",
            ],
            "survey": ["HD"] * 7,
            "source_var": [
                "INSTNM",
                "CONTROL",
                "SECTOR",
                "HBCU",
                "TRIBAL",
                "STABBR",
                "CARNEGIE",
            ],
            "year_start": [2018] * 7,
            "year_end": [2020] * 7,
        }
    )
    crosswalk_prepped = _prepare_crosswalk_df(crosswalk)
    expanded = _expand_crosswalk(crosswalk_prepped)

    records = []

    def add(unit: int, year: int, varname: str, value) -> None:
        records.append(
            {
                "unitid": unit,
                "year": year,
                "survey": "HD",
                "varname": varname,
                "value": value,
            }
        )

    unit1_data = {
        2018: {"INSTNM": "Alpha College", "CONTROL": 1, "SECTOR": 1, "HBCU": 0, "TRIBAL": 0, "STABBR": "AL", "CARNEGIE": 15},
        2019: {"INSTNM": None, "CONTROL": 1, "SECTOR": 1, "HBCU": 1, "TRIBAL": 1, "STABBR": None, "CARNEGIE": None},
        2020: {"INSTNM": "Alpha College University", "CONTROL": 1, "SECTOR": 1, "HBCU": None, "TRIBAL": 0, "STABBR": "AL", "CARNEGIE": None},
    }

    unit2_data = {
        2018: {"INSTNM": "Beta Institute", "CONTROL": 2, "SECTOR": 2, "HBCU": 0, "TRIBAL": 0, "STABBR": "TX", "CARNEGIE": 18},
        2019: {"INSTNM": "Beta Institute", "CONTROL": 3, "SECTOR": 3, "HBCU": 0, "TRIBAL": 0, "STABBR": "TX", "CARNEGIE": 18},
        2020: {"INSTNM": "Beta Institute", "CONTROL": None, "SECTOR": 3, "HBCU": 0, "TRIBAL": 0, "STABBR": None, "CARNEGIE": None},
    }

    unit3_data = {
        2018: {"INSTNM": "Gamma College", "CONTROL": 1, "SECTOR": 1, "HBCU": 2, "TRIBAL": 2, "STABBR": "CA", "CARNEGIE": 12},
        2019: {"INSTNM": "Gamma College", "CONTROL": 1, "SECTOR": 1, "HBCU": 2, "TRIBAL": 2, "STABBR": "CA", "CARNEGIE": None},
        2020: {"INSTNM": "Gamma College", "CONTROL": 1, "SECTOR": 1, "HBCU": 2, "TRIBAL": 2, "STABBR": "CA", "CARNEGIE": None},
    }

    for year, vars_map in unit1_data.items():
        for var, value in vars_map.items():
            add(1001, year, var, value)
    for year, vars_map in unit2_data.items():
        for var, value in vars_map.items():
            add(2002, year, var, value)
    for year, vars_map in unit3_data.items():
        for var, value in vars_map.items():
            add(3003, year, var, value)

    raw_df = pd.DataFrame(records)
    raw = _prepare_raw_panel_df(raw_df)
    merged = raw.merge(expanded, on=["year", "survey", "varname"], how="inner")
    wide = _pivot_wide(merged)
    if wide.duplicated(subset=["unitid", "year"]).any():
        raise AssertionError("Smoke test pivot unexpectedly produced duplicates.")
    wide = wide.sort_values(["unitid", "year"]).reset_index(drop=True)
    wide = _coerce_types(wide)

    for col in EVER_TRUE_COLS:
        _normalize_binary_flag(wide, col)
        _propagate_ever_true(wide, col)
    wide = wide.sort_values(["unitid", "year"]).reset_index(drop=True)
    for col in GAP_FILL_COLS:
        _propagate_gap_fill(wide, col)
    _propagate_latest_name(wide, "STABLE_INSTITUTION_NAME")
    for col in CARNEGIE_COLS:
        _propagate_carnegie(wide, col)

    unit1_hbcu = wide.loc[wide["unitid"] == 1001, "STABLE_HBCU"].unique()
    assert len(unit1_hbcu) == 1 and float(unit1_hbcu[0]) == 1.0, "HBCU ever-true failed to lock at 1."

    unit3_hbcu = wide.loc[wide["unitid"] == 3003, "STABLE_HBCU"].unique()
    assert len(unit3_hbcu) == 1 and float(unit3_hbcu[0]) == 0.0, "HBCU code 2 should normalize to 0."

    unit1_tribal = wide.loc[wide["unitid"] == 1001, "STABLE_TRIBAL"].unique()
    assert len(unit1_tribal) == 1 and float(unit1_tribal[0]) == 1.0, "Tribal flag did not propagate."

    stfips = wide.loc[wide["unitid"] == 1001, "STABLE_STFIPS"].unique()
    assert len(stfips) == 1 and stfips[0] == "AL", "STABLE_STFIPS gap fill failed."

    latest_name = wide.loc[wide["unitid"] == 1001, "STABLE_INSTITUTION_NAME"].unique()
    assert len(latest_name) == 1 and latest_name[0] == "Alpha College University", "Latest name propagation failed."

    unit2_control = wide.loc[wide["unitid"] == 2002, "STABLE_CONTROL"].tolist()
    assert unit2_control == [2, 3, 3], "Control change should persist."

    unit1_carn = wide.loc[wide["unitid"] == 1001, "CARNEGIE_2015"].tolist()
    assert all(val == 15 for val in unit1_carn), "Carnegie propagation failed."

    print("Smoke test passed: propagation logic behaves as expected.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_RAW_PANEL_PATH,
        help="Path to long-form raw HD/IC panel (panel_long_hd_ic.parquet).",
    )
    parser.add_argument(
        "--crosswalk",
        type=Path,
        default=DEFAULT_CROSSWALK_PATH,
        help="Path to hd_crosswalk.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output parquet path for the HD master panel (wide).",
    )
    parser.add_argument(
        "--run-smoke-test",
        action="store_true",
        help="Run an in-memory smoke test instead of processing files.",
    )

    args = parser.parse_args()

    if args.run_smoke_test:
        _run_smoke_test()
        return

    if args.input is None:
        parser.error("--input is required unless --run-smoke-test is provided.")

    stabilize_hd(args.input, args.crosswalk, args.output)


if __name__ == "__main__":
    main()
