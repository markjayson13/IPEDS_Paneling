"""Reset ic_ay_crosswalk_all.csv from the autofilled IC_AY crosswalk."""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[2]
DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks" / "Filled"
AUTOFILLED = CROSSWALK_DIR / "ic_ay_crosswalk_autofilled.csv"
ALL = CROSSWALK_DIR / "ic_ay_crosswalk_all.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset ic_ay_crosswalk_all.csv from ic_ay_crosswalk_autofilled.csv."
    )
    parser.add_argument("--force", action="store_true", help="Overwrite ic_ay_crosswalk_all.csv if it exists.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not AUTOFILLED.exists():
        raise SystemExit(f"Autofilled IC_AY crosswalk not found: {AUTOFILLED}")
    if ALL.exists() and not args.force:
        raise SystemExit(f"{ALL} already exists. Use --force to overwrite.")
    ALL.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(AUTOFILLED, ALL)
    print(f"Copied {AUTOFILLED} -> {ALL}")


if __name__ == "__main__":
    main()
