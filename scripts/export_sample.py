"""Refresh the fallback sample from a raw Parquet file written by the ingest step.

Usage: python scripts/export_sample.py data/raw/prices_DE-LU_<day>_api.parquet
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import config  # noqa: E402
from pipeline.transform import _to_local  # noqa: E402


def main(raw_path: str) -> None:
    raw = pd.read_parquet(raw_path)
    day = date.fromisoformat(Path(raw_path).stem.split("_")[2])
    sample = raw.loc[(_to_local(raw["unix_seconds"]).dt.date == day).to_numpy(), ["unix_seconds", "price", "unit"]]
    config.SAMPLE_CSV.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(config.SAMPLE_CSV, index=False)
    print(f"Wrote {len(sample)} rows for {day} to {config.SAMPLE_CSV}")


if __name__ == "__main__":
    main(sys.argv[1])
