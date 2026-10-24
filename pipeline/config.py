"""Central configuration. Every path can be moved with the PIPELINE_DATA_DIR env var."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

API_URL = "https://api.energy-charts.info/price"
BIDDING_ZONE = "DE-LU"
LOCAL_TZ = "Europe/Berlin"
# The SDAC day-ahead auction switched to a 15-minute market time unit on 2025-10-01.
INTERVAL_MINUTES = 15
# Harmonised SDAC clearing price limits in EUR/MWh.
PRICE_MIN = -500.0
PRICE_MAX = 4000.0

SAMPLE_CSV = REPO_ROOT / "data" / "sample" / "de_lu_day_ahead_sample.csv"

NAMESPACE = "energy"
TABLE_NAME = "day_ahead_prices"


@dataclass(frozen=True)
class Paths:
    data_dir: Path

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def warehouse_dir(self) -> Path:
        return self.data_dir / "warehouse"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"


def get_paths() -> Paths:
    return Paths(Path(os.environ.get("PIPELINE_DATA_DIR", REPO_ROOT / "data")).resolve())
