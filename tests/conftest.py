"""Shared fixtures. All test data here is synthetic and generated in code."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pipeline.load import get_catalog
from pipeline.transform import expected_rows

DAY = date(2026, 10, 20)
DST_DAY = date(2026, 10, 25)  # clocks go back: 25 local hours


def make_raw(day: date = DAY, n: int | None = None, price: float = 80.0) -> pd.DataFrame:
    """Raw frame shaped like the Energy-Charts response: one row per 15 minutes."""
    start = int(pd.Timestamp(day).tz_localize("Europe/Berlin").timestamp())
    n = expected_rows(day) if n is None else n
    return pd.DataFrame(
        {
            "unix_seconds": [start + 900 * i for i in range(n)],
            "price": [price + i % 7 for i in range(n)],
            "unit": "EUR / MWh",
        }
    )


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def catalog(data_dir):
    return get_catalog(data_dir / "warehouse")
