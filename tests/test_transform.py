from datetime import date, datetime, timezone

import pandas as pd
import pytest

from pipeline.transform import ARROW_SCHEMA, expected_rows, transform
from tests.conftest import DAY, make_raw


def test_transform_casts_types_and_converts_to_utc():
    rows = transform(make_raw(DAY), DAY)

    assert rows.schema.equals(ARROW_SCHEMA)
    assert rows.num_rows == 96
    first = rows.column("timestamp_utc")[0].as_py()
    # Local midnight in Berlin (CEST, UTC+2) is 22:00 UTC the day before.
    assert first == datetime(2026, 10, 19, 22, 0, tzinfo=timezone.utc)
    assert set(rows.column("delivery_day").to_pylist()) == {DAY}
    assert rows.column("bidding_zone")[0].as_py() == "DE-LU"


def test_transform_keeps_only_the_requested_local_day():
    # The ingest step asks the API for [day, day+1], so the next day leaks in.
    raw = pd.concat([make_raw(DAY), make_raw(date(2026, 10, 21))], ignore_index=True)

    rows = transform(raw, DAY)

    assert rows.num_rows == 96
    assert set(rows.column("delivery_day").to_pylist()) == {DAY}


def test_transform_rejects_raw_data_without_price_column():
    with pytest.raises(ValueError, match="missing required columns: \\['price'\\]"):
        transform(make_raw().drop(columns="price"), DAY)


def test_transform_keeps_duplicates_for_validation_to_catch():
    raw = make_raw()
    raw = pd.concat([raw, raw.head(3)], ignore_index=True)

    assert transform(raw, DAY).num_rows == 99


@pytest.mark.parametrize(
    ("day", "rows"),
    [(date(2026, 10, 20), 96), (date(2026, 3, 29), 92), (date(2026, 10, 25), 100)],
)
def test_expected_rows_accounts_for_daylight_saving_time(day, rows):
    assert expected_rows(day) == rows
