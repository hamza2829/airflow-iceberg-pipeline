import json

import duckdb
import pandas as pd
import pytest

from pipeline.load import load_day
from pipeline.quality import (
    DataQualityError,
    check_not_null,
    check_price_range,
    check_row_count,
    check_unique_timestamps,
    run_checks,
    validate_day,
)
from pipeline.transform import transform
from tests.conftest import DAY, DST_DAY, make_raw


def relation(df: pd.DataFrame) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.register("prices", df)
    return con


def good_rows() -> pd.DataFrame:
    return transform(make_raw(), DAY).to_pandas()


def test_all_checks_pass_on_good_data():
    results = run_checks(relation(good_rows()), "prices", DAY)

    assert [r.name for r in results] == [
        "not_null_key_columns",
        "price_in_range",
        "unique_timestamps",
        "expected_row_count",
    ]
    assert all(r.passed for r in results)


def test_not_null_check_fails_on_null_price():
    df = good_rows()
    df.loc[5, "price_eur_mwh"] = None

    result = check_not_null(relation(df), "prices")

    assert not result.passed
    assert result.details == "null counts {'price_eur_mwh': 1}"


def test_not_null_check_fails_on_null_timestamp():
    df = good_rows()
    df["timestamp_utc"] = df["timestamp_utc"].astype("object")
    df.loc[0, "timestamp_utc"] = None

    assert not check_not_null(relation(df), "prices").passed


@pytest.mark.parametrize("bad_price", [-600.0, 9999.0])
def test_price_range_check_fails_outside_market_limits(bad_price):
    df = good_rows()
    df.loc[10, "price_eur_mwh"] = bad_price

    result = check_price_range(relation(df), "prices")

    assert not result.passed
    assert "1 prices outside [-500.0, 4000.0]" in result.details


def test_unique_timestamp_check_fails_on_duplicates():
    df = good_rows()
    df = pd.concat([df, df.iloc[[0, 1]]], ignore_index=True)

    result = check_unique_timestamps(relation(df), "prices")

    assert not result.passed
    assert result.details.startswith("2 duplicated timestamps, first: [('2026-10-19T22:00:00Z', 2)")


def test_row_count_check_fails_when_rows_are_missing():
    result = check_row_count(relation(good_rows().head(95)), "prices", DAY)

    assert not result.passed
    assert result.details == "95 rows for 2026-10-20, expected 96 (15-minute intervals)"


def test_row_count_check_expects_100_rows_on_dst_change_day():
    con = duckdb.connect()
    con.register("prices", transform(make_raw(DST_DAY), DST_DAY))

    assert check_row_count(con, "prices", DST_DAY).passed


def test_validate_day_writes_report_on_success(catalog, data_dir):
    load_day(transform(make_raw(), DAY), DAY, catalog)

    report = json.loads(validate_day(DAY, catalog, data_dir / "reports").read_text())

    assert report["passed"] is True and len(report["checks"]) == 4


def test_validate_day_fails_with_clear_message_and_report(catalog, data_dir):
    raw = make_raw()
    load_day(transform(pd.concat([raw, raw.head(1)], ignore_index=True), DAY), DAY, catalog)

    with pytest.raises(DataQualityError) as err:
        validate_day(DAY, catalog, data_dir / "reports")

    msg = str(err.value)
    assert msg.startswith("2 of 4 checks failed for 2026-10-20")
    assert "unique_timestamps: 1 duplicated timestamps" in msg
    assert "expected_row_count: 97 rows" in msg
    report = json.loads((data_dir / "reports" / "dq_report_2026-10-20.json").read_text())
    assert report["passed"] is False
