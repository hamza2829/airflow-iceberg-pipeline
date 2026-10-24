from datetime import date

import pyarrow as pa
import pytest

from pipeline.load import TABLE_ID, load_day
from pipeline.transform import transform
from tests.conftest import DAY, make_raw


def table_rows(catalog) -> pa.Table:
    return catalog.load_table(TABLE_ID).scan().to_arrow()


def test_loading_the_same_day_twice_does_not_duplicate_rows(catalog):
    rows = transform(make_raw(), DAY)

    load_day(rows, DAY, catalog)
    load_day(rows, DAY, catalog)

    assert table_rows(catalog).num_rows == 96


def test_reload_replaces_the_rows_of_that_day(catalog):
    load_day(transform(make_raw(price=50.0), DAY), DAY, catalog)
    load_day(transform(make_raw(price=90.0), DAY), DAY, catalog)

    prices = table_rows(catalog).column("price_eur_mwh").to_pylist()
    assert len(prices) == 96 and min(prices) == 90.0


def test_reload_of_one_day_keeps_other_days(catalog):
    other = date(2026, 10, 21)
    load_day(transform(make_raw(DAY), DAY), DAY, catalog)
    load_day(transform(make_raw(other), other), other, catalog)
    load_day(transform(make_raw(DAY), DAY), DAY, catalog)

    days = table_rows(catalog).column("delivery_day").to_pylist()
    assert days.count(DAY) == 96 and days.count(other) == 96


def test_table_is_partitioned_by_delivery_day(catalog):
    load_day(transform(make_raw(), DAY), DAY, catalog)

    spec = catalog.load_table(TABLE_ID).spec()
    assert [(f.name, str(f.transform)) for f in spec.fields] == [("delivery_day", "identity")]


def test_load_rejects_rows_of_another_day(catalog):
    rows = transform(make_raw(DAY), DAY)

    with pytest.raises(ValueError, match="contain other delivery days"):
        load_day(rows, date(2026, 10, 21), catalog)
