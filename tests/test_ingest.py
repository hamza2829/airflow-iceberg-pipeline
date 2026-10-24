import logging

import pandas as pd
import requests

from pipeline import config, ingest
from tests.conftest import DAY, make_raw


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def test_ingest_saves_api_response_as_parquet(data_dir, monkeypatch):
    raw = make_raw()
    payload = {"unix_seconds": raw["unix_seconds"].tolist(), "price": raw["price"].tolist(), "unit": "EUR / MWh"}
    calls = []
    monkeypatch.setattr(ingest.requests, "get", lambda url, params, timeout: calls.append(params) or FakeResponse(payload))

    meta = ingest.ingest_day(DAY)

    assert calls == [{"bzn": "DE-LU", "start": "2026-10-20", "end": "2026-10-21"}]
    assert meta["source"] == "api" and meta["delivery_day"] == "2026-10-20"
    saved = pd.read_parquet(meta["raw_path"])
    assert len(saved) == 96 and set(saved["source"]) == {"api"}


def test_ingest_falls_back_to_sample_and_logs_it(data_dir, monkeypatch, caplog, tmp_path):
    sample = tmp_path / "sample.csv"
    make_raw().to_csv(sample, index=False)
    monkeypatch.setattr(config, "SAMPLE_CSV", sample)

    def unreachable(*args, **kwargs):
        raise requests.ConnectionError("network is down")

    monkeypatch.setattr(ingest.requests, "get", unreachable)

    with caplog.at_level(logging.WARNING, logger="pipeline.ingest"):
        meta = ingest.ingest_day(DAY)

    assert meta["source"] == "sample"
    assert "FALLBACK USED" in caplog.text and "network is down" in caplog.text
    assert set(pd.read_parquet(meta["raw_path"])["source"]) == {"sample"}
