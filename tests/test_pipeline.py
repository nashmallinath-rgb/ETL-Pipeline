"""Tests for the crypto ETL pipeline."""
import sqlite3
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from pipeline import validate, transform, init_db, load  # noqa: E402

MOCK_RAW = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 67000.5,
        "market_cap": 1_300_000_000_000,
        "total_volume": 25_000_000_000,
        "price_change_percentage_24h": 1.23,
        "ath": 73750.0,
        "ath_change_percentage": -9.15,
        "circulating_supply": 19_700_000,
    },
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": 3500.0,
        "market_cap": 420_000_000_000,
        "total_volume": 15_000_000_000,
        "price_change_percentage_24h": -0.5,
        "ath": 4878.0,
        "ath_change_percentage": -28.3,
        "circulating_supply": 120_000_000,
    },
]


class TestValidate:
    def test_valid_records_pass(self):
        result = validate(MOCK_RAW)
        assert len(result) == 2

    def test_rejects_missing_price(self):
        bad = [{
            "id": "bad", "symbol": "b", "name": "Bad",
            "current_price": None, "market_cap": 100
        }]
        result = validate(bad)
        assert len(result) == 0

    def test_rejects_zero_price(self):
        bad = [{**MOCK_RAW[0], "current_price": 0}]
        result = validate(bad)
        assert len(result) == 0

    def test_raises_when_all_rejected(self):
        with pytest.raises(ValueError, match="All records failed"):
            validate([{
                "id": "x", "symbol": None, "name": None,
                "current_price": None, "market_cap": None
            }])


class TestTransform:
    def test_output_keys(self):
        rows = transform(MOCK_RAW, "2024-01-01T00:00:00+00:00")
        expected = {
            "coin_id", "symbol", "name", "price_usd", "market_cap",
            "volume_24h", "change_24h", "ath", "ath_pct",
            "circulating", "fetched_at"
        }
        assert expected.issubset(set(rows[0].keys()))

    def test_symbol_uppercased(self):
        rows = transform(MOCK_RAW, "2024-01-01T00:00:00+00:00")
        assert all(r["symbol"].isupper() for r in rows)

    def test_price_is_float(self):
        rows = transform(MOCK_RAW, "2024-01-01T00:00:00+00:00")
        assert isinstance(rows[0]["price_usd"], float)

    def test_row_count(self):
        rows = transform(MOCK_RAW, "2024-01-01T00:00:00+00:00")
        assert len(rows) == 2


class TestDatabase:
    def test_init_creates_tables(self):
        conn = sqlite3.connect(":memory:")
        init_db(conn)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        names = {r[0] for r in tables}
        assert "crypto_prices" in names
        assert "pipeline_runs" in names
        conn.close()

    def test_load_inserts_rows(self):
        conn = sqlite3.connect(":memory:")
        init_db(conn)
        rows = transform(MOCK_RAW, "2024-01-01T00:00:00+00:00")
        count = load(rows, conn)
        assert count == 2
        db_count = conn.execute(
            "SELECT COUNT(*) FROM crypto_prices"
        ).fetchone()[0]
        assert db_count == 2
        conn.close()
