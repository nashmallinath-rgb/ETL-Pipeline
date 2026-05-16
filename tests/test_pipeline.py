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
        "symbol": "BTC",
        "name": "Bitcoin",
        "priceUsd": "67000.50",
        "marketCapUsd": "1300000000000",
        "volumeUsd24Hr": "25000000000",
        "changePercent24Hr": "1.23",
    },
    {
        "id": "ethereum",
        "symbol": "ETH",
        "name": "Ethereum",
        "priceUsd": "3500.00",
        "marketCapUsd": "420000000000",
        "volumeUsd24Hr": "15000000000",
        "changePercent24Hr": "-0.5",
    },
]


class TestValidate:
    def test_valid_records_pass(self):
        result = validate(MOCK_RAW)
        assert len(result) == 2

    def test_rejects_missing_price(self):
        bad = {"id": "bad", "symbol": "B", "name": "Bad", "priceUsd": None}
        result = validate([bad, MOCK_RAW[1]])
        assert len(result) == 1
        assert result[0]["id"] == "ethereum"

    def test_rejects_zero_price(self):
        bad = {**MOCK_RAW[0], "priceUsd": "0"}
        result = validate([bad, MOCK_RAW[1]])
        assert len(result) == 1

    def test_raises_when_all_rejected(self):
        with pytest.raises(ValueError, match="All records failed"):
            validate([{"id": "x", "symbol": None, "name": None, "priceUsd": None}])


class TestTransform:
    def test_output_keys(self):
        rows = transform(MOCK_RAW, "2024-01-01T00:00:00+00:00")
        expected = {
            "coin_id", "symbol", "name", "price_usd",
            "market_cap", "volume_24h", "change_24h", "fetched_at"
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
