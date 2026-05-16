"""
Crypto ETL Pipeline
Pulls live data from CoinCap API (free, no key, no rate limits),
validates, transforms, and loads into SQLite with full audit trail.
"""

import requests
import sqlite3
import logging
import json
import time
import os
from datetime import datetime, timezone
from pathlib import Path

# ── Logging ──────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
handlers = [logging.StreamHandler()]
if not os.getenv("CI"):
    handlers.append(logging.FileHandler("logs/pipeline.log"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=handlers,
)
log = logging.getLogger(__name__)

DB_PATH = Path("data/crypto.db")
COINS = ["bitcoin", "ethereum", "solana", "cardano", "polkadot"]
COINCAP_URL = "https://api.coincap.io/v2/assets"


def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id       TEXT    NOT NULL,
            symbol        TEXT    NOT NULL,
            name          TEXT    NOT NULL,
            price_usd     REAL    NOT NULL,
            market_cap    REAL,
            volume_24h    REAL,
            change_24h    REAL,
            fetched_at    TEXT    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id         TEXT    NOT NULL UNIQUE,
            started_at     TEXT    NOT NULL,
            finished_at    TEXT,
            status         TEXT    NOT NULL DEFAULT 'running',
            rows_extracted INTEGER DEFAULT 0,
            rows_loaded    INTEGER DEFAULT 0,
            errors         TEXT
        );
    """)
    conn.commit()
    log.info("Database initialised")


def extract():
    log.info(f"[EXTRACT] Fetching {len(COINS)} coins from CoinCap")
    results = []
    for coin in COINS:
        try:
            resp = requests.get(
                f"{COINCAP_URL}/{coin}",
                timeout=15,
                headers={"Accept-Encoding": "gzip"},
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            if data:
                results.append(data)
                log.info(f"[EXTRACT] Got {coin}")
            time.sleep(0.2)  # gentle rate limiting
        except requests.RequestException as e:
            log.warning(f"[EXTRACT] Skipped {coin}: {e}")
    log.info(f"[EXTRACT] Total: {len(results)} records")
    return results


REQUIRED_FIELDS = ["id", "symbol", "name", "priceUsd"]


def validate(raw):
    log.info("[VALIDATE] Running data quality checks")
    clean = []
    rejected = 0
    for rec in raw:
        errors = []
        for field in REQUIRED_FIELDS:
            if rec.get(field) is None:
                errors.append(f"missing {field}")
        price = rec.get("priceUsd")
        try:
            if price is None or float(price) <= 0:
                if "missing priceUsd" not in errors:
                    errors.append("price <= 0")
        except (ValueError, TypeError):
            errors.append("invalid price format")
        if errors:
            log.warning(f"[VALIDATE] Rejected {rec.get('id', '?')}: {errors}")
            rejected += 1
            continue
        clean.append(rec)
    log.info(f"[VALIDATE] {len(clean)} valid, {rejected} rejected")
    if len(clean) == 0:
        raise ValueError("All records failed validation — aborting")
    return clean


def transform(raw, fetched_at):
    log.info("[TRANSFORM] Normalising records")
    result = []
    for rec in raw:
        try:
            price = float(rec.get("priceUsd") or 0)
            if price <= 0:
                continue
            row = {
                "coin_id": rec.get("id", ""),
                "symbol": (rec.get("symbol") or "").upper(),
                "name": rec.get("name", ""),
                "price_usd": price,
                "market_cap": float(rec.get("marketCapUsd") or 0),
                "volume_24h": float(rec.get("volumeUsd24Hr") or 0),
                "change_24h": round(float(rec.get("changePercent24Hr") or 0), 4),
                "fetched_at": fetched_at,
            }
            result.append(row)
        except (ValueError, TypeError) as e:
            log.warning(f"[TRANSFORM] Skipped {rec.get('id', '?')}: {e}")
    log.info(f"[TRANSFORM] {len(result)} rows ready")
    return result


def load(rows, conn):
    log.info(f"[LOAD] Writing {len(rows)} rows")
    conn.executemany(
        """INSERT INTO crypto_prices
           (coin_id, symbol, name, price_usd, market_cap,
            volume_24h, change_24h, fetched_at)
           VALUES
           (:coin_id, :symbol, :name, :price_usd, :market_cap,
            :volume_24h, :change_24h, :fetched_at)""",
        rows,
    )
    conn.commit()
    log.info(f"[LOAD] {len(rows)} rows committed")
    return len(rows)


def start_run(conn, run_id, started_at):
    conn.execute(
        "INSERT INTO pipeline_runs (run_id, started_at, status) VALUES (?,?,?)",
        (run_id, started_at, "running"),
    )
    conn.commit()


def finish_run(conn, run_id, status, rows_extracted, rows_loaded, error=None):
    conn.execute(
        """UPDATE pipeline_runs
           SET finished_at=?, status=?, rows_extracted=?, rows_loaded=?, errors=?
           WHERE run_id=?""",
        (
            datetime.now(timezone.utc).isoformat(),
            status, rows_extracted, rows_loaded,
            json.dumps(error) if error else None,
            run_id,
        ),
    )
    conn.commit()


def run_pipeline():
    run_id = f"run_{int(time.time())}"
    started_at = datetime.now(timezone.utc).isoformat()
    log.info("=" * 60)
    log.info(f"Pipeline starting — run_id={run_id}")
    log.info("=" * 60)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    start_run(conn, run_id, started_at)
    rows_extracted = 0
    rows_loaded = 0
    try:
        raw = extract()
        rows_extracted = len(raw)
        validated = validate(raw)
        rows = transform(validated, started_at)
        rows_loaded = load(rows, conn)
        finish_run(conn, run_id, "success", rows_extracted, rows_loaded)
        log.info(f"Pipeline complete — extracted={rows_extracted} loaded={rows_loaded}")
    except Exception as exc:
        log.error(f"Pipeline failed: {exc}", exc_info=True)
        finish_run(conn, run_id, "failed", rows_extracted, rows_loaded, str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
