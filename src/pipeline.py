"""
Crypto ETL Pipeline
Pulls live data from CoinGecko (no API key needed),
validates, transforms, and loads into SQLite with full audit trail.
"""

import requests
import pandas as pd
import sqlite3
import logging
import json
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log"),
    ],
)
log = logging.getLogger(__name__)

DB_PATH = Path("data/crypto.db")
COINS = ["bitcoin", "ethereum", "solana", "cardano", "polkadot"]
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


# ── Database setup ────────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection) -> None:
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
            ath           REAL,
            ath_pct       REAL,
            circulating   REAL,
            fetched_at    TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id        TEXT    NOT NULL UNIQUE,
            started_at    TEXT    NOT NULL,
            finished_at   TEXT,
            status        TEXT    NOT NULL DEFAULT 'running',
            rows_extracted INTEGER DEFAULT 0,
            rows_loaded    INTEGER DEFAULT 0,
            errors         TEXT
        );
    """)
    conn.commit()
    log.info("Database initialised")


# ── EXTRACT ───────────────────────────────────────────────────────────────────
def extract(run_id: str) -> list[dict]:
    log.info(f"[EXTRACT] Fetching {len(COINS)} coins from CoinGecko")
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": len(COINS),
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "24h",
    }
    try:
        resp = requests.get(COINGECKO_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        log.info(f"[EXTRACT] ✓ Got {len(data)} records")
        return data
    except requests.RequestException as e:
        log.error(f"[EXTRACT] ✗ Failed: {e}")
        raise


# ── VALIDATE ──────────────────────────────────────────────────────────────────
REQUIRED_FIELDS = ["id", "symbol", "name", "current_price", "market_cap"]


def validate(raw: list[dict]) -> list[dict]:
    log.info("[VALIDATE] Running data quality checks")
    clean = []
    rejected = 0
    for rec in raw:
        errors = []
        for field in REQUIRED_FIELDS:
            if rec.get(field) is None:
                errors.append(f"missing {field}")
        if rec.get("current_price", 0) <= 0:
            errors.append("price <= 0")
        if errors:
            log.warning(f"[VALIDATE] ✗ Rejected {rec.get('id','?')}: {errors}")
            rejected += 1
            continue
        clean.append(rec)

    log.info(f"[VALIDATE] ✓ {len(clean)} valid, {rejected} rejected")
    if len(clean) == 0:
        raise ValueError("All records failed validation — aborting")
    return clean


# ── TRANSFORM ─────────────────────────────────────────────────────────────────
def transform(raw: list[dict], fetched_at: str) -> pd.DataFrame:
    log.info("[TRANSFORM] Building DataFrame + enrichment")
    df = pd.DataFrame(raw)

    df = df.rename(columns={
        "id": "coin_id",
        "current_price": "price_usd",
        "total_volume": "volume_24h",
        "price_change_percentage_24h": "change_24h",
        "ath_change_percentage": "ath_pct",
        "circulating_supply": "circulating",
    })

    keep = ["coin_id", "symbol", "name", "price_usd", "market_cap",
            "volume_24h", "change_24h", "ath", "ath_pct", "circulating"]
    df = df[keep].copy()

    # Normalise
    df["symbol"] = df["symbol"].str.upper()
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    df["volume_24h"] = pd.to_numeric(df["volume_24h"], errors="coerce")
    df["change_24h"] = df["change_24h"].round(4)
    df["fetched_at"] = fetched_at

    # Drop any rows that came out NaN after coercion
    before = len(df)
    df = df.dropna(subset=["price_usd"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"[TRANSFORM] Dropped {dropped} rows after numeric coercion")

    log.info(f"[TRANSFORM] ✓ {len(df)} rows ready")
    return df


# ── LOAD ──────────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    log.info(f"[LOAD] Writing {len(df)} rows to crypto_prices")
    df.to_sql("crypto_prices", conn, if_exists="append", index=False)
    conn.commit()
    log.info(f"[LOAD] ✓ {len(df)} rows committed")
    return len(df)


# ── RUN TRACKING ──────────────────────────────────────────────────────────────
def start_run(conn: sqlite3.Connection, run_id: str, started_at: str) -> None:
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
            status,
            rows_extracted,
            rows_loaded,
            json.dumps(error) if error else None,
            run_id,
        ),
    )
    conn.commit()


# ── ORCHESTRATOR ──────────────────────────────────────────────────────────────
def run_pipeline() -> None:
    run_id = f"run_{int(time.time())}"
    started_at = datetime.now(timezone.utc).isoformat()
    fetched_at = started_at
    log.info(f"{'='*60}")
    log.info(f"Pipeline starting — run_id={run_id}")
    log.info(f"{'='*60}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    start_run(conn, run_id, started_at)

    rows_extracted = 0
    rows_loaded = 0
    try:
        raw = extract(run_id)
        rows_extracted = len(raw)

        validated = validate(raw)
        df = transform(validated, fetched_at)
        rows_loaded = load(df, conn)

        finish_run(conn, run_id, "success", rows_extracted, rows_loaded)
        log.info(f"Pipeline complete ✓  extracted={rows_extracted} loaded={rows_loaded}")

    except Exception as exc:
        log.error(f"Pipeline failed: {exc}", exc_info=True)
        finish_run(conn, run_id, "failed", rows_extracted, rows_loaded, str(exc))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
