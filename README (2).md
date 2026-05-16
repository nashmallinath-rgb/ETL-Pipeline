# Crypto ETL Pipeline

![ETL Status](https://github.com/YOUR_USERNAME/etl-pipeline/actions/workflows/etl.yml/badge.svg)
![Coverage](https://codecov.io/gh/YOUR_USERNAME/etl-pipeline/branch/main/graph/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A production-grade ETL pipeline that ingests live cryptocurrency market data from the [CoinGecko API](https://www.coingecko.com/en/api), applies multi-stage validation and transformation, and persists results to a SQLite database — with a full audit trail of every run.

---

## Pipeline Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌────────────┐
│   EXTRACT   │───▶│   VALIDATE   │───▶│    TRANSFORM    │───▶│    LOAD    │
│             │    │              │    │                 │    │            │
│ CoinGecko   │    │ Schema check │    │ Normalise types │    │ SQLite DB  │
│ REST API    │    │ Null checks  │    │ Uppercase syms  │    │ Audit log  │
│ 5 coins     │    │ Price > 0    │    │ Round decimals  │    │ Run meta   │
└─────────────┘    └──────────────┘    └─────────────────┘    └────────────┘
       │                                                               │
       └────────────────── pipeline_runs audit table ─────────────────┘
```

Runs every **30 minutes** via GitHub Actions cron. Every execution is logged to `pipeline_runs` with timestamps, row counts, and error details.

---

## Project Structure

```
etl-pipeline/
├── src/
│   └── pipeline.py          # Core ETL logic (Extract → Validate → Transform → Load)
├── tests/
│   └── test_pipeline.py     # Unit tests (validate, transform, db operations)
├── .github/
│   └── workflows/
│       └── etl.yml          # GitHub Actions: quality check + scheduled ETL run
├── data/                    # SQLite database (auto-created)
├── logs/                    # Pipeline logs (auto-created)
└── requirements.txt
```

---

## Coins Tracked

| Coin | Symbol | Source |
|------|--------|--------|
| Bitcoin | BTC | CoinGecko |
| Ethereum | ETH | CoinGecko |
| Solana | SOL | CoinGecko |
| Cardano | ADA | CoinGecko |
| Polkadot | DOT | CoinGecko |

---

## Database Schema

### `crypto_prices`
| Column | Type | Description |
|--------|------|-------------|
| `coin_id` | TEXT | CoinGecko coin ID |
| `symbol` | TEXT | Ticker symbol (uppercase) |
| `price_usd` | REAL | Current price in USD |
| `market_cap` | REAL | Total market capitalisation |
| `volume_24h` | REAL | 24h trading volume |
| `change_24h` | REAL | 24h price change % |
| `ath` | REAL | All-time high price |
| `ath_pct` | REAL | % below all-time high |
| `fetched_at` | TEXT | UTC timestamp of fetch |

### `pipeline_runs`
| Column | Type | Description |
|--------|------|-------------|
| `run_id` | TEXT | Unique run identifier |
| `started_at` | TEXT | Run start UTC timestamp |
| `finished_at` | TEXT | Run finish UTC timestamp |
| `status` | TEXT | `success` / `failed` / `running` |
| `rows_extracted` | INT | Records fetched from API |
| `rows_loaded` | INT | Records written to DB |
| `errors` | TEXT | JSON-encoded error details |

---

## Run Locally

```bash
git clone https://github.com/YOUR_USERNAME/etl-pipeline
cd etl-pipeline
pip install -r requirements.txt
python src/pipeline.py
```

### Run Tests

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Data Quality Checks

Every record is validated before transformation:
- ✅ All required fields present (`id`, `symbol`, `name`, `current_price`, `market_cap`)
- ✅ `current_price` is numeric and greater than zero
- ✅ Pipeline aborts cleanly if **all** records fail validation
- ✅ Invalid records are logged and skipped individually

---

## GitHub Actions

The pipeline runs on three triggers:
1. **Scheduled** — every 30 minutes via cron
2. **Push to main** — validates code quality on every commit
3. **Manual** — trigger a run anytime from the Actions tab

Artifacts (SQLite DB + logs) are uploaded and retained for 7 days per run.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Data processing | pandas |
| HTTP client | requests |
| Database | SQLite (via stdlib) |
| Scheduling | GitHub Actions cron |
| Testing | pytest + pytest-cov |
| Linting | flake8 |
| CI/CD | GitHub Actions |
