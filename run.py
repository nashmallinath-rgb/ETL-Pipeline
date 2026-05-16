"""
Render entrypoint — runs the ETL pipeline every 30 minutes indefinitely.
"""
import time
import logging
from src.pipeline import run_pipeline

log = logging.getLogger(__name__)

INTERVAL = 1800  # 30 minutes

if __name__ == "__main__":
    print("ETL scheduler started — running every 30 minutes")
    while True:
        try:
            run_pipeline()
        except Exception as e:
            print(f"Pipeline error (will retry next cycle): {e}")
        print(f"Sleeping {INTERVAL}s until next run...")
        time.sleep(INTERVAL)
