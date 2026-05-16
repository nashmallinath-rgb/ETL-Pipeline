"""
Render entrypoint — runs ETL every 30 min + serves a health endpoint
so Render's web service port check passes.
"""
import time
import threading
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from src.pipeline import run_pipeline

log = logging.getLogger(__name__)
INTERVAL = 1800


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ETL pipeline is running")

    def log_message(self, format, *args):
        pass  # suppress HTTP access logs


def start_health_server():
    server = HTTPServer(("0.0.0.0", 10000), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print("Health server started on port 10000")


if __name__ == "__main__":
    start_health_server()
    print("ETL scheduler started — running every 30 minutes")
    while True:
        try:
            run_pipeline()
        except Exception as e:
            print(f"Pipeline error (will retry next cycle): {e}")
        print(f"Sleeping {INTERVAL}s until next run...")
        time.sleep(INTERVAL)
