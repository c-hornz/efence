"""
E-Fence Edge Agent — Main Loop
================================
Orchestrates three concurrent threads:
  1. Wi-Fi scanner  — runs iwlist scan every N seconds
  2. BT scanner     — runs BLE scan window every N seconds
  3. Uploader       — flushes buffer to Databricks every N seconds

All threads write through the shared SQLite buffer. The uploader reads
from it on its own schedule so network outages never block the scanners.

Usage:
  export EFENCE_DATABRICKS_TOKEN="dapi..."
  python3 -m efence.main --config /etc/efence/config.yaml

  # Or run as a service (see efence-agent.service)
"""

import argparse
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone

from . import config as cfg_module
from .buffer   import Buffer
from .uploader import Uploader
from .wifi_scanner import WiFiScanner
from .bt_scanner   import BTScanner


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


log = logging.getLogger("efence.main")


# ---------------------------------------------------------------------------
# Worker threads
# ---------------------------------------------------------------------------

class WiFiWorker(threading.Thread):
    def __init__(self, cfg: dict, buf: Buffer, stop_event: threading.Event):
        super().__init__(name="wifi-scanner", daemon=True)
        self._scanner  = WiFiScanner(cfg)
        self._buf      = buf
        self._interval = cfg["wifi"]["scan_interval_seconds"]
        self._stop     = stop_event

    def run(self):
        log.info("Wi-Fi scanner started (interface=%s, interval=%ds)",
                 self._scanner._interface, self._interval)
        while not self._stop.is_set():
            try:
                records = self._scanner.scan()
                if records:
                    self._buf.put_wifi(records)
                    log.info("Wi-Fi: %d records buffered", len(records))
            except Exception as exc:
                log.error("Wi-Fi scanner error: %s", exc)
            self._stop.wait(self._interval)
        log.info("Wi-Fi scanner stopped")


class BTWorker(threading.Thread):
    def __init__(self, cfg: dict, buf: Buffer, stop_event: threading.Event):
        super().__init__(name="bt-scanner", daemon=True)
        self._scanner  = BTScanner(cfg)
        self._buf      = buf
        self._interval = cfg["bluetooth"]["scan_interval_seconds"]
        self._stop     = stop_event

    def run(self):
        log.info("BT scanner started (scan_duration=%ds, interval=%ds)",
                 self._scanner._scan_duration, self._interval)
        while not self._stop.is_set():
            try:
                records = self._scanner.scan()
                if records:
                    self._buf.put_bt(records)
                    log.info("BT: %d records buffered", len(records))
            except Exception as exc:
                log.error("BT scanner error: %s", exc)
            self._stop.wait(self._interval)
        log.info("BT scanner stopped")


class UploadWorker(threading.Thread):
    def __init__(self, cfg: dict, buf: Buffer, stop_event: threading.Event):
        super().__init__(name="uploader", daemon=True)
        self._buf         = buf
        self._uploader    = Uploader(cfg)
        self._interval    = cfg["buffer"]["upload_interval_seconds"]
        self._max_records = cfg["buffer"]["max_records_per_upload"]
        self._retain_days = cfg["buffer"]["retain_uploaded_days"]
        self._stop        = stop_event

    def run(self):
        log.info("Uploader started (interval=%ds, max_per_cycle=%d)",
                 self._interval, self._max_records)
        cycle = 0
        while not self._stop.is_set():
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            try:
                self._flush_protocol("wifi", self._uploader.upload_wifi)
                self._flush_protocol("bt",   self._uploader.upload_bt)
                cycle += 1
                # Purge old uploaded records once per hour (~60 cycles at 60s)
                if cycle % 60 == 0:
                    self._buf.purge_old(self._retain_days)
                    stats = self._buf.stats()
                    log.info("Buffer stats: %s", stats)
            except Exception as exc:
                log.error("Upload cycle error: %s", exc)
        log.info("Uploader stopped")

    def _flush_protocol(self, protocol: str, upload_fn):
        records = self._buf.get_pending(protocol, self._max_records)
        if not records:
            return
        uploaded_ids = upload_fn(records)
        if uploaded_ids:
            self._buf.mark_uploaded(uploaded_ids)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="E-Fence edge agent")
    parser.add_argument("--config",   default=None,  help="Path to config.yaml")
    parser.add_argument("--log-level",default="INFO", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)
    log.info("E-Fence edge agent starting")

    try:
        cfg = cfg_module.load(args.config)
    except RuntimeError as exc:
        log.error(str(exc))
        sys.exit(1)

    log.info("Sensor ID: %s", cfg["sensor"]["id"])
    log.info("Workspace: %s", cfg["databricks"]["workspace_url"])
    log.info("Wi-Fi enabled: %s | BT enabled: %s",
             cfg["wifi"]["enabled"], cfg["bluetooth"]["enabled"])

    buf        = Buffer(cfg["buffer"]["db_path"])
    stop_event = threading.Event()

    threads = []

    if cfg["wifi"]["enabled"]:
        threads.append(WiFiWorker(cfg, buf, stop_event))
    else:
        log.info("Wi-Fi scanning disabled in config")

    if cfg["bluetooth"]["enabled"]:
        threads.append(BTWorker(cfg, buf, stop_event))
    else:
        log.info("Bluetooth scanning disabled in config")

    threads.append(UploadWorker(cfg, buf, stop_event))

    # Graceful shutdown on SIGINT / SIGTERM
    def _shutdown(signum, frame):
        log.info("Shutdown signal received — stopping workers...")
        stop_event.set()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for t in threads:
        t.start()

    log.info("All workers started. Press Ctrl+C to stop.")

    # Keep the main thread alive
    while not stop_event.is_set():
        stop_event.wait(5)

    # Wait for threads to finish current cycle
    for t in threads:
        t.join(timeout=15)

    log.info("E-Fence agent stopped cleanly")
    sys.exit(0)


if __name__ == "__main__":
    main()
